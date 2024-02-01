import os
import sys
import datetime
import pendulum
import numpy as np
import pandas as pd
from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure, get_mifos_clients, el_transactions_dimension, el_staff_dimension, el_loan_status_dimension, get_mifos_products_for_dimension, product_dimension_process_new_products


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure,
    'jubilee_office_id': 1,
    'jubilee_product_ids': [12]
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_Jubilee_dimensions',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 7 * * *',
        start_date=datetime.datetime(2022, 2, 22, 10, 00, tzinfo=local_tz),
        tags=['extract_load'],
        description='Load data into jubilee dimensions',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Extracts data from MIFOS and adds non existent rows into various tables in warehouse
    DAG is set to run daily at 7:00am.

    #### Actions
    <ol>
    <li>Extract from mifostenant-jubilee.m_staff load into ubuntu.jubilee.staff_dimension</li>
    <li>Extract from mifostenant-jubilee.m_product_loan load into ubuntu.jubilee.product_dimension</li>
    <li>Extract from mifostenant-jubilee.m_client load into ubuntu.jubilee.client_dimension</li>
    <li>Extract from mifostenant-jubilee.r_enum_value load into ubuntu.jubilee.loan_status_dimension</li>
    </ol>
    """

    def add_audit_details(new_row: pd.Series, record_created_on_date: datetime.datetime) -> tuple:
        """
        adds tracking details to a new loan record
        for example record created on date
        NB A new row here is the most recent version of a loan and thus expired on is Null and is most recent version is True
        :param new_row: a series object without audit details
        :param record_created_on_date: when a record was created
        :return: a tuple containing details for a new loan row
        """
        return tuple(
            new_row.append(pd.Series([
                record_created_on_date, None, True],
                index=['record_created_on_date', 'record_expired_on_date', 'is_most_recent_record']
            )).tolist()
        )

    def extract_load_client_dimension(**context) -> None:
        """
        Extracts from mifostenant-jubilee.m_client and loads into ubuntu.jubilee.client_dimension
        """
        warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
        mifos_tenant = 'jubilee'
        warehouse_schema = 'jubilee'

        clients = get_mifos_clients(
            mifos_tenant=mifos_tenant,
            warehouse_schema=warehouse_schema,
            mifos_filters=f"office_id = {context['templates_dict']['jubilee_office_id']}"
        )

        if clients is not None:
            # INSERT NEW RECORDS
            warehouse_hook.insert_rows(
                table='jubilee.client_dimension',
                target_fields=[
                    'mifos_id', 'account_number', 'office_id', 'status', 'default_savings_account_id',
                    'gender', 'client_type', 'first_name', 'middle_name', 'last_name', 'mobile_number',
                    'date_of_birth', 'submitted_on_date', 'submitted_on_user_id', 'activated_on_user_id',
                    'activation_date', 'office_joining_date', 'national_id'
                ],
                rows=tuple(clients[[
                    'id', 'account_no', 'office_id', 'status_enum', 'default_savings_account',
                    'gender_cv_id', 'client_type_cv_id', 'firstname', 'middlename', 'lastname', 'mobile_no',
                    'date_of_birth', 'submittedon_date', 'submittedon_userid', 'activatedon_userid',
                    'activation_date', 'office_joining_date', 'national_id'
                ]].replace({np.nan: None}).itertuples(index=False, name=None)),
                postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                schema='afsg_ds_prod_postgresql_dwh'
            )


    def extract_load_product_dimension(**context) -> None:
        """
        Extracts from mifostenant-safaricom.m_product_loan and loads into ubuntu.bloomlive.product_dimension

        :param context: Airflow context dictionary
        :type context: dict
        """
        warehouse_schema = 'jubilee'
        mifos_tenant = 'jubilee'

        iterables = ', '.join("{0}".format(w) for w in list(context['templates_dict']['jubilee_product_ids']))
        # Extract data from mifostenant-default.m_product_loan
        all_products = get_mifos_products_for_dimension(
            warehouse_schema=warehouse_schema,
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}'),
            mifos_filters=f"where id in ({iterables})"
        )

        # Separate data into products, loans_fact_table_updates, inserted_records, and expired_products_updates
        products = all_products['products']
        loans_fact_table_updates = all_products['loans_fact_table_updates']
        expired_products_updates = all_products['expired_products_updates']
        # Update the changed products already inserted into the warehouse with a bloom version value of 1

        product_dimension_process_new_products(
            warehouse_schema=warehouse_schema,
            products=products,
            loans_fact_table_updates=loans_fact_table_updates,
            expired_products_updates=expired_products_updates,
        )

    def extract_load_loan_status_dimension(**context) -> None:
        """
        Extracts new loan statuses from Mifos and loads them into the loan status dimension table.
        :param context: Airflow context
        """
        warehouse_schema = 'jubilee'
        mifos_tenant = 'jubilee'

        # Get new loan statuses from Mifos
        el_loan_status_dimension(
            warehouse_schema=warehouse_schema,
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}')
        )

    def extract_load_staff_dimension(**context) -> None:
        """
        Extracts staff data from the Mifos database, transforms the data by adding a timestamp and a Bloom version number,
        and loads the data into the Bloomlive staff_dimension table.

        :param context: Airflow context object
        :return: None
        """
        warehouse_schema = 'jubilee'
        mifos_tenant = 'jubilee'

        # Extract data from Mifos for staff dimension
        el_staff_dimension(
            warehouse_schema=warehouse_schema,
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}')
        )

    def trigger_ETL_jubilee_loans_fact_table(**context):
        from utils.common import trigger_dag_remotely

        trigger_dag_remotely(dag_id='ETL_Jubilee_loans_fact_table', conf={})

    def extract_load_transactions_dimension(**context):
        """
        Extracts from mifostenant-tanda.m_payment_detail and loads into ubuntu.jumia.transactions_dimension
        """
        mifos_tenant = 'jubilee'
        warehouse_schema = 'jubilee'
        transaction_types = [1, 2]
        office_id = context['templates_dict']['jubilee_office_id']

        el_transactions_dimension(
            mifos_tenant=mifos_tenant,
            warehouse_schema=warehouse_schema,
            transaction_types=transaction_types,
            office_id=office_id,
            year=context['dag_run'].conf.get('year', datetime.datetime.now().year),
            month=context['dag_run'].conf.get('month', datetime.datetime.now().month)
        )


    common_params = {
        'jubilee_office_id': '{{jubilee_office_id}}',
        'jubilee_product_ids': default_args['jubilee_product_ids']
    }

    # TASKS
    t1 = PythonOperator(
        task_id='extract_load_client_dimension',
        provide_context=True,
        python_callable=extract_load_client_dimension,
        templates_dict=common_params
    )
    t2 = PythonOperator(
        task_id='extract_load_loan_status_dimension',
        provide_context=True,
        python_callable=extract_load_loan_status_dimension
    )
    t3 = PythonOperator(
        task_id='extract_load_product_dimension',
        provide_context=True,
        python_callable=extract_load_product_dimension,
        templates_dict=common_params
    )
    t4 = PythonOperator(
        task_id='extract_load_staff_dimension',
        provide_context=True,
        python_callable=extract_load_staff_dimension
    )
    t5 = PythonOperator(
        task_id='trigger_ETL_jubilee_loans_fact_table',
        provide_context=True,
        python_callable=trigger_ETL_jubilee_loans_fact_table
    )
    t6 = PythonOperator(
        task_id='extract_load_transactions_dimension',
        provide_context=True,
        python_callable=extract_load_transactions_dimension,
        templates_dict=common_params
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
