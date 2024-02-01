import os
import sys
import datetime
import pendulum
import numpy as np
from airflow import DAG
from datetime import timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure, get_mifos_clients, el_transactions_dimension, el_staff_dimension, el_loan_status_dimension, get_mifos_products_for_dimension, product_dimension_process_new_products

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-tanda')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'jumia_fund_id': 3,
    'jumia_office_id': 3,
    'jumia_product_ids': [7,8,9,21,22,23],
    'on_failure_callback': on_failure,
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_Jumia_dimensions',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 */2 * * *' if Variable.get('DEBUG') == 'FALSE' else None,
        start_date=datetime.datetime(2021, 9, 29, 4, 30, tzinfo=local_tz),
        tags=['extract_load'],
        description='Load data into jumia dimensions',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Extracts data from MIFOS and adds non existent rows into various tables in warehouse
    DAG is set to run every 3 hours.

    #### Actions
    <ol>
    <li>Extract from mifostenant-tanda.m_staff load into ubuntu.jumia.staff_dimension</li>
    <li>Extract from mifostenant-tanda.m_product_loan load into ubuntu.jumia.product_dimension</li>
    <li>Extract from mifostenant-tanda.m_client load into ubuntu.jumia.client_dimension</li>
    <li>Extract from mifostenant-tanda.r_enum_value load into ubuntu.jumia.loan_type_dimension</li>
    </ol>
    """

    common_params = {
        'jumia_fund_id': '{{jumia_fund_id}}',
        'jumia_office_id': '{{jumia_office_id}}',
        'jumia_product_ids': default_args['jumia_product_ids']
    }

    def extract_load_transactions_dimension(**context):
        """
        Extracts from mifostenant-tanda.m_payment_detail and loads into ubuntu.jumia.transactions_dimension
        """
        mifos_tenant = 'tanda'
        warehouse_schema = 'jumia'
        transaction_types = [1, 2]
        office_id = context['templates_dict']['jumia_office_id']

        el_transactions_dimension(
            mifos_tenant=mifos_tenant,
            warehouse_schema=warehouse_schema,
            transaction_types=transaction_types,
            office_id=office_id,
            year=context['dag_run'].conf.get('year', datetime.datetime.now().year),
            month=context['dag_run'].conf.get('month', datetime.datetime.now().month)
        )

    def extract_load_client_dimension(**context) -> None:
        """
        Extracts from mifostenant-tanda.m_client and loads into ubuntu.jumia.client_dimension
        """
        mifos_tenant = 'tanda'
        warehouse_schema = 'jumia'

        clients = get_mifos_clients(
            mifos_tenant=mifos_tenant,
            warehouse_schema=warehouse_schema,
            mifos_filters=f"office_id = {context['templates_dict']['jumia_office_id']}"
        )

        if clients is not None:
            # INSERT NEW RECORDS
            insert_new_records(
                table_name='jumia.client_dimension',
                target_fields=[
                    'mifos_id', 'account_number', 'office_id', 'status', 'default_savings_account_id', 'gender',
                    'client_type', 'first_name', 'middle_name', 'last_name', 'mobile_number', 'date_of_birth',
                    'submitted_on_date', 'submitted_on_user_id', 'activated_on_user_id', 'activation_date',
                    'office_joining_date', 'national_id'
                ],
                rows=tuple(clients[[
                    'id', 'account_no', 'office_id', 'status_enum', 'default_savings_account', 'gender_cv_id',
                    'client_type_cv_id', 'firstname', 'middlename', 'lastname', 'mobile_no', 'date_of_birth',
                    'submittedon_date', 'submittedon_userid', 'activatedon_userid', 'activation_date',
                    'office_joining_date', 'national_id'
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
        warehouse_schema = 'jumia'
        mifos_tenant = 'tanda'

        # Extract data from mifostenant-default.m_product_loan
        all_products = get_mifos_products_for_dimension(
            warehouse_schema=warehouse_schema,
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}'),
            mifos_filters=f"where id in {tuple(list(context['templates_dict']['jumia_product_ids']))}"
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
        warehouse_schema = 'jumia'
        mifos_tenant = 'tanda'

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
        warehouse_schema = 'jumia'
        mifos_tenant = 'tanda'

        # Extract data from Mifos for staff dimension
        el_staff_dimension(
            warehouse_schema=warehouse_schema,
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}')
        )

    def insert_new_records(table_name, target_fields, rows, postgres_conn_id, schema) -> None:
        """
        :param table_name: the table to insert records into
        :param target_fields: the columns to insert records into
        :param rows: the data that is to be inserted
        :param postgres_conn_id: the connection credentials
        :param schema: the database
        :return: None
        """
        warehouse_hook = PostgresHook(postgres_conn_id=postgres_conn_id, schema=schema)
        warehouse_hook.insert_rows(
            table=table_name,
            target_fields=target_fields,
            replace=False,
            rows=rows,
            commit_every=1000
        )

    def trigger_ETL_Jumia_loans_fact_table(**context):
        from utils.common import trigger_dag_remotely
        trigger_dag_remotely(dag_id='ETL_Jumia_loans_fact_table', conf={})


    # TASKS
    t1 = PythonOperator(
        task_id='extract_load_product_dimension',
        provide_context=True,
        python_callable=extract_load_product_dimension,
        templates_dict=common_params
    )

    t2 = PythonOperator(
        task_id='extract_load_staff_dimension',
        provide_context=True,
        python_callable=extract_load_staff_dimension,
        templates_dict=common_params
    )

    t3 = PythonOperator(
        task_id='extract_load_loan_status_dimension',
        provide_context=True,
        python_callable=extract_load_loan_status_dimension
    )

    t4 = PythonOperator(
        task_id='extract_load_client_dimension',
        provide_context=True,
        python_callable=extract_load_client_dimension,
        templates_dict=common_params
    )
    t6 = PythonOperator(
        task_id='extract_load_transactions_dimension',
        provide_context=True,
        python_callable=extract_load_transactions_dimension,
        templates_dict=common_params
    )
    t5 = PythonOperator(
        task_id='trigger_ETL_Jumia_loans_fact_table',
        provide_context=True,
        python_callable=trigger_ETL_Jumia_loans_fact_table
    )

    t1 >> t2 >> t3 >> t4 >> t6 >> t5
