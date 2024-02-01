import os
import sys
import logging
import datetime
import pendulum
import numpy as np
from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure, get_mifos_clients, el_transactions_dimension, el_staff_dimension, el_loan_status_dimension, get_mifos_products_for_dimension, product_dimension_process_new_products

log_format = "%(asctime)s: %(message)s"
logging.basicConfig(format=log_format, level=logging.WARNING, datefmt="%H:%M:%S")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'tanda_fund_id': 1,
    'tanda_office_id': 1,
    'on_failure_callback': on_failure,
    'tanda_product_ids': [1,2]
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_Tanda_dimensions',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 */3 * * *',
        start_date=datetime.datetime(2021, 9, 29, 2, 30, tzinfo=local_tz),
        tags=['extract_load'],
        description='Load data into tanda dimensions',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Extracts data from MIFOS and adds non existent rows into various tables in warehouse
    DAG is set to run daily at 2:30 am.

    #### Actions
    <ol>
    <li>Extract from mifostenant-tanda.m_staff load into ubuntu.tanda.staff_dimension</li>
    <li>Extract from mifostenant-tanda.m_product_loan load into ubuntu.tanda.product_dimension</li>
    <li>Extract from mifostenant-tanda.m_client load into ubuntu.tanda.client_dimension</li>
    <li>Extract from mifostenant-tanda.r_enum_value load into ubuntu.tanda.loan_type_dimension</li>
    </ol>
    """

    common_params = {
        'tanda_fund_id': '{{tanda_fund_id}}',
        'tanda_office_id': '{{tanda_office_id}}',
        'tanda_product_ids': default_args['tanda_product_ids']
    }


    def extract_load_client_dimension(**context) -> None:
        """
        Extracts from mifostenant-tanda.m_client and loads into ubuntu.tanda.client_dimension
        """
        mifos_tenant = 'tanda'
        warehouse_schema = 'tanda'

        clients = get_mifos_clients(
            mifos_tenant=mifos_tenant,
            warehouse_schema=warehouse_schema,
            mifos_filters=f"office_id = {context['templates_dict']['tanda_office_id']}"
        )

        if clients is not None:
            # INSERT NEW RECORDS
            insert_new_records(
                table_name='tanda.client_dimension',
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

    def add_opt_in_key_to_client_dimension(**context) -> None:
        """
        add customer id from table tanda.raw_kyc
        """
        destination_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
        clients_without_opt_in_keys = tuple(destination_hook.get_pandas_df(
            sql="""
                SELECT national_id FROM tanda.client_dimension
                WHERE opt_in_key is null
            """
        )['national_id'].values)

        opt_in_keys = destination_hook.get_pandas_df(
            sql="""
                SELECT customer_id, customer_national_id FROM tanda.raw_kyc
                WHERE customer_national_id in %(national_ids)s
                GROUP BY customer_id, customer_national_id
            """,
            parameters={
                'national_ids': clients_without_opt_in_keys
            }
        )

        for index, row in opt_in_keys.replace(np.NAN, None).iterrows():
            destination_hook.run(
                sql="""
                        UPDATE tanda.client_dimension
                        SET opt_in_key = %(opt_in_key)s
                        WHERE national_id= %(customer_national_id)s
                    """,
                parameters={
                    'opt_in_key': row['customer_id'],
                    'customer_national_id': row['customer_national_id'],
                }
            )

    def extract_load_product_dimension(**context) -> None:
        """
        Extracts from mifostenant-safaricom.m_product_loan and loads into ubuntu.bloomlive.product_dimension

        :param context: Airflow context dictionary
        :type context: dict
        """
        mifos_tenant = 'tanda'
        warehouse_schema = 'tanda'

        # Extract data from mifostenant-default.m_product_loan
        all_products = get_mifos_products_for_dimension(
            warehouse_schema=warehouse_schema,
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}'),
            mifos_filters=f"where id in {tuple(list(context['templates_dict']['tanda_product_ids']))}"
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
        mifos_tenant = 'tanda'
        warehouse_schema = 'tanda'

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
        warehouse_schema = 'tanda'
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

    def extract_load_transactions_dimension(**context):
        """
        Extracts from mifostenant-tanda.m_payment_detail and loads into ubuntu.jumia.transactions_dimension
        """
        mifos_tenant = 'tanda'
        warehouse_schema = 'tanda'
        transaction_types = [1, 2]
        office_id = context['templates_dict']['tanda_office_id']

        el_transactions_dimension(
            mifos_tenant=mifos_tenant,
            warehouse_schema=warehouse_schema,
            transaction_types=transaction_types,
            office_id=office_id,
            year=context['dag_run'].conf.get('year', datetime.datetime.now().year),
            month=context['dag_run'].conf.get('month', datetime.datetime.now().month)
        )

    def trigger_ETL_Tanda_loans_fact_table(**context):
        from utils.common import trigger_dag_remotely
        trigger_dag_remotely(dag_id='ETL_Tanda_loans_fact_table', conf={})


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
        task_id='add_opt_in_key_to_client_dimension',
        provide_context=True,
        python_callable=add_opt_in_key_to_client_dimension,
    )
    t7 = PythonOperator(
        task_id='trigger_ETL_Tanda_loans_fact_table',
        provide_context=True,
        python_callable=trigger_ETL_Tanda_loans_fact_table
    )

    t1 >> t2 >> t3 >> t4 >> t6 >> t5 >> t7
