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
from utils.common import on_failure, get_mifos_clients, el_transactions_dimension, product_dimension_process_new_products, el_staff_dimension, el_loan_status_dimension

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-default')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure,
    'bloom1_product_ids': [1, 2, 3, 10, 11]
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_bloom_dimensions',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 */6 * * *',
        start_date=datetime.datetime(2021, 9, 29, 6, 00, tzinfo=local_tz),
        tags=['extract_load'],
        description='Load data into bloomlive dimensions',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Extracts data from MIFOS and adds non existent rows into various tables in warehouse
    DAG is set to run daily at 6 am.

    #### Actions
    <ol>
    <li>Extract from mifostenant-default.m_staff load into ubuntu.bloomlive.staff_dimension</li>
    <li>Extract from mifostenant-default.m_product_loan load into ubuntu.bloomlive.product_dimension</li>
    <li>Extract from mifostenant-default.m_client load into ubuntu.bloomlive.client_dimension</li>
    <li>Extract from mifostenant-default.r_enum_value load into ubuntu.bloomlive.loan_type_dimension</li>
    </ol>
    """


    def extract_load_client_dimension(**context) -> None:
        """
        Extracts data from Mifos Tenant Default, table m_client and loads into bloomlive.client_dimension.
        :param context: Airflow task context
        """

        # Extract data from mifostenant-default.m_client table
        clients = get_mifos_clients(
            mifos_tenant='default',
            warehouse_schema='bloomlive',
            warehouse_filters="bloom_version = '1'"
        )

        if clients is not None:
            clients['bloom_version'] = '1'

            # INSERT NEW RECORDS
            insert_new_records(
                table_name='bloomlive.client_dimension',
                target_fields=[
                    'mifos_id', 'account_number', 'office_id', 'status', 'default_savings_account_id', 'gender',
                    'client_type', 'first_name', 'middle_name', 'last_name', 'mobile_number', 'date_of_birth',
                    'submitted_on_date', 'submitted_on_user_id', 'activated_on_user_id', 'activation_date',
                    'office_joining_date', 'company_name', 'store_number', 'national_id', 'bloom_version'
                ],
                rows=tuple(clients[[
                    'id', 'account_no', 'office_id', 'status_enum', 'default_savings_account', 'gender_cv_id',
                    'client_type_cv_id', 'firstname', 'middlename', 'lastname', 'mobile_no', 'date_of_birth',
                    'submittedon_date', 'submittedon_userid', 'activatedon_userid', 'activation_date',
                    'office_joining_date', 'company_name', 'store_number', 'national_id', 'bloom_version'
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

        from utils.common import get_mifos_products_for_dimension
        warehouse_schema = 'bloomlive'
        mifos_tenant = 'default'

        # Extract data from mifostenant-default.m_product_loan
        all_products = get_mifos_products_for_dimension(
            warehouse_schema=warehouse_schema,
            warehouse_filters="bloom_version = '1'",
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}'),
            mifos_filters=f"where id in {tuple(list(context['templates_dict']['bloom1_product_ids']))}"
        )

        # Separate data into products, loans_fact_table_updates, inserted_records, and expired_products_updates
        products = all_products['products']
        loans_fact_table_updates = all_products['loans_fact_table_updates']
        expired_products_updates = all_products['expired_products_updates']
        changed_records = all_products['changed_records']
        # Update the changed products already inserted into the warehouse with a bloom version value of 1

        if len(changed_records) > 0:
            iterables = ', '.join("{0}".format(w) for w in changed_records)
            warehouse_hook.run(
                sql=f"update bloomlive.product_dimension set bloom_version = '1' where surrogate_id in ({iterables})")

        products['bloom_version'] = '1'

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
        warehouse_schema = 'bloomlive'
        mifos_tenant = 'default'

        # Get new loan statuses from Mifos
        el_loan_status_dimension(
            warehouse_schema=warehouse_schema,
            warehouse_filters="bloom_version = '1'",
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}')
        )

    def extract_load_staff_dimension(**context) -> None:
        """
        Extracts staff data from the Mifos database, transforms the data by adding a timestamp and a Bloom version number,
        and loads the data into the Bloomlive staff_dimension table.

        :param context: Airflow context object
        :return: None
        """
        warehouse_schema = 'bloomlive'
        mifos_tenant = 'default'

        # Extract data from Mifos for staff dimension
        el_staff_dimension(
            warehouse_schema=warehouse_schema,
            warehouse_filters="where bloom_version = '1'",
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
        warehouse_hook.insert_rows(
            table=table_name,
            target_fields=target_fields,
            replace=False,
            rows=rows,
            commit_every=1000
        )

    def trigger_ETL_bloom_loans_fact_table(**context):
        """
        Function to trigger the ETL_bloom_loans_fact_table DAG remotely.
        Parameters:
        ** context: Additional context to be passed to the DAG when triggered.
        Returns: None
        """
        # Importing the required function from a custom module
        from utils.common import trigger_dag_remotely

        # Triggering the DAG remotely
        trigger_dag_remotely(dag_id='ETL_bloom_loans_fact_table', conf={})

    def first_and_last_day_of_month(year, month):
        import calendar
        import datetime

        # Get the number of days in the given month
        num_days = calendar.monthrange(year, month)[1]

        # Create a datetime object for the first day of the month
        first_day = datetime.date(year=year, month=month, day=1)

        # Create a datetime object for the last day of the month
        last_day = datetime.date(year=year, month=month, day=num_days)

        return [first_day, last_day]

    def extract_load_transactions_dimension(**context):
        """
        Extracts from mifostenant-tanda.m_payment_detail and loads into ubuntu.jumia.transactions_dimension
        """
        mifos_tenant = 'default'
        warehouse_schema = 'bloomlive'
        transaction_types = [1, 2]
        office_id = 1

        el_transactions_dimension(
            mifos_tenant=mifos_tenant,
            warehouse_schema=warehouse_schema,
            transaction_types=transaction_types,
            office_id=office_id,
            year=context['dag_run'].conf.get('year', datetime.datetime.now().year),
            month=context['dag_run'].conf.get('month', datetime.datetime.now().month)
        )

    def extract_load_loan_officer_assignment_history_dimension(**context):
        """
        Extracts loan officer assignment history from Mifos and loads it into the warehouse.
        :param context: Airflow context
        :return: None
        """

        from utils.common import get_mifos_loan_officer_assignment_history

        # Get the loan officer assignment history data from Mifos
        assignment_history = get_mifos_loan_officer_assignment_history(
            mifos_tenant='default',
            warehouse_schema='bloomlive',
            filters="WHERE bloom_version = '1'",
            warehouse_hook=PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh'),
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-default'),
        )

        if assignment_history.shape[0] > 0:
            # set the bloom version
            assignment_history['bloom_version'] = '1'

            # INSERT NEW RECORDS
            insert_new_records(
                table_name='bloomlive.loan_officer_assignment_history',
                target_fields=[
                    'bloom_version', 'mifos_id', 'loan_mifos_id',
                    'loan_officer_mifos_id', 'start_date', 'end_date',
                    'created_by_id', 'created_date', 'last_modified_date',
                    'last_modified_by_id'
                ],
                # get the required columns and replace null values with None
                rows=tuple(assignment_history[[
                    'bloom_version', 'mifos_id', 'loan_mifos_id',
                    'loan_officer_mifos_id', 'start_date', 'end_date',
                    'created_by_id', 'created_date', 'last_modified_date',
                    'last_modified_by_id'
                ]].replace({np.nan: None}).itertuples(index=False, name=None)),
                postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                schema='afsg_ds_prod_postgresql_dwh'
            )


    common_params = {'bloom1_product_ids': default_args['bloom1_product_ids']}

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
    )
    t6 = PythonOperator(
        task_id='extract_load_transactions_dimension',
        provide_context=True,
        python_callable=extract_load_transactions_dimension,
    )
    t7 = PythonOperator(
        task_id='extract_load_loan_officer_assignment_history_dimension',
        provide_context=True,
        python_callable=extract_load_loan_officer_assignment_history_dimension
    )
    t5 = PythonOperator(
        task_id='trigger_ETL_bloom_loans_fact_table',
        provide_context=True,
        python_callable=trigger_ETL_bloom_loans_fact_table
    )

    t1 >> t2 >> t3 >> t4 >> t6 >> t7 >> t5
