import os
import sys
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

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-uganda')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure,
    'interswitch_office_id': 2,
    'interswitch_product_ids': [2, 3],
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_interswitch_ug_dimensions',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 */3 * * *',
        start_date=datetime.datetime(2023, 1, 17, 8, 20, tzinfo=local_tz),
        tags=['extract_load'],
        description='Load data into interswitch_ug dimensions',
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
    <li>Extract from mifostenant-uganda.m_staff load into ubuntu.interswitch_ug.staff_dimension</li>
    <li>Extract from mifostenant-uganda.m_product_loan load into ubuntu.interswitch_ug.product_dimension</li>
    <li>Extract from mifostenant-uganda.m_client load into ubuntu.interswitch_ug.client_dimension</li>
    <li>Extract from mifostenant-uganda.r_enum_value load into ubuntu.interswitch_ug.loan_type_dimension</li>
    </ol>
    """
    common_params = {'interswitch_office_id': '{{interswitch_office_id}}',}

    def extract_load_client_dimension(**context) -> None:
        """
        Extracts from mifostenant-uganda.m_client and loads into ubuntu.interswitch_ug.client_dimension
        """
        mifos_tenant = 'uganda'
        warehouse_schema = 'interswitch_ug'

        clients = get_mifos_clients(
            mifos_tenant=mifos_tenant,
            warehouse_schema=warehouse_schema,
            mifos_filters=f"office_id = {context['templates_dict']['interswitch_office_id']}"
        )

        if clients is not None:
            # INSERT NEW RECORDS
            insert_new_records(
                table_name='interswitch_ug.client_dimension',
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
        warehouse_schema = 'interswitch_ug'
        mifos_tenant = 'uganda'

        # Extract data from mifostenant-default.m_product_loan
        all_products = get_mifos_products_for_dimension(
            warehouse_schema=warehouse_schema,
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}'),
            mifos_filters=f"where id in {tuple(list(context['templates_dict']['interswitch_product_ids']))}"
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
        warehouse_schema = 'interswitch_ug'
        mifos_tenant = 'uganda'

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
        warehouse_schema = 'interswitch_ug'
        mifos_tenant = 'uganda'

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
        mifos_tenant = 'uganda'
        warehouse_schema = 'interswitch_ug'
        transaction_types = [1, 2]
        office_id = context['templates_dict']['interswitch_office_id']

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
        Extracts from mifostenant-uganda.m_payment_detail and loads into ubuntu.interswitch_ug.transactions_dimension
        """

        existing_records = warehouse_hook.get_pandas_df(
            sql="""
                SELECT mifos_id FROM interswitch_ug.loan_officer_assignment_history
            """
        )

        if existing_records.shape[0] > 0:
            # extract only those records that are not already stored in the warehouse
            assignment_history = mifos_hook.get_pandas_df(
                sql=f"""
                    select 
                        mloah.id as mifos_id, loan_id as loan_mifos_id, mloah.loan_officer_id as loan_officer_mifos_id, 
                        start_date, end_date, createdby_id as created_by_id, created_date, 
                        lastmodified_date as last_modified_date, lastmodifiedby_id as last_modified_by_id
                    from `mifostenant-uganda`.m_loan_officer_assignment_history mloah
                    left join `mifostenant-uganda`.m_loan ml on mloah.loan_id = ml.id 
                    left join `mifostenant-uganda`.m_client mc on ml.client_id = mc.id and mc.office_id = %(office_id)s
                    where mloah.id not in %(mifos_ids)s
                """,
                parameters={
                    'mifos_ids': tuple(existing_records['mifos_id'].tolist()),
                    'office_id': context['templates_dict']['interswitch_office_id']
                }
            )
        else:
            assignment_history = mifos_hook.get_pandas_df(
                sql=f"""
                    select 
                        mloah.id as mifos_id, loan_id as loan_mifos_id, mloah.loan_officer_id as loan_officer_mifos_id, 
                        start_date, end_date, createdby_id as created_by_id, created_date, 
                        lastmodified_date as last_modified_date, lastmodifiedby_id as last_modified_by_id
                    from `mifostenant-uganda`.m_loan_officer_assignment_history mloah
                    left join `mifostenant-uganda`.m_loan ml on mloah.loan_id = ml.id
                    left join `mifostenant-uganda`.m_client mc on ml.client_id = mc.id and mc.office_id = %(office_id)s
                """,
                parameters = {
                    'office_id': context['templates_dict']['interswitch_office_id']
                }
            )

        if assignment_history.shape[0] > 0:

            # INSERT NEW RECORDS
            insert_new_records(
                table_name='interswitch_ug.loan_officer_assignment_history',
                target_fields=[
                    'mifos_id', 'loan_mifos_id',
                    'loan_officer_mifos_id', 'start_date', 'end_date',
                    'created_by_id', 'created_date', 'last_modified_date',
                    'last_modified_by_id'
                ],
                rows=tuple(assignment_history[[
                    'mifos_id', 'loan_mifos_id',
                    'loan_officer_mifos_id', 'start_date', 'end_date',
                    'created_by_id', 'created_date', 'last_modified_date',
                    'last_modified_by_id'
                ]].replace({np.nan: None}).itertuples(index=False, name=None)),
                postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                schema='afsg_ds_prod_postgresql_dwh'
            )

    def trigger_ETL_interswitch_ug_loans_fact_table(**context):
        from utils.common import trigger_dag_remotely

        # Triggering the DAG remotely
        trigger_dag_remotely(dag_id='ETL_interswitch_ug_loans_fact_table', conf={})


    # TASKS
    t1 = PythonOperator(
        task_id='extract_load_product_dimension',
        provide_context=True,
        templates_dict={
            'interswitch_office_id': '{{interswitch_office_id}}',
            'interswitch_product_ids': default_args['interswitch_product_ids']
        },
        python_callable=extract_load_product_dimension,
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
        templates_dict={
            'interswitch_office_id': '{{interswitch_office_id}}',
        },
    )
    t6 = PythonOperator(
        task_id='extract_load_transactions_dimension',
        provide_context=True,
        templates_dict={
            'interswitch_office_id': '{{interswitch_office_id}}',
        },
        python_callable=extract_load_transactions_dimension,
    )
    t7 = PythonOperator(
        task_id='extract_load_loan_officer_assignment_history_dimension',
        provide_context=True,
        templates_dict=common_params,
        python_callable=extract_load_loan_officer_assignment_history_dimension
    )
    t5 = PythonOperator(
        task_id='trigger_ETL_interswitch_ug_loans_fact_table',
        provide_context=True,
        python_callable=trigger_ETL_interswitch_ug_loans_fact_table
    )
    t1 >> t2 >> t3 >> t4 >> t6 >> t7 >> t5
