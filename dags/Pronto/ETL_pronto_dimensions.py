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

mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-pronto')
pronto_hook = MySqlHook(mysql_conn_id='pronto_db', schema='pronto_staging')
warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure,
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_pronto_dimensions',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 */3 * * *' if Variable.get('DEBUG') == 'FALSE' else None,
        start_date=datetime.datetime(2021, 12, 30, 5, 00, tzinfo=local_tz),
        tags=['extract_load'],
        description='Load data into pronto dimensions',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Extracts data from MIFOS and adds non existent rows into various tables in warehouse
    DAG is set to run daily at 5 am.

    #### Actions
    <ol>
    <li>Extract from mifostenant-pronto.m_staff load into ubuntu.pronto.staff_dimension</li>
    <li>Extract from mifostenant-pronto.m_product_loan load into ubuntu.pronto.product_dimension</li>
    <li>Extract from mifostenant-pronto.m_client load into ubuntu.pronto.client_dimension</li>
    <li>Extract from mifostenant-pronto.r_enum_value load into ubuntu.pronto.loan_type_dimension</li>
    </ol>
    """

    def update_customer_codes():
        # update customer codes
        missing_customer_codes = warehouse_hook.get_pandas_df(
            sql="""
                select mifos_id from pronto.client_dimension where customer_code is null
            """
        )
        customer_codes = pronto_hook.get_pandas_df(
            sql="""
                select mifos_id as MifosClientId, customer_code from user_customer where mifos_id in %(mci)s
            """,
            parameters={'mci': tuple(missing_customer_codes['mifos_id'].tolist())}
        )
        rslts = missing_customer_codes.astype({'mifos_id': int}).merge(
            customer_codes.astype({'MifosClientId': int}),
            left_on='mifos_id',
            right_on='MifosClientId',
            how='inner'
        )
        updates = [
            f"update pronto.client_dimension set customer_code = '{r['customer_code']}' where mifos_id = {r['mifos_id']}"
            for i, r in rslts.iterrows()
        ]

        if len(updates) > 0:
            warehouse_hook.run(sql=updates)

    def update_customer_gender():
        # update customer gender
        missing_customer_gender = warehouse_hook.get_pandas_df(
            sql="""
                select mifos_id from pronto.client_dimension where gender is null
            """
        )
        customer_gender = mifos_hook.get_pandas_df(
            sql="""
                select 
                    mc.id as MifosClientId, mcv.code_value as gender
                from m_client mc
                left join m_code_value mcv 
                on mc.gender_cv_id = mcv.id 
            """,
            parameters={'mcg': tuple(missing_customer_gender['mifos_id'].tolist())}
        )

        rslts = missing_customer_gender.astype({'mifos_id': int}).merge(
            customer_gender.astype({'MifosClientId': int}),
            left_on='mifos_id',
            right_on='MifosClientId',
            how='inner'
        )

        rslts = rslts.dropna(subset=['gender'])

        updates = [
            f"update pronto.client_dimension set gender = '{r['gender']}' where mifos_id = {r['mifos_id']}"
            for i, r in rslts.iterrows()
        ]

        if len(updates) > 0:
            warehouse_hook.run(sql=updates)


    def extract_load_client_dimension(**context) -> None:
        """
        Extracts from mifostenant-pronto.m_client and loads into ubuntu.pronto.client_dimension
        """
        mifos_tenant = 'pronto'
        warehouse_schema = 'pronto'

        clients = get_mifos_clients(
            mifos_tenant=mifos_tenant,
            warehouse_schema=warehouse_schema
        )

        if clients is not None:
            # INSERT NEW RECORDS
            insert_new_records(
                table_name='pronto.client_dimension',
                target_fields=[
                    'mifos_id', 'account_number', 'national_id', 'office_id', 'status', 'default_savings_account_id', 'gender',
                    'client_type', 'first_name', 'middle_name', 'last_name', 'mobile_number', 'date_of_birth',
                    'submitted_on_date', 'submitted_on_user_id', 'activated_on_user_id', 'activation_date',
                    'office_joining_date'
                ],
                rows=tuple(clients[[
                    'id', 'account_no', 'national_id', 'office_id', 'status_enum', 'default_savings_account', 'gender_cv_id',
                    'client_type_cv_id', 'firstname', 'middlename', 'lastname', 'mobile_no', 'date_of_birth',
                    'submittedon_date', 'submittedon_userid', 'activatedon_userid', 'activation_date',
                    'office_joining_date',
                ]].replace({np.nan: None}).itertuples(index=False, name=None)),
                postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                schema='afsg_ds_prod_postgresql_dwh'
            )

        update_customer_codes()
        update_customer_gender()  # update customer gender


    def extract_load_product_dimension(**context) -> None:
        """
        Extracts from mifostenant-safaricom.m_product_loan and loads into ubuntu.bloomlive.product_dimension

        :param context: Airflow context dictionary
        :type context: dict
        """
        warehouse_schema = 'pronto'
        mifos_tenant = 'pronto'

        # Extract data from mifostenant-default.m_product_loan
        all_products = get_mifos_products_for_dimension(
            warehouse_schema=warehouse_schema,
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}')
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
        warehouse_schema = 'pronto'
        mifos_tenant = 'pronto'

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
        warehouse_schema = 'pronto'
        mifos_tenant = 'pronto'

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
        mifos_tenant = 'pronto'
        warehouse_schema = 'pronto'
        transaction_types = [1, 2]
        office_ids = mifos_hook.get_pandas_df(sql="""select id from m_office""")['id'].tolist()

        for office_id in office_ids:
            el_transactions_dimension(
                mifos_tenant=mifos_tenant,
                warehouse_schema=warehouse_schema,
                transaction_types=transaction_types,
                office_id=office_id,
                year=context['dag_run'].conf.get('year', datetime.datetime.now().year),
                month=context['dag_run'].conf.get('month', datetime.datetime.now().month)
            )

    def trigger_ETL_pronto_loans_fact_table(**context):
        from utils.common import trigger_dag_remotely

        # Triggering the DAG remotely
        trigger_dag_remotely(dag_id='ETL_pronto_loans_fact_table', conf={})


    # TASKS
    t1 = PythonOperator(
        task_id='extract_load_product_dimension',
        provide_context=True,
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
    )
    t6 = PythonOperator(
        task_id='extract_load_transactions_dimension',
        provide_context=True,
        python_callable=extract_load_transactions_dimension
    )
    t5 = PythonOperator(
        task_id='trigger_ETL_pronto_loans_fact_table',
        provide_context=True,
        python_callable=trigger_ETL_pronto_loans_fact_table
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
