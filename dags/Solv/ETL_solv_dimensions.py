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
from utils.common import on_failure, el_transactions_dimension, get_mifos_clients, el_staff_dimension, el_loan_status_dimension, product_dimension_process_new_products
from dags.Solv.helper_methods import get_mifos_products_for_dimension
solv_hook = MySqlHook(mysql_conn_id='solv_ke', schema='solvke_staging')
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
    'solv_office_id': 7,
    'on_failure_callback': on_failure,
    'solv_product_ids': [
        15,16,17,18,19,20,30,31,32,33,34,35,36,37,38,40,41,42,43,44,45,46,47,48,49,
        51,52,53,54,55,56,57,58,59,61,62,63,64,65,66,67,68,75,76,77,78,79,80,81,82,
        83,84,85,86,87,89,90,91,92,93,94,95,96,97,98,99,100,101,103
    ]
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_Solv_dimensions',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 */3 * * *',
        start_date=datetime.datetime(2021, 9, 29, 3, 30, tzinfo=local_tz),
        tags=['extract_load'],
        description='Load data into solv dimensions',
        user_defined_macros=default_args
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Extracts data from MIFOS and adds non existent rows into various tables in warehouse
    DAG is set to run daily at 3:30 am.

    #### Actions
    <ol>
    <li>Extract from mifostenant-tanda.m_staff load into ubuntu.solv_bat.staff_dimension</li>
    <li>Extract from mifostenant-tanda.m_product_loan load into ubuntu.solv_bat.product_dimension</li>
    <li>Extract from mifostenant-tanda.m_client load into ubuntu.solv_bat.client_dimension</li>
    </ol>
    """

    common_params = {
        'solv_office_id': '{{solv_office_id}}',
        'solv_product_ids': default_args['solv_product_ids']
    }
    mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-tanda')
    warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')


    def update_customer_codes():
        # update customer codes
        missing_customer_codes = warehouse_hook.get_pandas_df(
            sql="""
                select mifos_id from solv_bat.client_dimension where customer_code is null
            """
        )
        customer_codes = solv_hook.get_pandas_df(
            sql="""
                select MifosClientId, PartnerCustomerId as customer_code from Customers where MifosClientId in %(mci)s
            """,
            parameters={'mci': tuple(missing_customer_codes['mifos_id'].tolist())}
        )
        rslts = missing_customer_codes.merge(
            customer_codes,
            left_on='mifos_id',
            right_on='MifosClientId',
            how='inner'
        )
        updates = [
            f"update solv_bat.client_dimension set customer_code = '{r['customer_code']}' where mifos_id = {r['mifos_id']}"
            for i, r in rslts.iterrows()
        ]
        if len(updates) > 0:
            warehouse_hook.run(sql=updates)


    def update_email_addresses():
        # update customer codes
        missing_customer_codes = warehouse_hook.get_pandas_df(
            sql="""
                select mifos_id from solv_bat.client_dimension where email_address is null
            """
        )
        customer_codes = solv_hook.get_pandas_df(
            sql="""
                select MifosClientId, EmailAddress as email_address from Customers where MifosClientId in %(mci)s
            """,
            parameters={'mci': tuple(missing_customer_codes['mifos_id'].tolist())}
        )
        rslts = missing_customer_codes.merge(
            customer_codes,
            left_on='mifos_id',
            right_on='MifosClientId',
            how='inner'
        )
        for i, r in rslts.iterrows():
            warehouse_hook.run(
                sql="""update solv_bat.client_dimension set email_address = %(ea)s where mifos_id = %(mi)s""",
                parameters={'ea': r['email_address'], 'mi': r['mifos_id']}
            )

    def extract_load_client_dimension(**context) -> None:
        """
        Extracts from mifostenant-tanda.m_client and loads into ubuntu.solv_bat.client_dimension
        """
        mifos_tenant = 'tanda'
        warehouse_schema = 'solv_bat'

        clients = get_mifos_clients(
            mifos_tenant=mifos_tenant,
            warehouse_schema=warehouse_schema,
            mifos_filters=f"office_id = {context['templates_dict']['solv_office_id']}"
        )

        if clients is not None:
            clients['company_name'] = clients['lastname']

            def get_names(x):
                names = [''] * 10
                count = 0
                for name in [x for x in x['firstname'].strip().split(' ') if x]:
                    names[count] = name
                    count += 1

                clients.loc[x.name, 'firstname'] = names[0]
                clients.loc[x.name, 'middlename'] = names[1]
                clients.loc[x.name, 'lastname'] = ' '.join(names[2:]).strip()

            clients.copy().apply(
                lambda x: get_names(x),
                axis=1
            )

            # INSERT NEW RECORDS
            warehouse_hook.insert_rows(
                table='solv_bat.client_dimension',
                target_fields=[
                    'mifos_id', 'account_number', 'office_id', 'status', 'default_savings_account_id', 'gender',
                    'client_type', 'first_name', 'middle_name', 'last_name', 'mobile_number', 'date_of_birth',
                    'submitted_on_date', 'submitted_on_user_id', 'activated_on_user_id', 'activation_date',
                    'office_joining_date', 'national_id', 'company_name'
                ],
                rows=tuple(clients[[
                    'id', 'account_no', 'office_id', 'status_enum', 'default_savings_account', 'gender_cv_id',
                    'client_type_cv_id', 'firstname', 'middlename', 'lastname', 'mobile_no', 'date_of_birth',
                    'submittedon_date', 'submittedon_userid', 'activatedon_userid', 'activation_date',
                    'office_joining_date', 'national_id', 'company_name'
                ]].replace({np.nan: None}).itertuples(index=False, name=None)),
                commit_every=100
            )

        update_customer_codes()


    def extract_load_product_dimension(**context) -> None:
        """
        Extracts from mifostenant-safaricom.m_product_loan and loads into ubuntu.bloomlive.product_dimension

        :param context: Airflow context dictionary
        :type context: dict
        """
        mifos_tenant = 'tanda'
        warehouse_schema = 'solv_bat'

        # Extract data from mifostenant-default.m_product_loan
        all_products = get_mifos_products_for_dimension(
            warehouse_schema=warehouse_schema,
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}'),
            mifos_filters=f"where mpl.id in {tuple(list(context['templates_dict']['solv_product_ids']))}"
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
        warehouse_schema = 'solv_bat'

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
        warehouse_schema = 'solv_bat'
        mifos_tenant = 'tanda'

        # Extract data from Mifos for staff dimension
        el_staff_dimension(
            warehouse_schema=warehouse_schema,
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}')
        )

    def extract_load_transactions_dimension(**context):
        """
        Extracts from mifostenant-tanda.m_payment_detail and loads into ubuntu.jumia.transactions_dimension
        """
        mifos_tenant = 'tanda'
        warehouse_schema = 'solv_bat'
        transaction_types = [1, 2, 10]
        office_id = context['templates_dict']['solv_office_id']

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
        Extracts from mifostenant-tanda.loan_officer_assignment_history and loads into ubuntu.solv_bat.transactions_dimension
        """
        mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-default')
        warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

        existing_records = warehouse_hook.get_pandas_df(
            sql="""
                select mifos_id from solv_bat.loan_officer_assignment_history
            """
        )

        if existing_records.shape[0] > 0:
            # extract only those records that are not already stored in the warehouse
            assignment_history = mifos_hook.get_pandas_df(
                sql=f"""
                    select 
                        id as mifos_id, loan_id as loan_mifos_id, loan_officer_id as loan_officer_mifos_id, 
                        start_date, end_date, createdby_id as created_by_id, created_date, 
                        lastmodified_date as last_modified_date, lastmodifiedby_id as last_modified_by_id
                    from `mifostenant-tanda`.m_loan_officer_assignment_history
                    where id not in %(mifos_ids)s
                """,
                parameters={'mifos_ids': tuple(existing_records['mifos_id'].tolist())}
            )
        else:
            assignment_history = mifos_hook.get_pandas_df(
                sql=f"""
                    select 
                        id as mifos_id, loan_id as loan_mifos_id, loan_officer_id as loan_officer_mifos_id, 
                        start_date, end_date, createdby_id as created_by_id, created_date, 
                        lastmodified_date as last_modified_date, lastmodifiedby_id as last_modified_by_id
                    from `mifostenant-tanda`.m_loan_officer_assignment_history
                """
            )

        if assignment_history.shape[0] > 0:
            existing_loans = warehouse_hook.get_pandas_df(
                sql="""
                    select loan_mifos_id from solv_bat.loans_fact_table_summary_view
                    where loan_mifos_id in %(mi)s
                """,
                parameters={'mi': tuple(assignment_history['mifos_id'].tolist())}
            )
            assignment_history = assignment_history[assignment_history['mifos_id'].isin(existing_loans['loan_mifos_id'].tolist())]

            # INSERT NEW RECORDS
            warehouse_hook.insert_rows(
                table='solv_bat.loan_officer_assignment_history',
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

    def trigger_ETL_Solv_loans_fact_table(**context):
        from utils.common import trigger_dag_remotely

        # Triggering the DAG remotely
        trigger_dag_remotely(dag_id='ETL_solv_loans_fact_table', conf={})

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
    t7 = PythonOperator(
        task_id='extract_load_loan_officer_assignment_history_dimension',
        provide_context=True,
        python_callable=extract_load_loan_officer_assignment_history_dimension
    )
    t5 = PythonOperator(
        task_id='trigger_ETL_Solv_loans_fact_table',
        provide_context=True,
        python_callable=trigger_ETL_Solv_loans_fact_table
    )
    t1 >> t2 >> t3 >> t4 >> t6 >> t5
