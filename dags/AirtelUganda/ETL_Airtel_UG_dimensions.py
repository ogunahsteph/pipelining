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
from utils.common import on_failure, get_mifos_clients, el_transactions_dimension, product_dimension_process_new_products, el_loan_status_dimension, el_staff_dimension, get_mifos_products_for_dimension

mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-devices')
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
    'airtel_uganda_office_id': 1,
    'airtel_uganda_device_financing_product_ids': [1, 2, 3, 4]
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_Airtel_UG_dimensions',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 */3 * * *',
        start_date=datetime.datetime(2021, 10, 4, 6, 15, tzinfo=local_tz),
        tags=['extract_load'],
        description='Load data into airtel_uganda_device_financing dimensions',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Extracts data from MIFOS and adds non existent rows into various tables in warehouse
    DAG is set to run daily at 6:15 am.

    #### Actions
    <ol>
    <li>Extract from mifostenant-devices.m_staff load into ubuntu.airtel_uganda_device_financing.staff_dimension</li>
    <li>Extract from mifostenant-devices.m_product_loan load into ubuntu.airtel_uganda_device_financing.product_dimension</li>
    <li>Extract from mifostenant-devices.m_client load into ubuntu.airtel_uganda_device_financing.client_dimension</li>
    <li>Extract from mifostenant-devices.r_enum_value load into ubuntu.airtel_uganda_device_financing.loan_type_dimension</li>
    </ol>
    """
    common_params = {
        'airtel_uganda_office_id': '{{airtel_uganda_office_id}}',
        'airtel_uganda_device_financing_product_ids': default_args['airtel_uganda_device_financing_product_ids']
    }

    def extract_load_client_dimension(**context) -> None:
        """
        Extracts from mifostenant-devices.m_client and loads into ubuntu.airtel_uganda_device_financing.client_dimension
        """
        mifos_tenant = 'devices'
        warehouse_schema = 'airtel_uganda_device_financing'

        clients = get_mifos_clients(
            mifos_tenant=mifos_tenant,
            warehouse_schema=warehouse_schema,
            mifos_filters=context['templates_dict']['airtel_uganda_office_id']
        )
        if clients is not None:
            clients['mobile_no'] = clients['mobile_no'].apply(lambda x: x[1:] if x.startswith('+') else x)

            # INSERT NEW RECORDS
            warehouse_hook.insert_rows(
                table=f'{warehouse_schema}.client_dimension',
                target_fields=[
                    'mifos_id', 'account_number', 'office_id', 'status', 'default_savings_account_id', 'gender',
                    'client_type', 'first_name', 'middle_name', 'last_name', 'mobile_number', 'date_of_birth',
                    'submitted_on_date', 'submitted_on_user_id', 'activated_on_user_id', 'activation_date',
                    'office_joining_date', 'national_id',
                ],
                replace=False,
                rows=tuple(clients[[
                    'id', 'account_no', 'office_id', 'status_enum', 'default_savings_account', 'gender_cv_id',
                    'client_type_cv_id', 'firstname', 'middlename', 'lastname', 'mobile_no', 'date_of_birth',
                    'submittedon_date', 'submittedon_userid', 'activatedon_userid', 'activation_date',
                    'office_joining_date', 'national_id'
                ]].replace({np.nan: None}).itertuples(index=False, name=None)),
                commit_every=1000
            )

    def clean_firstname_lastname(**context):

        def firstname_split(firstname: str) -> list:
            """
            split the firstname column on ' ' to populate middlename and lastname
            :param firstname: string
            :return: a list containing the firstname, middlename and lastname of a client
            """
            try:
                split = firstname.strip().split()
                if len(split) > 3:
                    lastname = ' '.join(split[2:])
                    del split[2:]
                    split.append(lastname)
                elif len(split) == 1:
                    split += [None, None]
                elif len(split) == 2:
                    split += [None]
                return split
            except AttributeError as e:
                # firstname is of np.NaN type
                if np.isnan(firstname):
                    return [None, None, None]
                else:
                    print(firstname)
                    raise AttributeError

        data = warehouse_hook.get_pandas_df(
            sql="""
                    select mifos_id, national_id, first_name, middle_name, last_name
                    from airtel_uganda_device_financing.client_dimension
                    WHERE middle_name is null and first_name is not null
                """
        )

        data.replace(r'^\s*$', np.nan, regex=True, inplace=True)

        # first name is same as national id
        data.iloc[data[data['national_id'] == data['first_name']].index, data.columns.get_loc('first_name')] = np.NAN

        # last name is same as national id
        data.iloc[data[data['national_id'] == data['last_name']].index, data.columns.get_loc('last_name')] = np.NAN

        data['middle_name'] = data['first_name'].apply(
            lambda x: firstname_split(x)[1]
        )
        data['last_name'] = data['first_name'].apply(
            lambda x: firstname_split(x)[2]
        )
        data['first_name'] = data['first_name'].apply(
            lambda x: firstname_split(x)[0]
        )

        for index, row in data.replace({np.NAN: None}).iterrows():
            warehouse_hook.run(
                sql="""
                        UPDATE airtel_uganda_device_financing.client_dimension
                        SET first_name = %(first_name)s, middle_name = %(middle_name)s, last_name = %(last_name)s
                        WHERE mifos_id = %(id)s
                    """,
                parameters={
                    'first_name': row['first_name'],
                    'middle_name': row['middle_name'],
                    'last_name': row['last_name'],
                    'id': row['mifos_id']
                }
            )

    def extract_load_product_dimension(**context) -> None:
        """
        Extracts from mifostenant-safaricom.m_product_loan and loads into ubuntu.bloomlive.product_dimension

        :param context: Airflow context dictionary
        :type context: dict
        """
        warehouse_schema = 'airtel_uganda_device_financing'
        mifos_tenant = 'devices'

        # Extract data from mifostenant-default.m_product_loan
        all_products = get_mifos_products_for_dimension(
            warehouse_schema=warehouse_schema,
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}'),
            mifos_filters=f"where id in {tuple(list(context['templates_dict']['airtel_uganda_device_financing_product_ids']))}"
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
        warehouse_schema = 'airtel_uganda_device_financing'
        mifos_tenant = 'devices'

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
        warehouse_schema = 'airtel_uganda_device_financing'
        mifos_tenant = 'devices'

        # Extract data from Mifos for staff dimension
        el_staff_dimension(
            warehouse_schema=warehouse_schema,
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}')
        )

    def extract_load_transactions_dimension(**context):
        """
        Extracts from mifostenant-tanda.m_payment_detail and loads into ubuntu.jumia.transactions_dimension
        """
        mifos_tenant = 'devices'
        warehouse_schema = 'airtel_uganda_device_financing'
        transaction_types = [1, 2]
        office_id = context['templates_dict']['airtel_uganda_office_id']

        el_transactions_dimension(
            mifos_tenant=mifos_tenant,
            warehouse_schema=warehouse_schema,
            transaction_types=transaction_types,
            office_id=office_id,
            year=context['dag_run'].conf.get('year', datetime.datetime.now().year),
            month=context['dag_run'].conf.get('month', datetime.datetime.now().month)
        )

    def trigger_ETL_Airtel_UG_loans_fact_table(**context):
        from utils.common import trigger_dag_remotely
        trigger_dag_remotely(dag_id='ETL_Airtel_UG_loans_fact_table', conf={})

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
        templates_dict=common_params
    )
    t5 = PythonOperator(
        task_id='clean_firstname_lastname',
        provide_context=True,
        python_callable=clean_firstname_lastname,
    )
    t8 = PythonOperator(
        task_id='extract_load_transactions_dimension',
        provide_context=True,
        python_callable=extract_load_transactions_dimension,
        templates_dict=common_params
    )
    t7 = PythonOperator(
        task_id='trigger_ETL_Airtel_UG_loans_fact_table',
        provide_context=True,
        python_callable=trigger_ETL_Airtel_UG_loans_fact_table
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t8 >> t7
