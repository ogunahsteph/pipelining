import os
import sys
import json
import datetime
import pendulum
import numpy as np
import pandas as pd
from airflow import DAG
from datetime import timedelta
from sqlalchemy.exc import IntegrityError
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure, get_mifos_clients, el_transactions_dimension, el_staff_dimension, el_loan_status_dimension, get_mifos_products_for_dimension, product_dimension_process_new_products


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'twiga_fund_id': 4,
    'twiga_office_id': 4,
    'on_failure_callback': on_failure,
    'twiga_product_ids': [10]
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_Twiga_dimensions',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 2 * * *',
        start_date=datetime.datetime(2021, 9, 29, 2, 00, tzinfo=local_tz),
        tags=['extract_load'],
        description='Load data into twiga dimensions',
        user_defined_macros=default_args
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Extracts data from MIFOSX and adds non existent rows into various tables in ubuntu
    DAG is set to run daily at 2 am.
    
    #### Actions
    <ol>
    <li>Extract from mifostenant-tanda.m_staff load into ubuntu.twiga.staff_dimension</li>
    <li>Extract from mifostenant-tanda.m_product_loan load into ubuntu.twiga.product_dimension</li>
    <li>Extract from mifostenant-tanda.m_client load into ubuntu.twiga.client_dimension</li>
    <li>Extract from mifostenant-tanda.r_enum_value load into ubuntu.twiga.loan_status_dimension</li>
    <li>Extract from mifostenant-tanda.r_enum_value load into ubuntu.twiga.loan_type_dimension</li>
    </ol>
    """

    common_params = {
        'twiga_fund_id': '{{twiga_fund_id}}',
        'twiga_office_id': '{{twiga_office_id}}',
        'twiga_product_ids': default_args['twiga_product_ids']
    }


    def get_existing_ids(table_name) -> tuple:
        """Retrieve ids for rows existing in ubuntu.twiga tables:
        client_dimension, product_dimension, staff_dimension,
        loan_type_dimension, loan_status_dimension
        USED TO PREVENT OVERWRITE - CAN BE REPLACED WITH ON CONFLICT - ***assumption that this has better performance***
        """
        warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

        df = warehouse_hook.get_pandas_df(
            sql="SELECT mifos_id FROM {}".format(table_name)
        )

        return tuple(df['mifos_id'].tolist())


    def insert_new_records(table_name, target_fields, rows):
        warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
        warehouse_hook.insert_rows(
            table=table_name,
            target_fields=target_fields,
            replace=False,
            rows=rows,
            commit_every=0
        )


    def extract_load_client_dimension(**context) -> None:
        """
        Extracts only those clients that belong to Twiga from mifostenant-tanda.m_client
        and loads them into ubuntu.twiga.client_dimension
        """
        mifos_tenant = 'tanda'
        warehouse_schema = 'twiga'

        clients = get_mifos_clients(
            mifos_tenant=mifos_tenant,
            warehouse_schema=warehouse_schema,
            mifos_filters=f"office_id = {context['templates_dict']['twiga_office_id']}"
        )

        if clients is not None:
            def firstname_split(firstname: str) -> list:
                """
                split the firstname column on ' ' to populate middlename and lastname
                :param firstname: string
                :return: a list containing the firstname, middlename and lastname of a client
                """
                split = firstname.split()
                if len(split) > 3:
                    lastname = ' '.join(split[2:])
                    del split[2:]
                    split.append(lastname)
                elif len(split) == 1:
                    split += [None, None]
                elif len(split) == 2:
                    split += [None]
                return split

            clients['national_id'] = clients['lastname'].apply(
                lambda x: x[1:]
            )  # TRANSFORM LASTNAME TO NATIONAL ID

            clients['middlename'] = clients['firstname'].apply(
                lambda x: firstname_split(x)[1]
            )
            clients['lastname'] = clients['firstname'].apply(
                lambda x: firstname_split(x)[2]
            )
            clients['firstname'] = clients['firstname'].apply(
                lambda x: firstname_split(x)[0]
            )
            # INSERT NEW RECORDS
            insert_new_records(
                table_name='twiga.client_dimension',
                target_fields=[
                    'id', 'account_number', 'office_id', 'status', 'default_savings_account_id', 'gender',
                    'client_type', 'first_name', 'middle_name', 'last_name', 'mobile_number',
                    'date_of_birth', 'submitted_on_date', 'submitted_on_user_id', 'activated_on_user_id',
                    'activation_date', 'office_joining_date', 'national_id'
                ],
                rows=tuple(clients[[
                    'id', 'account_no', 'office_id', 'status_enum', 'default_savings_account', 'gender_cv_id',
                    'client_type_cv_id', 'firstname', 'middlename', 'lastname', 'mobile_no',
                    'date_of_birth', 'submittedon_date', 'submittedon_userid', 'activatedon_userid',
                    'activation_date', 'office_joining_date', 'national_id'
                ]].replace({np.nan: None}).itertuples(index=False, name=None)),
            )

    def standardize_mobile_number(**context):
        """
        converts mobile numbers from format '0' to format '+254'
        """
        destination_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
        data = destination_hook.get_pandas_df(
            sql="""
                SELECT mifos_id, mobile_number
                FROM twiga.client_dimension
                WHERE mobile_number LIKE '0%' OR mobile_number LIKE '2%'
            """
        )

        def standardize(phone_number):
            if phone_number[0] == '0':
                phone_number = '+254' + phone_number[1:]
            elif phone_number[0] == '2':
                phone_number = '+' + phone_number
            return phone_number

        data['mobile_number'] = data['mobile_number'].apply(
            lambda x: standardize(x)
        )

        # UPDATE RECORDS
        for index, row in data.iterrows():
            destination_hook.run(
                sql="""
                    UPDATE twiga.client_dimension
                    SET mobile_number = '{}'
                    WHERE mifos_id = {}
                """.format(
                    row['mobile_number'],
                    row['mifos_id']
                )
            )

    def extract_load_product_dimension(**context) -> None:
        """
        Extracts from mifostenant-safaricom.m_product_loan and loads into ubuntu.bloomlive.product_dimension

        :param context: Airflow context dictionary
        :type context: dict
        """
        mifos_tenant = 'tanda'
        warehouse_schema = 'twiga'

        iterables = ', '.join("{0}".format(w) for w in list(context['templates_dict']['twiga_product_ids']))
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
        mifos_tenant = 'tanda'
        warehouse_schema = 'twiga'

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
        warehouse_schema = 'twiga'
        mifos_tenant = 'tanda'

        # Extract data from Mifos for staff dimension
        el_staff_dimension(
            warehouse_schema=warehouse_schema,
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}')
        )

    def extract_load_client_opt_in_dimension(**context):
        """
        Extracts data from twiga.user_customer (server Twiga_server) and loads it into
        twiga.client_opt_in_dimension (server 157.245.248.249)
        """
        mifos_hook = MySqlHook(mysql_conn_id='Twiga_server', schema='twiga')
        destination_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

        if len(mifos_hook.get_records(
            sql="""
                select count(distinct(customer_id)) from twiga.user_customer;
            """
        )) != len(mifos_hook.get_records(
            sql="""
                select count(*) from twiga.user_customer
            """
        )):
            # Check for error in OLTP database: Duplicated customers
            raise IntegrityError

        existing_records = list(destination_hook.get_pandas_df(
            sql="SELECT customer_id FROM twiga.client_opt_in_dimension"
        )['customer_id'].values)

        if len(existing_records) > 0:
            iterables = ', '.join("'{0}'".format(w) for w in existing_records)
            data = mifos_hook.get_pandas_df(
                sql="""
                    SELECT
                        customer_id, opt_in_date, approved, date_created, last_delivery_date,
                        delivery_total_count, delivery_total_value
                    FROM user_customer
                    WHERE customer_id not IN ({})
                """.format(
                    iterables
                )
            )
        else:
            data = mifos_hook.get_pandas_df(
                sql="""
                    SELECT
                        customer_id, opt_in_date, approved, date_created, last_delivery_date,
                        delivery_total_count, delivery_total_value
                    FROM user_customer
                """
            )
        #
        if data[data['approved'] != 'TRUE'].shape[0] > 0:
            raise IntegrityError

        data['approved'] = data['approved'].apply(
            lambda x: True if x == 'TRUE' else False
        )

        insert_new_records(
            table_name='twiga.client_opt_in_dimension',
            target_fields=[
                'customer_id', 'opt_in_date', 'approved', 'date_created', 'last_delivery_date',
                'delivery_total_count', 'delivery_total_value'
            ],
            rows=tuple(data.replace(r'^\s*$', np.nan, regex=True).replace({np.nan: None}).itertuples(index=False, name=None)),
        )

    def add_opt_in_key_to_client_dimension(**context):
        mifos_hook = MySqlHook(mysql_conn_id='Twiga_server', schema='twiga')
        destination_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

        clients_without_customer_ids = destination_hook.get_pandas_df(
            sql="""
                SELECT national_id
                FROM twiga.client_dimension
                WHERE opt_in_key is null and national_id is not null;
            """
        )

        if clients_without_customer_ids.shape[0] > 0:
            iterables = ', '.join("'{0}'".format(w) for w in list(clients_without_customer_ids['national_id'].values))
            opt_in_data = mifos_hook.get_pandas_df(
                sql="""
                        SELECT
                            id_number, customer_id, id_type, id_verified
                        FROM user_customer
                        WHERE id_number in ({})
                    """.format(
                    iterables
                )
            )

            # convert the id number to the format used in client_dimension table format
            opt_in_data.rename(columns={'id_number': 'national_id'}, inplace=True)
            opt_in_data['national_id'] = pd.Series(opt_in_data['national_id'], dtype="string")

            # check that all ids have been verified -
            # to be modified upon raise of error(anticipated to happen only once)
            if opt_in_data['id_verified'].unique() != ['1']:
                raise IntegrityError

            opt_in_data['id_verified'] = opt_in_data['id_verified'].apply(
                lambda x: True if x == '1' else False
            )

            for index, row in opt_in_data.iterrows():
                destination_hook.run(
                    sql="""
                        UPDATE twiga.client_dimension
                        SET opt_in_key = %(opt_in_key)s, id_type = %(id_type)s, id_verified = %(id_verified)s
                        WHERE national_id = %(national_id)s
                    """,
                    parameters={
                        'opt_in_key': row['customer_id'],
                        'id_type': row['id_type'],
                        'id_verified': row['id_verified'],
                        'national_id': row['national_id']
                    }
                )

    def extract_load_stall_dimension(**context):
        """
        Extracts stall ids from database server 'Twiga_server'.twiga.user_customer and stores
        the ids in ubuntu.twiga.stall_dimension
        """
        import json

        mifos_hook = MySqlHook(mysql_conn_id='Twiga_server', schema='twiga')
        destination_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

        existing_stalls = list(destination_hook.get_pandas_df(
            sql="""
                SELECT id
                FROM twiga.stall_dimension
            """
        )['id'].values)

        stalls = mifos_hook.get_pandas_df(
            sql="""
                SELECT stalls, customer_id
                FROM user_customer
            """
        )

        # remove empty strings
        stalls = stalls[stalls['stalls'].notna()]
        stalls['stalls'] = stalls['stalls'].apply(
            lambda x: x.strip()
        )
        stalls['stalls'].replace('', np.nan, inplace=True)
        stalls = stalls[stalls['stalls'].notna()]

        separated_stalls = []
        client_keys = destination_hook.get_pandas_df(
            sql="""
                SELECT mifos_id as client_key, opt_in_key as customer_id
                FROM twiga.client_dimension
                WHERE opt_in_key in {}
            """.format(
                tuple(stalls['customer_id'].values)
            )
        )

        # all those stalls for clients in warehouse
        stalls = pd.merge(
            stalls,
            client_keys,
            on='customer_id',
            how='inner'
        )

        count = 0
        for index, row in stalls.iterrows():
            for stall in json.loads(row['stalls']):
                if stall['stall_id'] not in existing_stalls:
                    separated_stalls.append(
                        pd.DataFrame({
                            'stall_id': stall['stall_id'],
                            'client_key': row['client_key']
                        }, index=[count])
                    )
                    count += 1
        if len(separated_stalls) > 0:
            new_stalls = pd.concat(separated_stalls)

            destination_hook.insert_rows(
                table='twiga.stall_dimension',
                target_fields=['id', 'client_key'],
                replace=False,
                rows=tuple(new_stalls.replace({np.nan: None}).itertuples(index=False)),
                commit_every=100
            )

    def extract_load_delivery_dimension(**context):
        mifos_hook = MySqlHook(mysql_conn_id='Twiga_server', schema='twiga')
        destination_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

        existing_records = tuple(destination_hook.get_pandas_df(
            sql="""
                SELECT id
                FROM twiga.delivery_dimension
            """
        )['id'].values)
        existing_stalls = tuple(destination_hook.get_pandas_df(
            sql="""
                    SELECT id
                    FROM twiga.stall_dimension
                """
        )['id'].values)

        if len(existing_records) > 0:
            data = mifos_hook.get_pandas_df(
                sql="""
                    SELECT 
                        delivery_id, stall_id, mifos_loan_id, receipt_no, amount,
                        till_number, delivery_date, received_date, offer_status, offer_amount,
                        limit_amount, offer_date, offer_expiry, loan_confirmation_status,
                        rejection_reason, amount_applied, date_of_request,
                        offer_response, offer_response_string, confirmation_response 
                    FROM delivery_loan_offer
                    WHERE offer_expiry < CURDATE() and delivery_id not in %(existing_records)s and stall_id in %(existing_stalls)s 
                """,
                parameters={
                    'existing_records': existing_records,
                    'existing_stalls': existing_stalls
                }
            )
        else:
            data = mifos_hook.get_pandas_df(
                sql="""
                        SELECT 
                            delivery_id, stall_id, mifos_loan_id, receipt_no, amount,
                            till_number, delivery_date, received_date, offer_status, offer_amount,
                            limit_amount, offer_date, offer_expiry, loan_confirmation_status,
                            rejection_reason, amount_applied, date_of_request,
                            offer_response, offer_response_string, confirmation_response
                        FROM delivery_loan_offer
                        WHERE offer_expiry < CURDATE() and stall_id in {}  -- only those deliveries that cannot be changed
                    """.format(
                        existing_stalls
                    )
            )

        data['rejection_reason'] = data['rejection_reason'].replace('1001', 'Duplicate').replace(
            '1002', 'Any other reason'
        ).replace('1003', 'Requested more than limit')  # rejection reasons(provided by soft engineer)

        data['offer_status'] = data['offer_status'].str.strip()
        data['mifos_loan_id'] = data['mifos_loan_id'].str.strip()
        data = data.replace('', np.nan)  # remove whitespaces

        data['mifos_loan_id'] = data['mifos_loan_id'].replace(
            'NOT POSTED', np.nan).replace(
            'REJECTED', np.nan).replace(
            '', np.nan
        )

        data['offer_response'] = data['offer_response_string'].apply(
            lambda x: json.loads(x)['status_code']
        )
        data.drop(columns=['offer_response_string'], inplace=True)

        destination_hook.insert_rows(
            table='twiga.delivery_dimension',
            target_fields=[
                'id', 'stall_key', 'loan_key', 'receipt_number', 'amount',
                'till_number', 'delivery_date', 'received_date', 'offer_status', 'offer_amount',
                'limit_amount', 'offer_date', 'offer_expiry', 'loan_confirmation_status',
                'rejection_reason_key', 'amount_applied', 'date_of_request',
                'offer_response', 'confirmation_response'
            ],
            replace=False,
            rows=tuple(data.replace({np.nan: None}).itertuples(index=False)),
            commit_every=100
        )

    def extract_load_delivery_item_dimension(**context):
        mifos_hook = MySqlHook(mysql_conn_id='Twiga_server', schema='twiga')
        destination_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

        deliveries_without_items = tuple(destination_hook.get_pandas_df(
            sql="""
                SELECT id 
                FROM twiga.delivery_dimension
                WHERE id not in (
                    SELECT delivery_key 
                    FROM twiga.delivery_item_dimension
                )
            """
        )['id'].values)

        if len(deliveries_without_items) > 0:
            iterables = ', '.join("'{0}'".format(w) for w in deliveries_without_items)
            data = mifos_hook.get_pandas_df(
                sql="""
                    SELECT delivery_items, delivery_id
                    FROM delivery_loan_offer
                    WHERE delivery_id in ({})
                """.format(
                    iterables
                )
            )

            items = []
            for index, row in data.iterrows():
                for item in json.loads(row['delivery_items'], strict=False):
                    items.append((row['delivery_id'], item['product_name'], item['product_item_name'], item['amount']))

            destination_hook.insert_rows(
                table='twiga.delivery_item_dimension',
                target_fields=[
                    'delivery_key', 'name', 'description', 'amount'
                ],
                replace=False,
                rows=tuple(items),
                commit_every=100
            )

    def extract_load_transactions_dimension(**context):
        """
        Extracts from mifostenant-tanda.m_payment_detail and loads into ubuntu.jumia.transactions_dimension
        """
        mifos_tenant = 'tanda'
        warehouse_schema = 'twiga'
        transaction_types = [1, 2]
        office_id = context['templates_dict']['twiga_office_id']

        el_transactions_dimension(
            mifos_tenant=mifos_tenant,
            warehouse_schema=warehouse_schema,
            transaction_types=transaction_types,
            office_id=office_id,
            year=context['dag_run'].conf.get('year', datetime.datetime.now().year),
            month=context['dag_run'].conf.get('month', datetime.datetime.now().month)
        )

    def trigger_ETL_Twiga_loans_fact_table(**context):
        from utils.common import trigger_dag_remotely
        trigger_dag_remotely(dag_id='ETL_Twiga_loans_fact_table', conf={})


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
        task_id='extract_load_client_dimension',
        provide_context=True,
        python_callable=extract_load_client_dimension,
        templates_dict=common_params
    )

    t4 = PythonOperator(
        task_id='standardize_mobile_number',
        provide_context=True,
        python_callable=standardize_mobile_number
    )

    t5 = PythonOperator(
        task_id='extract_load_loan_status_dimension',
        provide_context=True,
        python_callable=extract_load_loan_status_dimension
    )

    t6 = PythonOperator(
        task_id='extract_load_client_opt_in_dimension',
        provide_context=True,
        python_callable=extract_load_client_opt_in_dimension
    )

    t7 = PythonOperator(
        task_id='add_opt_in_key_to_client_dimension',
        provide_context=True,
        python_callable=add_opt_in_key_to_client_dimension
    )

    t8 = PythonOperator(
        task_id='extract_load_stall_dimension',
        provide_context=True,
        python_callable=extract_load_stall_dimension
    )

    t9 = PythonOperator(
        task_id='extract_load_delivery_dimension',
        provide_context=True,
        python_callable=extract_load_delivery_dimension
    )

    t10 = PythonOperator(
        task_id='extract_load_delivery_item_dimension',
        provide_context=True,
        python_callable=extract_load_delivery_item_dimension
    )
    t12 = PythonOperator(
        task_id='extract_load_transactions_dimension',
        provide_context=True,
        python_callable=extract_load_transactions_dimension,
        templates_dict=common_params
    )
    t11 = PythonOperator(
        task_id='trigger_ETL_Twiga_loans_fact_table',
        provide_context=True,
        python_callable=trigger_ETL_Twiga_loans_fact_table
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t12 >> t11
