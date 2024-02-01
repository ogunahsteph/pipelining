import io
import os
import sys
import logging
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

from utils.common import on_failure, get_mifos_clients, el_transactions_dimension, product_dimension_process_new_products, el_loan_status_dimension, el_staff_dimension, get_mifos_loan_officer_assignment_history
from utils.office365_api import get_access_token, read_file, get_children

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-safaricom')
airflow_hook = MySqlHook(mysql_conn_id='mysql_airflow', schema='monitoring')
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
    'on_failure_callback': on_failure,
    'bloom2_product_ids': [1, 2, 3]
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_bloom2_dimensions',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 */2 * * *',
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
    <li>Extract from mifostenant-safaricom.m_staff load into ubuntu.bloomlive.staff_dimension</li>
    <li>Extract from mifostenant-safaricom.m_product_loan load into ubuntu.bloomlive.product_dimension</li>
    <li>Extract from mifostenant-safaricom.m_client load into ubuntu.bloomlive.client_dimension</li>
    <li>Extract from mifostenant-safaricom.r_enum_value load into ubuntu.bloomlive.loan_type_dimension</li>
    </ol>
    """

    def extract_load_client_dimension(**context) -> None:
        """
        Extracts data from Mifos Tenant Safaricom, table m_client and loads into bloomlive.client_dimension.
        :param context: Airflow task context
        """

        # Extract data from mifostenant-safaricom.m_client table
        clients = get_mifos_clients(
            mifos_tenant='safaricom',
            warehouse_schema='bloomlive',
            warehouse_filters="bloom_version = '2'"
        )
        if clients is not None:
            clients.rename(columns={'middlename': 'store_number'}, inplace=True)
            clients['middlename'] = np.NAN
            # Clean and standardize client names
            def get_names(x):
                names = [''] * 10
                count = 0
                for name in [x for x in x['lastname'].replace('-', '').strip().split(' ') if x]:
                    names[count] = name
                    count += 1

                clients.loc[x.name, 'firstname'] = names[0]
                clients.loc[x.name, 'middlename'] = names[1]
                clients.loc[x.name, 'lastname'] = ' '.join(names[2:]).strip()

            # Apply name cleaning function to dataframe
            clients.copy().apply(
                lambda x: get_names(x),
                axis=1
            )

            # Add additional columns
            clients['bloom_version'] = '2'

            # Insert new records into client_dimension table
            insert_new_records(
                table_name='bloomlive.client_dimension',
                target_fields=[
                    'mifos_id', 'account_number', 'national_id', 'office_id', 'status', 'default_savings_account_id',
                    'gender',
                    'client_type', 'first_name', 'middle_name', 'store_number', 'last_name', 'mobile_number',
                    'date_of_birth',
                    'submitted_on_date', 'submitted_on_user_id', 'activated_on_user_id', 'activation_date',
                    'office_joining_date', 'bloom_version'],
                rows=tuple(clients[[
                    'id', 'account_no', 'national_id', 'office_id', 'status_enum', 'default_savings_account',
                    'gender_cv_id',
                    'client_type_cv_id', 'firstname', 'middlename', 'store_number', 'lastname', 'mobile_no',
                    'date_of_birth',
                    'submittedon_date', 'submittedon_userid', 'activatedon_userid', 'activation_date',
                    'office_joining_date', 'bloom_version'
                ]].replace({np.nan: None}).itertuples(index=False, name=None)),
                postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                schema='afsg_ds_prod_postgresql_dwh'
            )

            # Refresh materialized view
            warehouse_hook.run(
                sql="""refresh materialized view concurrently bloomlive.client_materialized_summary_view""")


    def extract_load_product_dimension(**context) -> None:
        """
        Extracts from mifostenant-safaricom.m_product_loan and loads into ubuntu.bloomlive.product_dimension

        :param context: Airflow context dictionary
        :type context: dict
        """

        from utils.common import get_mifos_products_for_dimension
        warehouse_schema = 'bloomlive'
        mifos_tenant = 'safaricom'

        # Extract data from mifostenant-default.m_product_loan
        all_products = get_mifos_products_for_dimension(
            warehouse_schema=warehouse_schema,
            warehouse_filters="bloom_version = '2'",
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}'),
            mifos_filters=f"where id in {tuple(list(context['templates_dict']['bloom2_product_ids']))}"
        )

        # Separate data into products, loans_fact_table_updates, inserted_records, and expired_products_updates
        products = all_products['products']
        loans_fact_table_updates = all_products['loans_fact_table_updates']
        expired_products_updates = all_products['expired_products_updates']
        changed_records = all_products['changed_records']
        # Update the changed products already inserted into the warehouse with a bloom version value of 2

        if len(changed_records) > 0:
            iterables = ', '.join("{0}".format(w) for w in changed_records)
            warehouse_hook.run(
                sql=f"update bloomlive.product_dimension set bloom_version = '2' where surrogate_id in ({iterables})")

        products['bloom_version'] = '2'

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
        mifos_tenant = 'safaricom'

        # Get new loan statuses from Mifos
        el_loan_status_dimension(
            warehouse_schema=warehouse_schema,
            warehouse_filters="bloom_version = '2'",
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
        mifos_tenant = 'safaricom'

        # Extract data from Mifos for staff dimension
        el_staff_dimension(
            warehouse_schema=warehouse_schema,
            warehouse_filters="where bloom_version = '2'",
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

    def trigger_ETL_bloom2_loans_fact_table(**context):
        """
        Trigger the ETL bloom2 loans fact table DAG using the Airflow REST API.
        :param context: The context dictionary used in Airflow to pass state between tasks.
        :return: None
        """
        from utils.common import trigger_dag_remotely
        trigger_dag_remotely(dag_id='ETL_bloom2_loans_fact_table', conf={})

    def extract_load_defaulters_dimension(**context):
        """
        Extracts, processes and loads data from the latest file in a folder of defaulters data to a target table.

        Args:
        - **context: Variable keyword argument.

        Returns: None.
        """

        # Get the latest date of the file loaded into the target table
        max_file_date = warehouse_hook.get_pandas_df(
            sql="""select max(file_date) as file_date from bloomlive.defaulters_dimension"""
        ).iloc[0]['file_date']

        # Define the payload dictionary containing information about the site and folder to extract data from
        payload = {
            'site': {'name': 'AsanteBloomDefaultersList', 'id': 'b91dfc87-5643-4a6f-90d3-af017d03896c'},
            'folder': {
                'description': 'defaulters folder',
                'path': '/Documents/',
                'id': '01BDVLPEV6Y2GOVW7725BZO354PWSELRRZ'
            }
        }

        # Get access token to authenticate the API call and get list of files in the folder
        access_token = get_access_token()
        files = get_children(token=access_token, site_id=payload['site']['id'], item_id=payload['folder']['id'])

        # Sort the files by creation date, get the latest one and extract the date from its name
        latest_file = sorted(files, key=lambda d: d['createdDateTime'], reverse=True)[0]
        latest_file['file_date'] = datetime.datetime.strptime(latest_file['name'].split('_')[2].split('.')[0],
                                                              '%Y-%m-%d').date()

        # Define a function to load data from a file to the target table
        def load_data(file: dict) -> None:
            """
            Loads data from a file to the target table.

            Args:
            - file: Dictionary containing information about the file to extract data from.

            Returns: None.
            """

            # Download the file and read it as a Pandas DataFrame
            data = pd.read_csv(io.BytesIO(read_file(site=payload['site']['id'], file_id=file['id'], access_token=access_token)))
            # Select and rename relevant columns from the data
            data['file_date'] = file['file_date']
            data = data[[
                'merchant_name', 'till_name', 'mobilenumber', 'accountnumber', 'customeridnumber', 'loannumber',
                't24ref', 'amount', 'balance', 'disbursement_ref', 'loanterm', 'datetimeadded',
                'repaymentdate', 'loanstatus', 'file_date'
            ]]

            # Insert the data into the target table
            warehouse_hook.insert_rows(
                table='bloomlive.defaulters_dimension',
                target_fields=[
                    'merchant_name', 'till_name', 'mobile_number', 'account_number', 'customer_id_number',
                    'loan_number',
                    'transaction_ref', 'amount', 'balance', 'disbursement_ref', 'loan_term', 'date_time_added',
                    'repayment_date', 'loan_status', 'file_date'
                ],
                rows=tuple(data.replace({np.NAN: None}).itertuples(index=False)),
                commit_every=100
            )

        # Check if there is data already loaded to the target table
        if not pd.isnull(max_file_date):
            # If there is, compare the file dates to determine if the latest file should be loaded
            if datetime.datetime.strptime(max_file_date, '%Y-%m-%d').date() < latest_file['file_date']:
                # If the latest file should be loaded, delete the existing data and load the new data
                warehouse_hook.run(
                    sql=f"""delete from bloomlive.defaulters_dimension where file_date = '{max_file_date}'""")
                load_data(latest_file)
        else:
            # If there is no existing data in the target table, load the latest file data
            load_data(latest_file)

    def extract_load_location_mapping(**context):
        """
        Extracts, processes and loads data from the latest file in a folder of defaulters data to a target table.

        Args:
        - **context: Variable keyword argument.

        Returns: None.
        """

        # Define the payload dictionary containing information about the site and folder to extract data from
        payload = {
            'site': {'name': 'Safaricom Bloom', 'id': 'ea835111-82a4-467d-81ed-3c5994f4d899'},
            'file': {'name': 'Location_Mapped_Merchants_20230516.xlsx', 'id': '01YAZIWULR7QLZ5RK32ZB2ARGQ2T6PKZK2'}
        }

        # Get access token to authenticate the API call and get list of files in the folder
        access_token = get_access_token()

        dt = pd.read_excel(read_file(site=payload['site']['id'], file_id=payload['file']['id'], access_token=access_token))

        warehouse_hook.insert_rows(
            table='bloomlive.location_mapping',
            target_fields=['short_code', 'till_number', 'nominated_number', 'latitude', 'longitude', 'business_name',
                           '_type'],
            replace=True,
            replace_index=['short_code', 'till_number'],
            rows=tuple(dt[[
                'SHORT_CODE', 'TILL_NUMBER', 'NOMINATED_NUMBER', 'Latitude', 'Longitude',
                'Business Name (Based on Visit)', 'Type'
            ]].replace({np.nan: None}).itertuples(index=False, name=None)),
            commit_every=0
        )

    def extract_load_till_suspension_dimension(**context):
        """
        This function is used to extract the Till Suspension data from the sharepoint and load it to the warehouse.
        :param context: The context variable used to pass params to the function
        :return: None
        """

        def compare_tills(filtered_row, existing_stores):
            """
            Function used to compare the till data from the previous time to the new data
            :param filtered_row: The row of data to compare
            :param existing_stores: The existing data
            :return: boolean
            """
            filtered = existing_stores[(existing_stores['store_number'] == filtered_row['store_number']) & (
                    existing_stores['is_suspended'] == filtered_row['is_suspended'])].drop(
                columns=['store_number', 'id'])

            if filtered.shape[0] > 0:
                prev_till = filtered.iloc[0]

                return filtered_row.drop(labels=['store_number']).reindex(
                    sorted(filtered_row.drop(labels=['store_number']).index)).equals(
                    prev_till.reindex(sorted(prev_till.index)))

        # payload for the sharepoint connection
        payload = {
            'site': {'name': 'DSGrafanaTest', 'id': '2be7256e-c06d-40e9-9fe5-0d15b48ead72'},
            'folder': {
                'description': 'till suspension folder',
                'path': '/General/Bloom/Till Suspension/',
                'id': '01IBHK7MPESI4B5HKCOFAYADXMS3WW2HRO'
            }
        }
        sharepoint_access_token = get_access_token()
        files = get_children(token=sharepoint_access_token, site_id=payload['site']['id'],
                             item_id=payload['folder']['id'])

        for file in files:
            # Read excel data and convert store_number column to int
            data = pd.read_excel(read_file(site=payload['site']['id'], file_id=file['id'], access_token=sharepoint_access_token)).astype({'store_number': int})

            # Convert date columns to datetime.date format, create new columns, and update is_suspended column
            if 'start_date' in data.columns.tolist():
                data['start_date'] = data['start_date'].dt.date
                data['end_date'] = np.NAN
                data['is_suspended'] = True
            elif 'end_date' in data.columns.tolist():
                data['end_date'] = data['end_date'].dt.date
                data['start_date'] = np.NAN
                data['is_suspended'] = False

            data.dropna(subset=['is_suspended']).replace({np.NAN: None}, inplace=True)

            # Get existing suspended tills data from till_suspension_dimension table
            existing_stores = warehouse_hook.get_pandas_df(
                sql="""
                    select id, store_number::int, is_suspended, start_date, end_date
                    from bloomlive.till_suspension_view where store_number in %(store_numbers)s
                """,
                parameters={'store_numbers': tuple([str(x) for x in data['store_number'].tolist()])}
            )

            # Merge dataframes to get suspended tills data that is new or has changed
            suspended_tills = data[data['is_suspended']].merge(existing_stores[existing_stores['is_suspended']],
                                                               left_on='store_number', right_on='store_number',
                                                               how='left')
            suspended_tills = suspended_tills.drop(
                index=suspended_tills[suspended_tills['start_date_y'] <= suspended_tills['start_date_x']].index)[
                ['store_number', 'start_date_x', 'end_date_x', 'is_suspended_x']].rename(
                columns={'start_date_x': 'start_date', 'end_date_x': 'end_date', 'is_suspended_x': 'is_suspended'})

            # Merge dataframes to get unsuspended tills data that is new or has changed
            unsuspended_tills = data[~data['is_suspended']].merge(existing_stores[~existing_stores['is_suspended']],
                                                                  left_on='store_number', right_on='store_number',
                                                                  how='left')
            unsuspended_tills.drop(
                index=unsuspended_tills[unsuspended_tills['end_date_y'] <= unsuspended_tills['end_date_x']].index,
                inplace=True)

            # Combine suspended and unsuspended tills into one dataframe
            data = pd.concat([suspended_tills, unsuspended_tills])[
                ['store_number', 'start_date_x', 'end_date_x', 'is_suspended_x']
            ].rename(columns={'start_date_x': 'start_date', 'end_date_x': 'end_date', 'is_suspended_x': 'is_suspended'})

            if data.shape[0] > 0:
                # Add timestamp to data
                data['record_created_on_timestamp'] = datetime.datetime.now()

                # Compare tills and filter out versions that match
                data['versions_match'] = data.apply(
                    lambda x: compare_tills(
                        filtered_row=x.drop(labels=[col for col in data.columns if
                                                    col not in ['is_suspended', 'start_date', 'end_date',
                                                                'store_number']]),
                        existing_stores=existing_stores
                    ) if x['store_number'] in existing_stores['store_number'].tolist() else np.NAN,
                    axis=1
                )
                data = data[~data['versions_match'].isin([True])]

                # Update expired records
                updates = []
                for index, row in data[~data['versions_match'].isna()].replace({np.NAN: 'null'}).iterrows():
                    expired_on = str(row['record_created_on_timestamp'])
                    prev_till_id = int(
                        existing_stores[existing_stores['store_number'] == row['store_number']].iloc[0]['id'])

                    updates.append(
                        f"update bloomlive.till_suspension_dimension set record_expired_on_timestamp = '{str(expired_on)}', is_most_recent_record = false where id={int(prev_till_id)}"
                    )

                # Update database with new data
                if len(updates) > 0:
                    warehouse_hook.run(sql=updates)

                data.dropna(subset=['is_suspended'], inplace=True)
                warehouse_hook.insert_rows(
                    table='bloomlive.till_suspension_dimension',
                    target_fields=['store_number', 'is_suspended', 'start_date', 'end_date',
                                   'record_created_on_timestamp'],
                    replace=False,
                    rows=tuple(data[~data['store_number'].isna()][
                                   ['store_number', 'is_suspended', 'start_date', 'end_date',
                                    'record_created_on_timestamp']].replace(
                        {np.NAN: None}).itertuples(index=False)),
                    commit_every=100
                )

    def extract_load_limit_reinstatement_dimension(**context):
        """
        Extracts data from a limit reinstatement file, compares the data to existing data in a SQL database,
        and updates the SQL database with any new data. The updated data is then returned as a pandas dataframe

        Args:
            context: context object containing metadata and other data

        Returns:
            A pandas dataframe containing updated limit reinstatement data.

        """

        # sharepoint metadata
        payload = {
            'site': {'name': 'Safaricom Bloom', 'id': 'ea835111-82a4-467d-81ed-3c5994f4d899'},
            'file': {
                'description': 'limit reinstatement file',
                'path': '/Bloom 2.0 Reinstate Limits/Bloom 2.0 Reinstate Limits List.xlsx',
                'id': '01YAZIWUM2A6J3KOXRPBDZ5UJK46T73USL'
            }
        }

        # get access token for sharepoint
        sharepoint_access_token = get_access_token()

        # read data from excel file
        data = pd.read_excel(read_file(site=payload['site']['id'], file_id=payload['file']['id'], access_token=sharepoint_access_token))

        # clean up data
        data['store_number'] = data['store_number'].apply(
            lambda x: str(x).replace('.0', '') if not pd.isnull(x) else x)
        data['modified_on_date'] = data['modified_on_date'].apply(
            lambda x: datetime.datetime.strptime(str(x).split(' ')[0], '%Y-%m-%d').date()
        )
        warehouse_hook.insert_rows(
            table='bloomlive.limit_reinstatement_dimension',
            target_fields=['store_number', 'update_flag', 'modified_on_date', 'reinstatement_reason'],
            replace=True,
            rows=tuple(data[['store_number', 'update_flag', 'modified_on_date', 'reinstatement_reason']].replace(
                {np.NAN: None}).itertuples(index=False)),
            commit_every=100,
            replace_index = ['store_number']
        )

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
        mifos_tenant = 'safaricom'
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
        Extracts from mifostenant-safaricom.m_payment_detail and loads into ubuntu.bloomlive.transactions_dimension
        """

        # Get the loan officer assignment history data
        assignment_history = get_mifos_loan_officer_assignment_history(
            mifos_tenant='safaricom',
            warehouse_schema='bloomlive',
            filters="WHERE bloom_version = '2'",
            warehouse_hook=PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh'),
            mifos_hook=MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-safaricom'),
        )

        # Check if there are any records in the data
        if assignment_history.shape[0] > 0:
            assignment_history['bloom_version'] = '2'

            # INSERT NEW RECORDS
            # Insert new records into the loan_officer_assignment_history table
            insert_new_records(
                table_name='bloomlive.loan_officer_assignment_history',
                target_fields=[
                    'bloom_version', 'mifos_id', 'loan_mifos_id',
                    'loan_officer_mifos_id', 'start_date', 'end_date',
                    'created_by_id', 'created_date', 'last_modified_date',
                    'last_modified_by_id'
                ],
                # Get the rows to insert and replace NaN values with None
                rows=tuple(assignment_history[[
                    'bloom_version', 'mifos_id', 'loan_mifos_id',
                    'loan_officer_mifos_id', 'start_date', 'end_date',
                    'created_by_id', 'created_date', 'last_modified_date',
                    'last_modified_by_id'
                ]].replace({np.nan: None}).itertuples(index=False, name=None)),
                postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                schema='afsg_ds_prod_postgresql_dwh'
            )

    def extract_load_raw_mpesa_transactions(**context):
        """"""

        # Define the payload dictionary containing information about the site and folder to extract data from
        payload = {
            'site': {'name': 'Safaricom Bloom', 'id': 'ea835111-82a4-467d-81ed-3c5994f4d899'},
            'folders': [
                {'path': '/Bloom Collections - Inhouse/MPESA Statements/Disbursements/', 'id': '01YAZIWUK7QRMJF7MSBRHZFQDQHPBLRGEU'},
                {'path': '/Bloom Collections - Inhouse/MPESA Statements/Repayments/', 'id': '01YAZIWUPZ5IGJOV7PWBHIVYZCC6C5XVMN'}
            ]
        }

        # Get access token to authenticate the API call and get list of files in the folder
        access_token = get_access_token()

        for folder in payload['folders']:
            files = get_children(token=access_token, site_id=payload['site']['id'], item_id=folder['id'])

            # Sort the files by creation date, get the latest one and extract the date from its name
            latest_file = sorted(files, key=lambda d: d['createdDateTime'], reverse=True)[0]


            try:
                dt = pd.read_excel(
                    read_file(site=payload['site']['id'], file_id=latest_file['id'], access_token=access_token),
                    skiprows=6).rename(columns={
                    'Receipt No.': 'receipt_number', 'Completion Time': 'completion_time',
                    'Initiation Time': 'initiation_time',
                    'Transaction Status': 'transaction_status', 'Paid In': 'paid_in',
                    'Balance Confirmed': 'balance_confirmed',
                    'Reason Type': 'reason_type', 'Other Party Info': 'other_party_info',
                    'Linked Transaction ID': 'linked_transaction_id',
                    'A/C No.': 'account_number',
                })
            except ValueError:
                try:
                    dt = pd.read_csv(io.BytesIO(
                        read_file(site=payload['site']['id'], file_id=latest_file['id'], access_token=access_token)),
                        skiprows=6).rename(columns={
                        'Receipt No.': 'receipt_number', 'Completion Time': 'completion_time',
                        'Initiation Time': 'initiation_time',
                        'Transaction Status': 'transaction_status', 'Paid In': 'paid_in',
                        'Balance Confirmed': 'balance_confirmed',
                        'Reason Type': 'reason_type', 'Other Party Info': 'other_party_info',
                        'Linked Transaction ID': 'linked_transaction_id',
                        'A/C No.': 'account_number',
                    })
                except pd.errors.EmptyDataError:
                    logging.warning("Empty Files")

            dt['initiation_time'] = dt['initiation_time'].apply(lambda x: datetime.datetime.strptime(str(x), '%d-%m-%Y %H:%M:%S'))
            dt['completion_time'] = dt['completion_time'].apply(lambda x: datetime.datetime.strptime(str(x), '%d-%m-%Y %H:%M:%S'))

            dt['is_disbursement'] = True if 'Disbursement' in latest_file['name'] else False
            dt['is_repayment'] = True if 'Repayment' in latest_file['name'] else False
            warehouse_hook.insert_rows(
                table='bloomlive.raw_mpesa_transactions',
                target_fields=dt.reindex().columns.tolist(),
                replace=True,
                replace_index=['receipt_number', 'details'],
                rows=tuple(dt.reindex().replace({np.NAN: None}).itertuples(index=False)),
                commit_every=100
            )


    common_params = {'bloom2_product_ids': default_args['bloom2_product_ids']}

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
    t7 = PythonOperator(
        task_id='extract_load_transactions_dimension',
        provide_context=True,
        python_callable=extract_load_transactions_dimension
    )
    t6 = PythonOperator(
        task_id='extract_load_till_suspension_dimension',
        provide_context=True,
        python_callable=extract_load_till_suspension_dimension,
    )
    t8 = PythonOperator(
        task_id='extract_load_defaulters_dimension',
        provide_context=True,
        python_callable=extract_load_defaulters_dimension
    )
    t9 = PythonOperator(
        task_id='extract_load_loan_officer_assignment_history_dimension',
        provide_context=True,
        python_callable=extract_load_loan_officer_assignment_history_dimension
    )
    t10 = PythonOperator(
        task_id='extract_load_limit_reinstatement_dimension',
        provide_context=True,
        python_callable=extract_load_limit_reinstatement_dimension
    )
    t13 = PythonOperator(
        task_id='extract_load_location_mapping',
        provide_context=True,
        python_callable=extract_load_location_mapping
    )
    t14 = PythonOperator(
        task_id='extract_load_raw_mpesa_transactions',
        provide_context=True,
        python_callable=extract_load_raw_mpesa_transactions
    )
    t5 = PythonOperator(
        task_id='trigger_ETL_bloom2_loans_fact_table',
        provide_context=True,
        python_callable=trigger_ETL_bloom2_loans_fact_table
    )

    t1 >> t2 >> t3 >> t4 >> t7 >> t6 >> t8 >> t9 >> t10 >> t13 >> t14 >> t5

