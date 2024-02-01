import os
import sys
import json
import pytz
import logging
import datetime
import pendulum
import numpy as np
import pandas as pd
from airflow import DAG
from urllib import parse
from datetime import timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure, trigger_dag_remotely, store_national_id_updates, store_company_name_updates
from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
gateway_hook = MySqlHook(mysql_conn_id='gateway_server', schema='bloom_staging2')
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
}

local_tz = pendulum.timezone("Africa/Nairobi")


with DAG(
        'ETL_TEP_data',
        default_args=default_args,
        catchup=False,
        schedule_interval='*/30 * * * *' if Variable.get('DEBUG') == 'FALSE' else None,
        start_date=datetime.datetime(2022, 8, 15, 8, 00, tzinfo=local_tz),
        tags=['extract_load'],
        description='Load data into bloomlive warehouse tables',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ### DAG SUMMARY
    
    This pipeline extracts TEP(Transaction Event Publisher) data from the gateway server, 138.68.227.210, and loads it into the warehouse. 
    DAG is set to run every 30 minutes.
    
    TEP data is streamed to Asante FSG and stored in the gateway server.
    Specifically, the data is stored in database 'bloom_staging2' in the table named 'request_log'.
    
    The data is then posted to MIFOS Core Banking System by a function managed by the Digital team.
    
    This pipeline fetches data from the gateway server regardless of whether the data has been posted to the core banking system or not.
    The data is then cleaned and posted to the data warehouse, in the schema bloomlive in the tables; 
    raw_tep_disbursements, raw_tep_repayments and raw_client_activity 
    
    Currently, the data is fetched for the current day. If need be, the source code can be 
    modified to fetch data for specific days or time periods.
    
    
    #### Actions
        1. Extract opt_ins from bloom_staging2.request_log and load into ubuntu.bloomlive.raw_tep_client_activity</li>
        2. Extract opt_outs from bloom_staging2.request_log and load into ubuntu.bloomlive.raw_tep_client_activity</li>
        3. Extract disbursements from bloom_staging2.request_log and load into ubuntu.bloomlive.raw_tep_disbursements</li>
        4. Extract repayments from bloom_staging2.request_log load and into ubuntu.bloomlive.raw_tep_repayments</li>
    
    ### Configuration Parameters
    The pipeline does not take in any configuration parameters

    """


    def get_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Extracts data from a pandas dataframe and returns a list of dictionaries

        param df: pandas dataframe containing the data to be extracted
        type df: pandas.DataFrame

        return: pandas dataframe containing the extracted data
        rtype: pd.DataFrame
        """
        # Initialize an empty list to store dictionaries
        dicts = []

        # Iterate over each row in the dataframe
        for i, x in df.iterrows():
            # Load the "raw_request" column as a JSON object
            # and remove the newline characters
            try:
                raw = json.loads(x['raw_request'].replace('\n', '').replace('"{', '{').replace('}"', '}'))

                # Check if the "response_message" column exists and is not equal to "raw_log"
                response_message = x.get('response_message', None)
                if response_message and response_message != 'raw_log':
                    # Load the "response_message" column as a JSON object
                    # and remove the newline and carriage return characters
                    resp = json.loads(
                        x['response_message'].replace('\n', '').replace('\r', '').replace('"{', '{').replace('}"', '}'))[
                        'responseMessage']
                    # Add the extracted "responseMessage" to the dictionary
                    raw['response_message'] = resp

                raw['request_time'] = x.get('request_time', None)
                # Check if the "status_code" column exists and is not equal to "raw_log"
                status_code = x.get('status_code', None)
                if status_code and status_code != 'raw_log':
                    # Add the "status_code" column to the dictionary
                    raw['status_code'] = status_code

                # Append the dictionary to the list
                dicts.append(raw)
            except json.JSONDecodeError as e:
                logging.warning(f'{e}')
                # Skip this row and continue to the next one
                continue
            except TypeError as e:
                # Skip this row and continue to the next one
                logging.warning(f'{raw}')
                continue

        return pd.DataFrame(dicts)


    def store_repayments(repayments: pd.DataFrame, existing: pd.DataFrame):
        """
        Store the repayments in the database after removing the records that already exist.

        param repayments: A DataFrame containing the repayments information
        param existing: A DataFrame containing the already stored repayments information
        """
        # Remove the repayments that already exist in the database
        repayments = repayments[~repayments['fundMovementTransactionID'].isin(existing['fund_movement_transaction_id'].tolist())]

        # Insert the repayments into the database
        warehouse_hook.insert_rows(
            table='bloomlive.raw_tep_repayments',
            target_fields=[
                'request_id', 'loan_contract_id', 'due_date', 'loan_amount', 'loan_request_date', 'loan_balance',
                'store_number', 'sender_name', 'recipient_name', 'transaction_date_time', 'paid_amount',
                'notification_description', 'lender_id', 'fund_movement_transaction_id'
            ],
            rows=tuple(repayments[[
                'requestId', 'loanContractId', 'dueDate', 'loanAmount', 'loanRequestDate', 'loanBalance',
                'storeNumber', 'senderName', 'recipientName', 'transactionDateTime', 'paidAmount',
                'notificationDescription', 'lenderId', 'fundMovementTransactionID'
            ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
            commit_every=10000
        )


    def store_opt_outs(opt_outs: pd.DataFrame, existing: pd.DataFrame):
        """
        Store the opt outs in the database after removing the records that already exist.

        param opt_outs: A DataFrame containing the opt out information
        param existing: A DataFrame containing the already stored opt out information
        """
        # Remove the opt outs that already exist in the database
        opt_outs = opt_outs[~opt_outs['requestId'].isin(existing['request_id'].tolist())]

        # Mark the opt outs as opt out
        opt_outs['is_opt_in'] = False
        opt_outs['is_opt_out'] = True

        # Insert the opt outs into the database
        warehouse_hook.insert_rows(
            table='bloomlive.raw_tep_client_activity',
            target_fields=[
                'request_id', 'customer_name', 'customer_identity_id', 'store_number', 'business_name', 'mobile_number',
                'opt_out_date_time', 'is_opt_in', 'is_opt_out'
            ],
            replace=False,
            rows=tuple(opt_outs[[
                'requestId', 'customerName', 'customerIdentityId', 'storeNumber', 'businessName', 'mobileNumber',
                'optoutDateTime', 'is_opt_in', 'is_opt_out'
            ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
            commit_every=10000
        )


    def store_disbursements(disbursements: pd.DataFrame, existing: pd.DataFrame):
        """
        Store disbursements data in the database, filtering out any rows that already exist in the database.

        Parameters:
        disbursements (pd.DataFrame): The disbursements data to be stored in the database
        existing (pd.DataFrame): The existing disbursements data in the database

        Returns:
        None
        """
        # Filter out disbursements data that already exists in the database
        disbursements = disbursements[~disbursements['requestId'].isin(existing['request_id'].tolist())]

        # Insert the disbursements data into the database
        warehouse_hook.insert_rows(
            table='bloomlive.raw_tep_disbursements',
            target_fields=[
                'request_id', 'loan_number', 'loan_amount', 'store_number', 'store_name', 'product',
                'credit_limit_balance',
                'loan_disbursement_transaction_id', 'application_date_time', 'mobile_number',
                'fund_movement_transaction_id'
            ],
            replace=False,
            rows=tuple(disbursements[[
                'requestId', 'loanNumber', 'loanAmount', 'storeNumber', 'storeName', 'product',
                'creditLimitBalance',
                'loanDisbursementTransactionId', 'applicationDateTime', 'mobileNumber',
                'fundMovementTransactionID'
            ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
            commit_every=10000
        )


    def store_opt_ins(opt_ins: pd.DataFrame, existing: pd.DataFrame, menu: str):
        """
        Store opt-in data in the database, filtering out any rows that already exist in the database.

        Parameters:
        opt_ins (pd.DataFrame): The opt-in data to be stored in the database
        existing (pd.DataFrame): The existing opt-in data in the database
        menu str: The existing opt-in data in the database

        Returns:
        None
        """
        # Filter out opt-in data that already exists in the database
        opt_ins = opt_ins[~opt_ins['requestId'].isin(existing['request_id'].tolist())]

        # Add a column for national ID number if it doesn't already exist
        if 'idNumber' not in opt_ins.columns:
            opt_ins['idNumber'] = np.NAN

        # Add columns to indicate opt-in and opt-out status
        opt_ins['is_opt_in'] = True
        opt_ins['is_opt_out'] = False

        # Insert the opt-in data into the database
        warehouse_hook.insert_rows(
            table='bloomlive.raw_tep_client_activity',
            target_fields=[
                'request_id', 'customer_name', 'customer_identity_id', 'store_number', 'business_name', 'mobile_number',
                'opt_in_date_time', 'is_opt_in', 'is_opt_out', 'national_id'
            ],
            replace=False,
            rows=tuple(opt_ins[[
                'requestId', 'customerName', 'customerIdentityId', 'storeNumber', 'businessName', 'mobileNumber',
                'optinDateTime', 'is_opt_in', 'is_opt_out', 'idNumber'
            ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
            commit_every=10000
        )


    def extract_repayments(**context) -> None:
        """
        Extract repayments from the request_log table and store them in the raw_tep_repayments table

        param context: Airflow context that contains information about task instance, execution date, task instance id, etc.
        return: None
        """
        # Looping over the 'raw_loan_repayment' menu
        for menu in ['loan_repayment', 'raw_loan_repayment']:
            # Fetching the total number of repayments for the given menu and current date
            total_repayments = int(gateway_hook.get_pandas_df(
                sql=f"""
                    select count(*) as ttl from bloom_staging2.request_log
                    where menu = '{menu}' and DATE(request_time) >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                """,
            )['ttl'].iloc[0])

            chunk_size = 100000
            # Loop over the number of repayments to extract the data in chunks of 100000
            for num in range(0, total_repayments, chunk_size):
                # Columns to fetch from the request_log table
                columns = ['raw_request', 'status_code', 'response_message', 'request_time']
                # If the menu is 'raw_loan_request', remove the 'response_message' column
                if menu == 'raw_loan_request':
                    columns.remove('response_message')

                # Fetching repayments from the request_log table
                repayments = gateway_hook.get_pandas_df(
                    sql=f""" 
                        select {", ".join(columns)} from bloom_staging2.request_log
                        where menu = "{menu}" and DATE(request_time) >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                        order by id desc, status_code asc
                        LIMIT {num}, {chunk_size}
                    """,
                )

                # If the repayments dataframe is not empty, process the data
                if not repayments.empty:
                    # Call the get_data function to process the repayments dataframe
                    repayments = get_data(df=repayments)

                    # Drop the NaN values in the 'transactionDateTime' column
                    repayments.dropna(subset=['transactionDateTime'], inplace=True)

                    # Convert the 'transactionDateTime' column to datetime format
                    repayments['transactionDateTime'] = repayments['transactionDateTime'].apply(
                        lambda x: datetime.datetime.strptime(str(x), '%Y%m%d%H%M%S')
                    )
                    # Convert the 'loanRequestDate' column to datetime format
                    repayments['loanRequestDate'] = repayments['loanRequestDate'].apply(
                        lambda x: datetime.datetime.strptime(str(x), '%Y%m%d%H%M%S')
                    )
                    # Convert the due date column in the repayments dataframe to datetime format
                    repayments['dueDate'] = repayments['dueDate'].apply(
                        lambda x: datetime.datetime.strptime(str(x), '%Y%m%d'))

                    # Get the data from the raw_tep_repayments table in the bloomlive database
                    existing = warehouse_hook.get_pandas_df(
                        sql="""
                            select request_id, fund_movement_transaction_id, status_code as sc, failure_reason from bloomlive.raw_tep_repayments
                            where fund_movement_transaction_id in %(fmti)s 
                        """,
                        parameters={'fmti': tuple(repayments['fundMovementTransactionID'].astype(str).tolist())}
                    )

                    if menu == 'loan_repayment':
                        # Remove duplicate rows from repayments based on the "requestId" column
                        repayments = repayments.sort_values(
                            by=['fundMovementTransactionID', 'status_code', 'request_time'],
                            ascending=[True, True, False])
                        repayments['rank'] = repayments.groupby('fundMovementTransactionID')['status_code'].rank(
                            method='dense', ascending=True)
                        repayments = repayments[repayments['rank'] == 1]

                        # Replace the values in the "response_message" column that contain "Duplicate Repayment" with "Duplicate Repayment"
                        repayments.loc[repayments['response_message'].str.contains(
                            'Duplicate Repayment'), 'response_message'] = 'Duplicate Repayment'

                        # Merge the repayments and existing dataframes on the "requestId" and "request_id" columns, respectively
                        existing2 = repayments.merge(existing, left_on='fundMovementTransactionID', right_on='fund_movement_transaction_id',
                                                     how='right').drop_duplicates(subset=['fund_movement_transaction_id'])
                        # updates = existing2[(existing2['status_code'].astype(int) != existing2['sc'].astype(int)) | (existing2['failure_reason'].isna())]

                        existing2['status_code'] = existing2['status_code'].fillna(0).astype(int)
                        existing2['sc'] = existing2['sc'].fillna(0).astype(int)
                        updates = existing2[
                            (existing2['status_code'] != existing2['sc']) | (existing2['failure_reason'].isna())]

                        status_code_updates = updates.groupby('status_code')
                        failure_reason_updates = updates.groupby('response_message')

                        # Loop through the groups of updated status codes and update the raw_tep_repayments table
                        for i in status_code_updates.groups.keys():
                            grp = status_code_updates.get_group(i)
                            warehouse_hook.run(
                                sql="""update bloomlive.raw_tep_repayments set status_code = %(sc)s where fund_movement_transaction_id in %(ris)s""",
                                parameters={'sc': str(i), 'ris': tuple(grp['fund_movement_transaction_id'].dropna().tolist())}
                            )

                        # Loop through the groups of updated failure reasons and update the raw_tep_repayments table
                        for i in failure_reason_updates.groups.keys():
                            grp = failure_reason_updates.get_group(i)
                            warehouse_hook.run(
                                sql="""update bloomlive.raw_tep_repayments set failure_reason = %(fr)s where fund_movement_transaction_id in %(ris)s""",
                                parameters={'fr': str(i), 'ris': tuple(grp['fund_movement_transaction_id'].dropna().tolist())}
                            )

                    repayments.drop_duplicates(subset=['fundMovementTransactionID'], inplace=True)

                    # Store the repayments data in the data warehouse
                    store_repayments(repayments, existing)


    def extract_disbursements(**context) -> None:
        """
        This function extracts disbursements data from the bloom_staging2.request_log table and updates the
        bloomlive.raw_tep_disbursements table. The function loops through two different menus, loan_request and
        raw_loan_request, and retrieves data in chunks of 100000 records each time.

        param context: Additional context parameters that can be passed to the function
        return: None
        """
        # loop through two different menus, loan_request and raw_loan_request
        for menu in ['loan_request', 'raw_loan_request']:
            # retrieve the total number of disbursements for the current day for the current menu
            total_disbursements = int(gateway_hook.get_pandas_df(
                sql=f"""
                    select count(*) as ttl from bloom_staging2.request_log
                    where menu = %(menu)s and DATE(request_time) >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                """,
                parameters={'menu': menu}
            )['ttl'].iloc[0])

            # define the chunk size as 100000
            chunk_size = 100000

            # loop through the total disbursements in chunks of 100000 each time
            for num in range(0, total_disbursements, chunk_size):
                # define the columns to retrieve based on the current menu
                columns = ['id', 'raw_request', 'status_code', 'response_message']
                if menu == 'raw_loan_request':
                    columns.remove('response_message')

                # retrieve disbursements data from the bloom_staging2.request_log table
                disbursements = gateway_hook.get_pandas_df(
                    sql=f""" 
                        select {", ".join(columns)} from bloom_staging2.request_log
                        where menu = "{menu}" and DATE(request_time) >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                        order by id desc, status_code asc
                        LIMIT {num}, {chunk_size}
                    """,
                )

                # if the disbursements data is not empty, process the data
                if not disbursements.empty:
                    # get additional data using the get_data function
                    disbursements = get_data(df=disbursements)
                    # drop any duplicates in the requestId column
                    disbursements.drop_duplicates(subset=['requestId'], inplace=True)
                    # drop any rows with missing applicationDateTime values
                    disbursements.dropna(subset=['applicationDateTime'], inplace=True)
                    # convert the applicationDateTime column to a datetime type
                    disbursements['applicationDateTime'] = disbursements['applicationDateTime'].apply(
                        lambda x: datetime.datetime.strptime(str(x), '%Y%m%d%H%M%S')
                    )
                    # drop any duplicates in the requestId column
                    disbursements.drop_duplicates(subset=['requestId'], inplace=True)

                    # Get the existing disbursement data from the warehouse
                    existing = warehouse_hook.get_pandas_df(
                        sql="""
                            select request_id, status_code as sc, failure_reason from bloomlive.raw_tep_disbursements
                            where request_id in %(ris)s 
                        """,
                        parameters={'ris': tuple(disbursements['requestId'].astype(str).tolist())}
                    )

                    # Perform update operations only if the menu is loan_request
                    if menu == 'loan_request':
                        # Merge existing and disbursement data
                        existing2 = disbursements.merge(
                            existing,
                            left_on='requestId',
                            right_on='request_id',
                            how='right'
                        ).drop_duplicates(subset=['request_id'])

                        # Identify updates based on status_code and failure_reason
                        updates = existing2[
                            (existing2['status_code'] != existing2['sc']) | (existing2['failure_reason'].isna())]

                        # Group updates by status_code and failure_reason
                        status_code_updates = updates.groupby('status_code')
                        failure_reason_updates = updates.groupby('response_message')

                        # Update the status_code in the warehouse
                        for i in status_code_updates.groups.keys():
                            grp = status_code_updates.get_group(i)
                            warehouse_hook.run(
                                sql="""update bloomlive.raw_tep_disbursements set status_code = %(sc)s where request_id in %(ris)s""",
                                parameters={'sc': str(i), 'ris': tuple(grp['request_id'].dropna().tolist())}
                            )

                        # Update the failure_reason in the warehouse
                        for i in failure_reason_updates.groups.keys():
                            grp = failure_reason_updates.get_group(i)
                            warehouse_hook.run(
                                sql="""update bloomlive.raw_tep_disbursements set failure_reason = %(fr)s where request_id in %(ris)s""",
                                parameters={'fr': str(i), 'ris': tuple(grp['request_id'].dropna().tolist())}
                            )

                    # Store the disbursements
                    store_disbursements(disbursements, existing)


    def extract_opt_ins(**context) -> None:
        """
        Extract opt-in requests from the request_log table, clean up and process the data, and store the
        processed data in the raw_tep_client_activity table.
        """
        for menu in ['opt_in', 'raw_opt_in']:
            # Get the total number of opt-in requests in the request_log table for the current date.
            total_opt_ins = int(
                gateway_hook.get_pandas_df(
                    sql=f"""
                        select count(*) as ttl from bloom_staging2.request_log
                        where menu = %(menu)s and DATE(request_time) >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                    """,
                    parameters={'menu': menu}
                )['ttl'].iloc[0]
            )

            chunk_size = 100000
            for num in range(0, total_opt_ins, chunk_size):
                # Define the columns to retrieve from the request_log table.
                columns = ['id', 'raw_request', 'status_code', 'response_message']

                # Remove the 'response_message' column for the raw_opt_in menu.
                if menu == 'raw_opt_in':
                    columns.remove('response_message')

                # Get a chunk of opt-in requests from the request_log table.
                opt_ins = gateway_hook.get_pandas_df(
                    sql=f""" 
                        select {", ".join(columns)} from bloom_staging2.request_log
                        where menu = "{menu}" and DATE(request_time) >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                        order by id desc
                        LIMIT {num}, {chunk_size}
                    """
                )

                # If there are opt-in requests, process and clean the data.
                if not opt_ins.empty:
                    opt_ins = get_data(df=opt_ins)
                    opt_ins.drop_duplicates(subset=['requestId'], inplace=True)
                    opt_ins['optinDateTime'] = opt_ins['optinDateTime'].apply(
                        lambda x: datetime.datetime.strptime(str(x), '%Y%m%d%H%M%S')
                    )
                    opt_ins.drop_duplicates(subset=['requestId'], inplace=True)

                    # Get existing opt-in requests from the raw_tep_client_activity table.
                    existing = warehouse_hook.get_pandas_df(
                        sql="""
                            select request_id, status_code as sc from bloomlive.raw_tep_client_activity 
                            where is_opt_in and request_id in %(rids)s
                        """,
                        parameters={'rids': tuple(opt_ins['requestId'].tolist())}
                    )

                    # Check if the menu selected is 'opt_in'
                    if menu == 'opt_in':
                        # Merge existing and opt_ins dataframes on requestId and request_id
                        existing2 = opt_ins.merge(existing, left_on='requestId', right_on='request_id',
                                                  how='right').drop_duplicates(subset=['requestId'])

                        # Get the rows where the status_code in existing2 is not equal to sc
                        updates = existing2[(existing2['status_code'] != existing2['sc'])]
                        # Group the updates dataframe by status_code
                        status_code_updates = updates.groupby('status_code')
                        # Group the updates dataframe by response_message
                        failure_reason_updates = updates.groupby('response_message')

                        # Loop through the groups in status_code_updates
                        for i in status_code_updates.groups.keys():
                            # Get the current group
                            grp = status_code_updates.get_group(i)

                            # Update the `status_code` column in the `bloomlive.raw_tep_client_activity` table
                            warehouse_hook.run(
                                sql="""update bloomlive.raw_tep_client_activity set status_code = %(sc)s where request_id in %(ris)s and is_opt_in""",
                                parameters={'sc': str(i), 'ris': tuple(grp['request_id'].dropna().tolist())}
                            )

                        # Loop through the groups in failure_reason_updates
                        for i in failure_reason_updates.groups.keys():
                            # Get the current group
                            grp = failure_reason_updates.get_group(i)

                            # Update the `failure_reason` column in the `bloomlive.raw_tep_client_activity` table
                            warehouse_hook.run(
                                sql="""update bloomlive.raw_tep_client_activity set failure_reason = %(fr)s where request_id in %(ris)s and is_opt_in""",
                                parameters={'fr': str(i), 'ris': tuple(grp['request_id'].dropna().tolist())}
                            )

                    # Store the opt_ins and existing data
                    store_opt_ins(opt_ins, existing, menu)


    def extract_opt_outs(**context) -> None:
        """
        This function extracts the opt-out data from the request_log table in the bloom_staging2 database, processes it and stores it in the raw_tep_client_activity table in the bloomlive database.

        Parameters:
        context (dict): The context contains information about the task instance.

        Returns:
        None
        """
        menus = ['raw_opt_out', 'opt_out']

        for menu in menus:
            # Get the total number of rows in the request_log table with the menu type and current date
            total_opt_outs = int(gateway_hook.get_pandas_df(
                sql=f"""
                    select count(*) as ttl from bloom_staging2.request_log
                    where menu = %(menu)s and DATE(request_time) >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                """,
                parameters={'menu': menu}
            )['ttl'].iloc[0])

            chunk_size = 100000
            # Fetch the data in chunks of 100000 rows at a time
            for num in range(0, total_opt_outs, chunk_size):
                # Get opt-out data from the request_log table
                opt_outs = gateway_hook.get_pandas_df(
                    sql=f"""
                        select id, raw_request, status_code from bloom_staging2.request_log
                        where menu = "{menu}" and DATE(request_time) >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                        order by id desc
                        LIMIT {num}, {chunk_size}
                    """
                )

                if not opt_outs.empty:
                    # Process the opt-out data
                    opt_outs = get_data(df=opt_outs)
                    opt_outs.drop_duplicates(subset=['requestId'], inplace=True)
                    opt_outs['optoutDateTime'] = opt_outs['optoutDateTime'].apply(
                        lambda x: datetime.datetime.strptime(str(x), '%Y%m%d%H%M%S')
                    )
                    opt_outs.drop_duplicates(subset=['requestId'], inplace=True)

                    # Get existing data from the raw_tep_client_activity table
                    existing = warehouse_hook.get_pandas_df(
                        sql="""
                            select request_id, status_code as sc from bloomlive.raw_tep_client_activity where is_opt_out
                            and request_id in %(rids)s
                        """,
                        parameters={'rids': tuple(opt_outs['requestId'].tolist())}
                    )

                    if menu == 'opt_out':
                        # Merge the existing data and the processed opt-out data
                        existing2 = opt_outs.merge(
                            existing,
                            left_on='requestId',
                            right_on='request_id',
                            how='right'
                        ).drop_duplicates(subset=['requestId'])

                        # Find status codes that have different values in 'existing2' dataframe
                        # and group them by 'status_code'
                        updates = existing2[
                            (existing2['status_code'] != existing2['sc']) & (existing2['sc'] != '')].groupby(
                            'status_code')

                        # For each group of different status codes
                        for i in updates.groups.keys():
                            # Get the group by its status code
                            grp = updates.get_group(i)
                            # Update the status code in table 'bloomlive.raw_tep_client_activity'
                            # for requests that are opt-outs and are in the current group
                            warehouse_hook.run(
                                sql="""
                                    update bloomlive.raw_tep_client_activity 
                                    set status_code = %(sc)s 
                                    where request_id in %(ris)s and is_opt_out
                                """,
                                parameters={'sc': str(i), 'ris': tuple(grp['request_id'].dropna().tolist())}
                            )

                        # Store the processed opt-outs data in the warehouse
                        store_opt_outs(opt_outs, existing)


    def trigger_tep_data_recon(**context):
        """
        Trigger the 'tep_data_recon' DAG by calling the `trigger_dag_remotely` function.

        param context: The context for the task instance, containing information about the task and its execution.
        """
        # Call the trigger_dag_remotely function to trigger the 'tep_data_recon' DAG
        trigger_dag_remotely(dag_id='tep_data_bloom_recon', conf={})
        notification = MSTeamsWebhookOperator(
            task_id='send_ms_teams_notification',  # unique identifier for this task
            http_conn_id='msteams_webhook_url',  # connection id of the MS Teams webhook
            message="TEP data",  # main message to be sent
            subtitle="New data added to the warehouse, national ids updated and mifos RECON posting triggered",
            # subtitle for the message
            dag=dag,  # the dag to which this operator belongs
            retries=0  # number of retries in case the task fails
        )
        notification.execute(context)


    def update_warehouse_national_ids(**context):
        """
        This function updates the national IDs in the warehouse using the data from raw TEP.
        """
        # Get the clients with long-format national IDs in the warehouse
        with_long_format_national_id_in_warehouse = warehouse_hook.get_pandas_df(
            sql="""
                SELECT
                    csv1.national_id, csv1.surrogate_id, csv1.store_number,
                    rtca.national_id AS tep_national_id, rtca.customer_identity_id,
                    rtca.opt_in_date_time
                FROM
                    bloomlive.client_summary_view csv1
                    LEFT JOIN bloomlive.raw_TEP_client_activity rtca
                    ON csv1.national_id = rtca.customer_identity_id
                WHERE
                    LENGTH(csv1.national_id) > 15
                    AND csv1.national_id ~ '^[0-9\.]+$'
                    AND rtca.is_opt_in IS TRUE
                    AND csv1.bloom_version = '2'
                    AND csv1.national_id IS NOT NULL
                    AND rtca.national_id IS NOT NULL
                    AND NOT rtca.national_id = '000000000'
                    AND rtca.is_opt_in
            """
        )

        # Get the clients without national IDs in the warehouse
        without_national_id_in_warehouse = warehouse_hook.get_pandas_df(
            sql="""
                SELECT
                    csv1.national_id,
                    csv1.surrogate_id,
                    csv1.store_number,
                    rtca.national_id AS tep_national_id,
                    rtca.customer_identity_id,
                    rtca.opt_in_date_time
                FROM
                    bloomlive.client_summary_view csv1
                    LEFT JOIN bloomlive.raw_TEP_client_activity rtca
                    ON csv1.store_number = rtca.store_number
                WHERE
                    rtca.is_opt_in IS TRUE
                    AND csv1.bloom_version = '2'
                    AND (csv1.national_id IS NULL or csv1.national_id = '000000000')
                    AND rtca.national_id IS NOT NULL
                    AND NOT rtca.national_id = '000000000'
                    AND rtca.is_opt_in
            """
        )

        # Prepare the updates for the national IDs in the warehouse
        updates = []
        for index, row in without_national_id_in_warehouse.replace({np.NAN: "null"}).iterrows():
            updates.append(
                f"UPDATE bloomlive.client_dimension SET national_id = '{row['tep_national_id']}' WHERE surrogate_id = {row['surrogate_id']} returning current_timestamp::varchar, '{row['tep_national_id']}', null, surrogate_id"
            )

        for index, row in with_long_format_national_id_in_warehouse.replace({np.NAN: "null"}).iterrows():
            updates.append(
                f"UPDATE bloomlive.client_dimension set national_id = '{row['tep_national_id']}' where surrogate_id = {row['surrogate_id']} returning current_timestamp::varchar, '{row['tep_national_id']}', '{row['national_id']}', surrogate_id"
            )

        # If there are any updates to make, execute them.
        if len(updates) > 0:
            updated = warehouse_hook.run(sql=updates, handler=lambda x: x.fetchall())
            store_national_id_updates(
                updated=updated,
                source='TEP',
                airflow_hook=MySqlHook(mysql_conn_id='mysql_airflow', schema='bloom_pipeline')
            )

    def update_warehouse_company_names(**context):
        clients = warehouse_hook.get_pandas_df(
            sql="""
                with rnked as (
                    select store_number as tep_store_number, business_name, rank() over (
                        partition by store_number order by opt_in_date_time desc
                    ) rnk
                    from bloomlive.raw_tep_client_activity rtca where is_opt_in
                ) select cd.surrogate_id, tep_store_number, business_name, cd.store_number, cd.company_name from rnked 
                right join bloomlive.client_dimension cd on rnked.tep_store_number = cd.store_number
                where rnked.rnk = 1 and cd.company_name is null and rnked.business_name is not null
            """
        )
        updates = []
        for index, client in clients.replace({np.NAN: None}).iterrows():
            updates.append(
                f"UPDATE bloomlive.client_dimension SET company_name = '{client['business_name']}' WHERE surrogate_id = {client['surrogate_id']} returning current_timestamp::varchar, '{client['business_name']}', null, surrogate_id"
            )

        if len(updates) > 0:
            updated = warehouse_hook.run(sql=updates, handler=lambda x: x.fetchall())
            store_company_name_updates(
                updated=updated,
                source='TEP',
                airflow_hook=MySqlHook(mysql_conn_id='mysql_airflow', schema='bloom_pipeline')
            )


    def send_mifos_TEP_data_summary_ms_teams_notification(**context):
        report = warehouse_hook.get_pandas_df(
            sql="""
            SELECT
                successful_transactions.transaction_date,
                total_successful_repayments,
                total_successful_disbursements,
                failure_type,
                failure_reason,
                total_failed_transactions
            from
            (
                SELECT
                    DATE(initiation_time) AS transaction_date,
                    COUNT(CASE WHEN trxns.is_repayment = 1 THEN 1 END) AS total_repayments,
                    COUNT(CASE WHEN trxns.is_disbursement = 1 THEN 1 END) AS total_disbursements
                from (
                    select application_date_time as initiation_time, 1 as is_disbursement, 0 as is_repayment, status_code 
                    from bloomlive.raw_tep_disbursements rtd union (
                        select transaction_date_time as initiation_time, 1 as is_repayment, 0 as is_disbursement, status_code 
                        from bloomlive.raw_tep_repayments rtr
                    )
                ) trxns
                group by DATE(initiation_time)
            ) as all_transactions left join (
                SELECT
                    DATE(initiation_time) AS transaction_date,
                    COUNT(CASE WHEN trxns.is_repayment = 1 THEN 1 END) AS total_successful_repayments,
                    COUNT(CASE WHEN trxns.is_disbursement = 1 THEN 1 END) AS total_successful_disbursements
                from (
                    select application_date_time as initiation_time, 1 as is_disbursement, 0 as is_repayment, status_code from bloomlive.raw_tep_disbursements rtd where status_code = 200
                    union
                    select transaction_date_time as initiation_time, 0 as is_disbursement, 1 as is_repayment, status_code from bloomlive.raw_tep_repayments rtr where status_code = 200
                ) trxns
                GROUP BY
                    DATE(initiation_time)
            ) AS successful_transactions on successful_transactions.transaction_date = all_transactions.transaction_date left join (
                SELECT
                    DATE(initiation_time) AS failed_transaction_date,
                    CASE WHEN trxns.is_repayment = 1 THEN 'Repayment' ELSE 'Disbursement' END AS failure_type,
                    failure_reason,
                    COUNT(failure_reason) AS total_failed_transactions
                from (
                    select application_date_time as initiation_time, 1 as is_disbursement, 0 as is_repayment, status_code, failure_reason from bloomlive.raw_tep_disbursements rtd where status_code = 400
                    union
                    select transaction_date_time as initiation_time, 0 as is_disbursement, 1 as is_repayment, status_code, failure_reason from bloomlive.raw_tep_repayments rtr where status_code = 400
                ) trxns
                GROUP BY
                    DATE(initiation_time), failure_type, failure_reason      
            ) AS failed_transactions
            ON
                all_transactions.transaction_date = failed_transactions.failed_transaction_date
            where all_transactions.transaction_date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY
                transaction_date desc, failure_type, failure_reason
            """
        )

        report_str = ""
        cnt = 0
        for date in report['transaction_date'].unique():
            cnt += 1
            dt = report[report['transaction_date'] == date]
            disb_fr = [f"{r['failure_reason']}: {r['total_failed_transactions']}" for i, r in
                       dt[dt['failure_type'] == 'Disbursement'].iterrows()]
            rep_fr = [f"{r['failure_reason']}: {r['total_failed_transactions']}".strip() for i, r in
                      dt[dt['failure_type'] == 'Repayment'].iterrows()]

            rep_fail_reasons = ', '.join(rep_fr) if len(rep_fr) > 0 else 'None'
            disb_fail_reasons = ', '.join(disb_fr) if len(disb_fr) > 0 else 'None'

            report_str += f"""
                        TEP Posting to MIFOS {date}
                        Successful Disbursements: {dt.iloc[0]['total_successful_disbursements']},
                        Successful Repayments: {dt.iloc[0]['total_successful_repayments']},
                        Failed Disbursements: {dt[dt['failure_type'] == 'Disbursement']['total_failed_transactions'].sum()},
                        Failed Repayments: {dt[dt['failure_type'] == 'Repayment']['total_failed_transactions'].sum()}

                        Disbursement Failure reasons
                        {disb_fail_reasons}


                        Repayment Failure reasons
                        {rep_fail_reasons}

                        {'-' * 50}

                    """

        dag_id = context['dag_run'].dag_id
        task_id = context['task_instance'].task_id
        context['task_instance'].xcom_push(key=dag_id, value=True)

        logs_url = "https://airflow.asantefsg.com/data-pipelines/airflow/log?dag_id={}&task_id={}&execution_date={}".format(
            dag_id, task_id, parse.quote_plus(context['ts']))

        teams_notification = MSTeamsWebhookOperator(
            task_id="send_ms_summary_to_ms_teams",
            message=f"Bloom TEP Posting TO MIFOS: {context['dag_run'].conf.get('recon_type', '')}",
            subtitle=report_str,
            button_text="View log",
            button_url=logs_url,
            http_conn_id='msteams_webhook_url_safaricom_data_dumps_channel' if Variable.get(
                'DEBUG') == 'FALSE' else 'msteams_webhook_url',
        )
        teams_notification.execute(context)

    def trigger_Bloom_IPRS_check(**context):
        # Get the number of queued DAG runs for the ETL bloom2 loans fact table DAG
        trigger_dag_remotely(dag_id='Bloom_IPRS_check', conf={})

    # TASKS
    # Extraction task for repayments data
    t1 = PythonOperator(
        task_id='extract_repayments',
        provide_context=True,
        python_callable=extract_repayments,
    )

    # Extraction task for disbursements data
    t2 = PythonOperator(
        task_id='extract_disbursements',
        provide_context=True,
        python_callable=extract_disbursements,
    )

    # Extraction task for opt-in data
    t3 = PythonOperator(
        task_id='extract_opt_ins',
        provide_context=True,
        python_callable=extract_opt_ins,
    )

    # Extraction task for opt-out data
    t4 = PythonOperator(
        task_id='extract_opt_outs',
        provide_context=True,
        python_callable=extract_opt_outs,
    )

    # Trigger TEP reconciliation task
    t9 = PythonOperator(
        task_id='trigger_tep_data_recon',
        provide_context=True,
        python_callable=trigger_tep_data_recon,
    )

    t11 = PythonOperator(
        task_id='update_warehouse_national_ids',
        provide_context=True,
        python_callable=update_warehouse_national_ids,
    )

    t12 = PythonOperator(
        task_id='update_warehouse_company_names',
        provide_context=True,
        python_callable=update_warehouse_company_names,
    )
    # t14 = PythonOperator(
    #     task_id='trigger_Bloom_IPRS_check',
    #     provide_context=True,
    #     python_callable=trigger_Bloom_IPRS_check
    # )

    # t1 >> t2 >> t3 >> t4 >> t9 >> t11 >> t12 >> t14
    t1 >> t2 >> t3 >> t4 >> t9 >> t11 >> t12
