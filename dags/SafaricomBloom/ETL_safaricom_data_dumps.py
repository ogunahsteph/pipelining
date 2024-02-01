import os
import sys
import base64
import logging
import datetime
import pendulum
import numpy as np
import pandas as pd
from airflow import DAG
from datetime import timedelta
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator
from utils.common import on_failure, cntrct_id_check_saf_disbursements_dump, store_national_id_updates, store_company_name_updates, trigger_dag_remotely
from dags.SafaricomBloom.scripts.saf_data_dumps_to_files import upload_contracts_ms_teams, upload_closed_contracts_ms_teams, upload_statements_ms_teams, upload_disbursements_ms_teams, upload_repayments_ms_teams, upload_opt_outs_ms_teams, upload_outstanding_ms_teams, upload_opt_ins_ms_teams

saf_server_ssh_hook = SSHHook(ssh_conn_id="ssh_saf_server")
gateway_server_ssh_hook = SSHHook(ssh_conn_id="ssh_gateway_server")
gateway_hook = MySqlHook(mysql_conn_id='gateway_server', schema='bloom_staging')
mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-safaricom')
warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
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
    'on_failure_callback': on_failure if Variable.get('DEBUG') == 'FALSE' else None,
    'remote_files_directory_path': f'/root/data/safaricom_bloom/daily_data_dumps',
    'local_files_directory_path': f"/{os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))}/data/safaricom_bloom/daily_data_dumps"
}

local_tz = pendulum.timezone("Africa/Nairobi")

class CustomSFTPOperator(SFTPOperator):
    """
    A custom operator to copy safaricom data dumps using the SFTP operator,
    Customized to fetch xcom from other task
    """
    def __init__(self, *, local_filepath, remote_filepath, template_dict, **kwargs):
        self.template_dict = template_dict
        super().__init__(local_filepath=local_filepath, remote_filepath=remote_filepath, **kwargs)

    def execute(self, context):
        encoded_string = context['ti'].xcom_pull(task_ids='copy_files_from_safaricom_server_to_gateway_server', key='return_value')
        source_str = base64.b64decode(encoded_string).decode().strip().split(' /')

        local_files_directory_path = self.template_dict['local_files_directory_path']

        self.local_filepath = [f"{local_files_directory_path}/{x.split('/')[-1]}" for x in source_str]
        self.remote_filepath = [f'/{x}' if x[0] != '/' else x for x in source_str]

        logging.warning(f'{self.local_filepath}')

        super().execute(context)


with DAG(
        'ETL_safaricom_data_dumps',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 11 * * *' if Variable.get('DEBUG') == 'FALSE' else None,
        start_date=datetime.datetime(2021, 12, 8, 14, 30, tzinfo=local_tz),
        tags=['extract_load'],
        description='Load data into bloomlive.data_dumps',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ### DAG SUMMARY
    Extracts data from safaricom data dump files posted on the shared server and loads the data into the 
    warehouse
    DAG is set to run daily at 11:00 am.
    
    Pass "files_date" parameter to fetch files for a specific date. For example, to fetch files for date 2023-01-05 (YY-mm-dd)
    pass below configuration parameters.
    
    The parameters have to be in valid JSON. Use double quotes for keys and values
    ```
     {
        "files_date": "20230105"
     }
    ```
    
    If the no "files_date" parameter is passed, the pipeline defaults to fetching files dated today minus one day (T - 1) date.
    
    ### Details
    Safaricom loads daily dumps into sftp server 41.90.220.13. 
    This server is only reachable via a VPN tunnel connected to gateway server 138.68.227.210 (owned by Asante FSG).
    
    To retrieve these files, the files have to be copied from the safaricom server, into the gateway server
    and finally into the airlfow server in order to be processed.

    """
    def missing_fields_check(df: pd.DataFrame, fields: list) -> pd.DataFrame:
        """
        df : the pandas DataFrame for which to check for missing values
        fields: A list of columns that should not be null
        checks for rows with missing required columns
        return : a dataframe of rows containing null values
        """
        return df[~df.index.isin(df.dropna(subset=fields, how='any').index)]

    def replace_special_characters(string: str) -> str:
        """
        removes special characters from a string
        param string: the text to be cleaned up
        """
        special_characters = ['½', '¿', '~', '�', 'ï', 'Â', 'Ã¯']

        for char in special_characters:
            string = string.replace(char, '')
        return string


    def trigger_data_dumps_bloom_data_recon(**context):
        """
        Triggers sharing of data dumps to the reconciliations database bloom_staging.

        Parameters:
        -----------
        **context: dict
            The context dictionary that contains information about the current execution.

        Returns:
        --------
        None
        """

        trigger_dag_remotely(dag_id='data_dumps_bloom_data_recon', conf={})

    def load_contract_details(**context):
        """
        Loads contract details data from local files, checks for missing data, and inserts data into a database table.
        """

        # Get tasks to run from dag run configuration
        tasks_to_run = context['dag_run'].conf.get('tasks_to_run', None)

        def execute_task():
            # Get date from dag run configuration, or use yesterday's date if not provided
            file_date = context['dag_run'].conf.get('files_date', str((datetime.datetime.today() - datetime.timedelta(
                days=1)).date()).replace('-', ''))

            # Get path to local files directory
            path = f"{context['templates_dict']['local_files_directory_path']}/"

            # Initialize list for contract details data
            contract_details = []

            # Iterate through files in local files directory and append daily_contracts files with the specified date to contract_details
            for filename in os.listdir(path):
                if 'daily_contracts' in filename and file_date in filename:
                    try:
                        contract_details.append(pd.read_csv("{}{}".format(path, filename)))
                    except pd.errors.EmptyDataError:
                        # If file is empty, push XCom variables to indicate missing data
                        context['ti'].xcom_push(key='is_missing', value=False)
                        context['ti'].xcom_push(key='is_empty', value=True)

            # If contract_details list is not empty
            if len(contract_details) > 0:
                # Push XCom variable to indicate no missing data
                context['ti'].xcom_push(key='is_empty', value=False)

                # Concatenate contract_details dataframes and perform necessary data cleaning
                contract_details = pd.concat(contract_details, ignore_index=True)
                contract_details.columns = [x.lower() for x in contract_details.columns]
                contract_details['id_date'] = contract_details['id_date'].apply(
                    lambda x: str(x).replace('.0', '')
                )
                required_fields = [
                    'id_date', 'cntrct_id', 'src_acces_fee', 'src_id_idnty', 'src_prncpl_amnt'
                ]
                contract_details['has_missing_fields'] = False
                contract_details.loc[missing_fields_check(df=contract_details,
                                                          fields=required_fields).index, 'has_missing_fields'] = True

                # Insert cleaned contract_details data into database table
                warehouse_hook.insert_rows(
                    table='bloomlive.contract_details_data_dump',
                    target_fields=[
                        'id_date', 'cntrct_id', 'src_id_prdct', 'src_prdct_name', 'src_tm_cntrct_create',
                        'src_dt_cntrct_due', 'src_id_idnty', 'src_micro_loan_cntrct_stts', 'src_totl_amnt',
                        'src_prncpl_amnt', 'src_acces_fee', 'src_intrst_fee', 'src_maintnanc_fee', 'src_rollovr_fee',
                        'src_penlty_fee', 'has_missing_fields'
                    ],
                    rows=contract_details[[
                        'id_date', 'cntrct_id', 'src_id_prdct', 'src_prdct_name', 'src_tm_cntrct_create',
                        'src_dt_cntrct_due', 'src_id_idnty', 'src_micro_loan_cntrct_stts', 'src_totl_amnt',
                        'src_prncpl_amnt', 'src_acces_fee', 'src_intrst_fee', 'src_maintnanc_fee', 'src_rollovr_fee',
                        'src_penlty_fee', 'has_missing_fields'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None),
                    commit_every=0
                )

                # Upload Contracts to MS Teams
                upload_contracts_ms_teams(str(datetime.datetime.strptime(file_date, '%Y%m%d').date()))

                context['ti'].xcom_push(key='is_missing', value=False)
            else:
                context['ti'].xcom_push(key='is_missing', value=True)

        if tasks_to_run is not None:
            if context['task'].task_id in tasks_to_run:  # only specified tasks should run
                execute_task()
            else:
                context['ti'].xcom_push(key='is_missing', value=False)
        else:  # all tasks should run
            execute_task()

    def load_daily_closed_contracts(**context):
        """
        Loads daily closed contracts data from local files, checks for missing data, and inserts data into a database table.
        """
        # Get tasks to run from dag run configuration
        tasks_to_run = context['dag_run'].conf.get('tasks_to_run', None)

        def execute_task():
            # Get date from dag run configuration, or use yesterday's date if not provided
            file_date = context['dag_run'].conf.get('files_date',
                                                    str((datetime.datetime.today() - datetime.timedelta(
                                                        days=1)).date()).replace('-', ''))

            # Get path to local files directory
            path = f"{context['templates_dict']['local_files_directory_path']}/"

            # Initialize list for daily closed contracts data
            daily_closed_contracts = []

            # Iterate through files in local files directory and append daily_contracts files with the specified date to contract_details
            for filename in os.listdir(path):
                if 'daily_closed_contracts' in filename and file_date in filename:
                    try:
                        daily_closed_contracts.append(pd.read_csv("{}{}".format(path, filename)))
                    except pd.errors.EmptyDataError:
                        # If file is empty, push XCom variables to indicate missing data
                        context['ti'].xcom_push(key='is_missing', value=False)
                        context['ti'].xcom_push(key='is_empty', value=True)

            # If contract_details list is not empty
            if len(daily_closed_contracts) > 0:
                # Push XCom variable to indicate no missing data
                context['ti'].xcom_push(key='is_empty', value=False)

                # Concatenate contract_details dataframes and perform necessary data cleaning
                daily_closed_contracts = pd.concat(daily_closed_contracts, ignore_index=True)
                daily_closed_contracts.columns = [x.lower() for x in daily_closed_contracts.columns]
                daily_closed_contracts['id_date'] = daily_closed_contracts['id_date'].apply(
                    lambda x: str(x).replace('.0', '')
                )
                required_fields = ['id_date', 'cntrct_id']

                daily_closed_contracts['has_missing_fields'] = False
                daily_closed_contracts.loc[missing_fields_check(df=daily_closed_contracts,
                                                          fields=required_fields).index, 'has_missing_fields'] = True

                # Insert cleaned contract_details data into database table
                warehouse_hook.insert_rows(
                    table='bloomlive.daily_closed_contracts_dump',
                    target_fields=[
                        'id_date', 'cntrct_id', 'prdct', 'dt_creatd', 'dt_complte',
                        'id_idnty', 'cntrct_stts', 'prncpl_amnt', 'acces_fee', 'intrst_fee',
                        'maintnanc_fee', 'rollovr_fee', 'penlty_fee', 'days'
                    ],
                    rows=daily_closed_contracts[[
                        'id_date', 'cntrct_id', 'prdct', 'dt_creatd', 'dt_complte',
                        'id_idnty', 'cntrct_stts', 'prncpl_amnt', 'acces_fee', 'intrst_fee',
                        'maintnanc_fee', 'rollovr_fee', 'penlty_fee', 'days'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None),
                    commit_every=0
                )

                # Upload Contracts to MS Teams
                upload_closed_contracts_ms_teams(str(datetime.datetime.strptime(file_date, '%Y%m%d').date()))

                context['ti'].xcom_push(key='is_missing', value=False)
            else:
                context['ti'].xcom_push(key='is_missing', value=True)

        if tasks_to_run is not None:
            if context['task'].task_id in tasks_to_run:  # only specified tasks should run
                execute_task()
            else:
                context['ti'].xcom_push(key='is_missing', value=False)
        else:  # all tasks should run
            execute_task()
    def load_daily_disbursements(**context):
        """
        Loads and processes daily disbursements data and inserts it into a database table.

        :param context: The context dictionary with information about the current DAG run.
                        The dictionary should contain:
                        - 'dag_run': The current DAG run object.
                        - 'templates_dict': A dictionary with template variables used in the DAG.
                        - 'task': The current task instance object.
        """

        # Get the list of tasks to run from the DAG run configuration
        tasks_to_run = context['dag_run'].conf.get('tasks_to_run', None)

        def execute_task():
            """
            Load and process the daily disbursements data and insert it into the database table.
            """

            # Get the date of the files to be loaded
            file_date = context['dag_run'].conf.get('files_date', str((datetime.datetime.today() - datetime.timedelta(
                days=1)).date()).replace('-', ''))

            # Get the local path to the files
            path = f"{context['templates_dict']['local_files_directory_path']}/"

            # Load the daily disbursements data from the files
            daily_disbursements = []
            for filename in os.listdir(path):
                if 'daily_disbursements' in filename and file_date in filename:
                    try:
                        daily_disbursements.append(pd.read_csv("{}{}".format(path, filename)))
                    except pd.errors.EmptyDataError:
                        # If the file is empty, push an XCom with the appropriate flag
                        context['ti'].xcom_push(key='is_missing', value=False)
                        context['ti'].xcom_push(key='is_empty', value=True)

            # If there is data to process, continue with data processing and insertion
            if len(daily_disbursements) > 0:
                # Push an XCom with the appropriate flag to indicate that the data is not empty
                context['ti'].xcom_push(key='is_empty', value=False)

                # Concatenate the daily disbursements data and clean up the column values
                daily_disbursements = pd.concat(daily_disbursements, ignore_index=True).replace({'nan--': np.NAN})
                daily_disbursements['is_repayment'] = False
                daily_disbursements['is_disbursement'] = True
                daily_disbursements['ID_DATE'] = daily_disbursements['ID_DATE'].apply(
                    lambda x: '{}-{}-{}'.format(str(x).replace('.0', '')[0:4], str(x).replace('.0', '')[4:6],
                                                str(x).replace('.0', '')[6:])
                )
                daily_disbursements['DT_TRXN_END'] = daily_disbursements['DT_TRXN_END'].apply(
                    lambda x: datetime.datetime.strptime(str(x).replace('.0', ''), '%d/%m/%Y %H:%M') if '/' in str(
                        x).replace('.0', '') else str(x).replace('.0', '')
                )

                # Check for missing required fields in the data
                daily_disbursements.columns = [x.lower() for x in daily_disbursements.columns]
                required_fields = [
                    'id_loan_cntrct', 'trxn_amnt', 'id_date', 'id_trxn',
                    'cd_mpsa_orga_shrt', 'trxn_type', 'trxn_stts', 'id_trxn_linkd',
                    'id_idnty', 'src_assgnd_crdt_lmt', 'src_used_crdt_lmit',
                    'src_avail_crdt_lmit'
                ]
                daily_disbursements['has_missing_fields'] = False

                # START Data Checks
                daily_disbursements.loc[
                    missing_fields_check(df=daily_disbursements, fields=required_fields).index, 'has_missing_fields'
                ] = True
                cntrct_id_check_saf_disbursements_dump(
                    warehouse_hook=warehouse_hook
                )
                # END Data Checks

                warehouse_hook.insert_rows(
                    table='bloomlive.transactions_data_dump',
                    target_fields=[
                        'id_date', 'id_trxn', 'id_loan_cntrct', 'dt_trxn_end', 'src_term_loan',
                        'ds_mpsa_enty_name', 'cd_mpsa_orga_shrt', 'trxn_type', 'trxn_amnt', 'maintnanc_fee',
                        'trxn_stts', 'faild_reasn', 'id_trxn_linkd', 'id_idnty', 'src_lendr',
                        'src_crdt_score', 'src_assgnd_crdt_lmt', 'src_used_crdt_lmit', 'src_avail_crdt_lmit',
                        'is_repayment', 'is_disbursement', 'has_missing_fields'
                    ],
                    rows=daily_disbursements[[
                        'id_date', 'id_trxn', 'id_loan_cntrct', 'dt_trxn_end', 'src_term_loan',
                        'ds_mpsa_enty_name', 'cd_mpsa_orga_shrt', 'trxn_type', 'trxn_amnt', 'maintnanc_fee',
                        'trxn_stts', 'faild_reasn', 'id_trxn_linkd', 'id_idnty', 'src_lendr',
                        'src_crdt_score', 'src_assgnd_crdt_lmt', 'src_used_crdt_lmit', 'src_avail_crdt_lmit',
                        'is_repayment', 'is_disbursement', 'has_missing_fields'
                    ]].replace({np.NAN: None, 'nan--': None, 'nan': None}).itertuples(index=False, name=None),
                    commit_every=0
                )

                # Upload Disbursements to MS Teams
                upload_disbursements_ms_teams(str(datetime.datetime.strptime(file_date, '%Y%m%d').date()))

                context['ti'].xcom_push(key='is_missing', value=False)
            else:
                context['ti'].xcom_push(key='is_missing', value=True)

        if tasks_to_run is not None:
            if context['task'].task_id in tasks_to_run:  # only specified tasks should run
                execute_task()
            else:
                context['ti'].xcom_push(key='is_missing', value=False)
        else:  # all tasks should run
            execute_task()


    def load_daily_repayments(**context):
        """
        Loads daily repayment data from local files, processes it and inserts it into a database table.
        """

        # Get tasks to run from DAG configuration
        tasks_to_run = context['dag_run'].conf.get('tasks_to_run', None)

        def execute_task():
            # Get file date from DAG configuration or set it to yesterday
            file_date = context['dag_run'].conf.get('files_date', str((datetime.datetime.today() - datetime.timedelta(
                days=1)).date()).replace('-', ''))

            # Set path to directory containing files
            path = f"{context['templates_dict']['local_files_directory_path']}/"

            # Read data from files containing daily repayment data
            daily_repayment = []
            for filename in os.listdir(path):
                if 'daily_repayment' in filename and file_date in filename:
                    try:
                        daily_repayment.append(pd.read_csv("{}{}".format(path, filename)))
                    except pd.errors.EmptyDataError:
                        # Handle empty file by setting `is_empty` to True and `is_missing` to False
                        context['ti'].xcom_push(key='is_missing', value=False)
                        context['ti'].xcom_push(key='is_empty', value=True)

            # Process the daily repayment data
            if len(daily_repayment) > 0:
                context['ti'].xcom_push(key='is_empty', value=False)

                daily_repayment = pd.concat(daily_repayment, ignore_index=True).replace({'nan--': np.NAN})
                daily_repayment['is_repayment'] = True
                daily_repayment['is_disbursement'] = False
                daily_repayment['record_created_on_timestamp'] = datetime.datetime.now()

                # Format date fields
                daily_repayment['ID_DATE'] = daily_repayment['ID_DATE'].apply(
                    lambda x: '{}-{}-{}'.format(str(x).replace('.0', '')[0:4], str(x).replace('.0', '')[4:6],
                                                str(x).replace('.0', '')[6:])
                )
                daily_repayment['DT_TRXN_END'] = daily_repayment['DT_TRXN_END'].apply(
                    lambda x: datetime.datetime.strptime(str(x).replace('.0', ''), '%d/%m/%Y %H:%M') if '/' in str(
                        x).replace('.0', '') else str(x).replace('.0', '')
                )

                # Check for missing values
                daily_repayment.columns = [x.lower() for x in daily_repayment.columns]
                required_fields = [
                    'id_date', 'id_trxn', 'cd_mpsa_orga_shrt', 'trxn_type', 'trxn_amnt',
                    'maintnanc_fee', 'trxn_stts', 'id_trxn_linkd', 'id_idnty', 'id_loan_cntrct',
                    'dt_trxn_end'
                ]
                daily_repayment['has_missing_fields'] = False
                daily_repayment.loc[
                    missing_fields_check(df=daily_repayment, fields=required_fields).index, 'has_missing_fields'] = True

                # Insert processed data into database table
                warehouse_hook.insert_rows(
                    table='bloomlive.transactions_data_dump',
                    target_fields=[
                        'id_date', 'id_trxn', 'dt_trxn_end', 'ds_mpsa_enty_name', 'cd_mpsa_orga_shrt', 'trxn_type',
                        'trxn_amnt', 'maintnanc_fee', 'trxn_stts', 'faild_reasn', 'id_trxn_linkd', 'id_idnty',
                        'src_lendr', 'id_loan_cntrct', 'is_repayment', 'is_disbursement',
                        'record_created_on_timestamp', 'has_missing_fields'
                    ],
                    rows=daily_repayment[[
                        'id_date', 'id_trxn', 'dt_trxn_end', 'ds_mpsa_enty_name', 'cd_mpsa_orga_shrt', 'trxn_type',
                        'trxn_amnt', 'maintnanc_fee', 'trxn_stts', 'faild_reasn', 'id_trxn_linkd', 'id_idnty',
                        'src_lendr', 'id_loan_cntrct', 'is_repayment', 'is_disbursement',
                        'record_created_on_timestamp', 'has_missing_fields'
                    ]].replace({np.NAN: None, 'nan--': None, 'nan': None}).itertuples(index=False, name=None),
                    commit_every=0
                )

                # Upload Repayments to MS Teams
                upload_repayments_ms_teams(str(datetime.datetime.strptime(file_date, '%Y%m%d').date()))

                context['ti'].xcom_push(key='is_missing', value=False)
            else:
                context['ti'].xcom_push(key='is_missing', value=True)

        if tasks_to_run is not None:
            if context['task'].task_id in tasks_to_run:  # only specified tasks should run
                execute_task()
            else:
                context['ti'].xcom_push(key='is_missing', value=False)
        else:  # all tasks should run
            execute_task()


    def load_daily_active(**context):
        """
        Load daily active user data from local files and insert into database table.

        Parameters:
        - **context (dict): The context dictionary containing various parameters and objects used in the DAG run.

        Returns: None

        """

        # Get the list of tasks to run from DAG run configuration
        tasks_to_run = context['dag_run'].conf.get('tasks_to_run', None)

        def execute_task():
            """
            Helper function to execute the task of loading daily active user data.
            """

            # Get the date of the files to be processed from DAG run configuration, or use yesterday's date
            file_date = context['dag_run'].conf.get('files_date', str((datetime.datetime.today() - datetime.timedelta(
                days=1)).date()).replace('-', ''))

            # Get the local files directory path from template dictionary and replace tilde (~) with user's home directory
            path = f"{context['templates_dict']['local_files_directory_path']}/"

            # Initialize empty list to store daily active user data from files
            daily_active = []

            # Loop through all files in the directory and append those containing 'daily_active' and file_date in their names
            for filename in os.listdir(path):
                if 'daily_active' in filename and file_date in filename:
                    try:
                        daily_active.append(pd.read_csv("{}{}".format(path, filename), encoding="ISO-8859-1"))
                    except pd.errors.EmptyDataError:
                        # If file is empty, push xcom value to indicate empty data
                        context['ti'].xcom_push(key='is_missing', value=False)
                        context['ti'].xcom_push(key='is_empty', value=True)

            # If any daily active user data has been extracted from files
            if len(daily_active) > 0:
                context['ti'].xcom_push(key='is_empty', value=False)

                # Concatenate all daily active user data into a single dataframe
                daily_active = pd.concat(daily_active, ignore_index=True)

                # Reformat the OPT_IN_DATE column to match database format
                daily_active['OPT_IN_DATE'] = daily_active['OPT_IN_DATE'].apply(
                    lambda x: '{}-{}-{}'.format(str(x).replace('.0', '')[0:4], str(x).replace('.0', '')[4:6],
                                                str(x).replace('.0', '')[6:])
                )
                daily_active['is_opt_in'] = True

                # Check for missing values in required columns and mark rows with missing values
                daily_active.columns = [x.lower() for x in daily_active.columns]
                required_fields = [
                    'mpsa_orga_shrt', 'enty_name', 'cust_idnty_id', 'cust_id_nmbr',
                    'nr_mpsa_enty_phne', 'opt_in_date', 'ds_mpsa_enty_name'
                ]
                daily_active['has_missing_fields'] = False
                daily_active.loc[
                    missing_fields_check(df=daily_active, fields=required_fields).index, 'has_missing_fields'] = True

                # Insert daily active user data into the database table
                warehouse_hook.insert_rows(
                    table='bloomlive.client_activity_data_dump',
                    target_fields=[
                        'cust_id_type', 'cust_id_nmbr', 'cust_idnty_id', 'enty_name', 'opt_in_date',
                        'mpsa_orga_shrt', 'mpsa_enty_phne', 'ds_cst_name', 'is_opt_in'
                    ],
                    rows=daily_active[[
                        'cust_id_type', 'cust_id_nmbr', 'cust_idnty_id', 'enty_name', 'opt_in_date',
                        'mpsa_orga_shrt', 'nr_mpsa_enty_phne', 'ds_mpsa_enty_name', 'is_opt_in'
                    ]].replace({np.NAN: None, 'nan--': None, 'nan': None}).itertuples(index=False, name=None),
                    commit_every=0
                )

                # Upload Opt Ins to MS Teams
                # saf_data_dumps_remove_duplicates()
                upload_opt_ins_ms_teams(file_date=str(datetime.datetime.strptime(file_date, '%Y%m%d').date()))

                context['ti'].xcom_push(key='is_missing', value=False)
            else:
                context['ti'].xcom_push(key='is_missing', value=True)

        if tasks_to_run is not None:
            if context['task'].task_id in tasks_to_run:  # only specified tasks should run
                execute_task()
            else:
                context['ti'].xcom_push(key='is_missing', value=False)
        else:  # all tasks should run
            execute_task()


    def load_daily_deactive(**context):
        """
        Loads daily deactive data from CSV files and inserts it into a database table.

        Args:
            **context: keyword arguments containing task information and dependencies

        Returns:
            None
        """

        # Check if only specified tasks should run
        tasks_to_run = context['dag_run'].conf.get('tasks_to_run', None)

        # Function to execute the task
        def execute_task():
            # Get the date of the files to load
            file_date = context['dag_run'].conf.get('files_date', str((datetime.datetime.today() - datetime.timedelta(
                days=1)).date()).replace('-', ''))

            # Set the path to the local files directory
            path = f"{context['templates_dict']['local_files_directory_path']}/"

            # Initialize empty list to store daily deactive data
            daily_deactive = []

            # Loop over files in the directory
            for filename in os.listdir(path):
                # Check if the filename contains 'daily_deactive' and the file_date
                if 'daily_deactive' in filename and file_date in filename:
                    try:
                        # Read the file and append the data to the daily_deactive list
                        daily_deactive.append(pd.read_csv("{}{}".format(path, filename)))
                    except pd.errors.EmptyDataError:
                        # Push XCom values if the file is empty
                        context['ti'].xcom_push(key='is_missing', value=False)
                        context['ti'].xcom_push(key='is_empty', value=True)

            # Check if any data was loaded
            if len(daily_deactive) > 0:
                # Push XCom value to indicate data is not empty
                context['ti'].xcom_push(key='is_empty', value=False)

                # Concatenate the daily_deactive list into a single dataframe
                daily_deactive = pd.concat(daily_deactive, ignore_index=True)

                # Convert the OPT_OUT_DATE column to a datetime format
                daily_deactive['OPT_OUT_DATE'] = daily_deactive['OPT_OUT_DATE'].apply(
                    lambda x: '{}-{}-{}'.format(str(x).replace('.0', '')[0:4], str(x).replace('.0', '')[4:6],
                                                str(x).replace('.0', '')[6:])
                )

                # Convert column names to lowercase
                daily_deactive.columns = [x.lower() for x in daily_deactive.columns]

                # Add a new column 'is_opt_out' with value True
                daily_deactive['is_opt_out'] = True

                # Insert data into the database table using a hook
                warehouse_hook.insert_rows(
                    table='bloomlive.client_activity_data_dump',
                    target_fields=[
                        'cust_id_type', 'cust_id_nmbr', 'cust_idnty_id', 'enty_name', 'opt_out_date', 'mpsa_orga_shrt',
                        'mpsa_enty_phne', 'ds_cst_name', 'is_opt_out'
                    ],
                    rows=daily_deactive[[
                        'cust_id_type', 'cust_id_nmbr', 'cust_idnty_id', 'enty_name', 'opt_out_date', 'mpsa_orga_shrt',
                        'nr_mpsa_enty_phne', 'ds_mpsa_enty_name', 'is_opt_out'
                    ]].replace({np.NAN: None, 'nan--': None, 'nan': None}).itertuples(index=False, name=None),
                    commit_every=0
                )

                # Upload Opt Outs to MS Teams
                upload_opt_outs_ms_teams(str(datetime.datetime.strptime(file_date, '%Y%m%d').date()))

                context['ti'].xcom_push(key='is_missing', value=False)
            else:
                context['ti'].xcom_push(key='is_missing', value=True)

        if tasks_to_run is not None:
            if context['task'].task_id in tasks_to_run:  # only specified tasks should run
                execute_task()
            else:
                context['ti'].xcom_push(key='is_missing', value=False)
        else:  # all tasks should run
            execute_task()


    def load_daily_outstanding(**context):
        """
        Loads the daily outstanding loans data and inserts it into a database table.

        Parameters:
            **context: keyword arguments containing Airflow context variables

        Returns:
            None
        """
        # Retrieve the tasks to run from dag_run configuration
        tasks_to_run = context['dag_run'].conf.get('tasks_to_run', None)

        def execute_task():
            # Retrieve the date of the files to be loaded
            file_date = context['dag_run'].conf.get('files_date', str((datetime.datetime.today() - datetime.timedelta(
                days=1)).date()).replace('-', ''))

            # Retrieve the local file directory path
            path = f"{context['templates_dict']['local_files_directory_path']}/"

            # Load data from all files in the directory with the specified date and append to dataframe
            daily_outstanding = []
            for filename in os.listdir(path):
                if 'daily_outst' in filename and file_date in filename:
                    try:
                        daily_outstanding.append(pd.read_csv("{}{}".format(path, filename)))
                    except pd.errors.EmptyDataError:
                        context['ti'].xcom_push(key='is_missing', value=False)
                        context['ti'].xcom_push(key='is_empty', value=True)

            # If data is available, process and insert it into database table
            if len(daily_outstanding) > 0:
                context['ti'].xcom_push(key='is_empty', value=False)
                daily_outstanding = pd.concat(daily_outstanding, ignore_index=True)
                daily_outstanding['SRC_ID_DATE'] = daily_outstanding['SRC_ID_DATE'].apply(
                    lambda x: '{}-{}-{}'.format(str(x).replace('.0', '')[0:4], str(x).replace('.0', '')[4:6],
                                                str(x).replace('.0', '')[6:])
                )
                daily_outstanding['record_created_on_timestamp'] = datetime.datetime.now()
                warehouse_hook.insert_rows(
                    table='bloomlive.outstanding_loans_data_dump',
                    target_fields=[
                        'SRC_ID_DATE', 'SRC_MICRO_LOAN', 'SRC_CNTRCT_NMBR',
                        'SRC_OUTSTNDNG_AMNT', 'SRC_PRNCPAL_AMNT_UNPAID', 'SRC_MICRO_LOAN_NMBR',
                        'SRC_WTCH_MICRO_LOAN_NMBR', 'SRC_SUBSTNDRD_MICRO_LOAN_NMBR',
                        'SRC_LOST_MICRO_LOAN_NMBR', 'SRC_BAD_MICRO_LOAN_NMBR', 'record_created_on_timestamp'
                    ],
                    rows=daily_outstanding[[
                        'SRC_ID_DATE', 'SRC_MICRO_LOAN', 'SRC_CNTRCT_NMBR',
                        'SRC_OUTSTNDNG_AMNT', 'SRC_PRNCPAL_AMNT_UNPAID', 'SRC_MICRO_LOAN_NMBR',
                        'SRC_WTCH_MICRO_LOAN_NMBR', 'SRC_SUBSTNDRD_MICRO_LOAN_NMBR',
                        'SRC_LOST_MICRO_LOAN_NMBR', 'SRC_BAD_MICRO_LOAN_NMBR', 'record_created_on_timestamp'
                    ]].replace({np.NAN: None, 'nan--': None, 'nan': None}).itertuples(index=False, name=None),
                    commit_every=0
                )

                # Upload daily outstanding to MS Teams
                upload_outstanding_ms_teams(str(datetime.datetime.strptime(file_date, '%Y%m%d').date()))

                context['ti'].xcom_push(key='is_missing', value=False)
            else:
                context['ti'].xcom_push(key='is_missing', value=True)

        if tasks_to_run is not None:
            if context['task'].task_id in tasks_to_run:  # only specified tasks should run
                execute_task()
            else:
                context['ti'].xcom_push(key='is_missing', value=False)
        else:  # all tasks should run
            execute_task()


    def load_statement_details(**context):
        """
        Load statement details data into a Postgres table and upload it to MS Teams.

        :param context: the context dictionary containing Airflow variables and other context variables
        :type context: dict

        """

        # Get the list of tasks to run
        tasks_to_run = context['dag_run'].conf.get('tasks_to_run', None)

        # Define the function that executes the task
        def execute_task():
            # Get the date for the file to be loaded
            file_date = context['dag_run'].conf.get('files_date', str((datetime.datetime.today() - datetime.timedelta(
                days=1)).date()).replace('-', ''))
            # Define the path to the local files directory
            path = f"{context['templates_dict']['local_files_directory_path']}/"

            statements = []

            # Loop over the files in the directory and load the statement details data
            for filename in os.listdir(path):
                if 'statement_details' in filename and file_date in filename:
                    try:
                        statements.append(pd.read_csv("{}{}".format(path, filename)))
                    except pd.errors.EmptyDataError:
                        # Push to XCom to indicate that the file is empty
                        context['ti'].xcom_push(key='is_missing', value=False)
                        context['ti'].xcom_push(key='is_empty', value=True)

            # If there are statement details to load, process and insert them into the database
            if len(statements) > 0:
                # Push to XCom to indicate that the file is not empty
                context['ti'].xcom_push(key='is_empty', value=False)

                # Concatenate the statement details dataframes and clean the column names
                statements = pd.concat(statements, ignore_index=True)
                statements.columns = [x.strip().lower() for x in statements.columns]
                statements['id_date'] = statements['id_date'].apply(
                    lambda x: str(
                        datetime.datetime.strptime(str(x).replace('.0', ''), '%Y%m%d').date()) if not pd.isnull(
                        x) else x
                )

                # Insert the statement details into the database
                warehouse_hook.insert_rows(
                    table='bloomlive.statement_details_data_dump',
                    target_fields=['id_date', 'id_loan_cntrct', 'id_micro_loan_stmt', 'id_prdct', 'prdct_name',
                                   'tm_cntrct_create', 'dt_cntrct_due', 'id_idnty', 'micro_loan_stmt_stts', 'totl_amnt',
                                   'prncpl_amnt', 'acces_fee', 'intrst_fee', 'maintnanc_fee', 'rollovr_fee',
                                   'penlty_fee', 'outstndg_amnt', 'prncpl_amnt_unpaid', 'acces_fee_unpaid',
                                   'intrst_fee_unpaid', 'maintnanc_fee_unpaid', 'rollovr_fee_unpaid',
                                   'penlty_fee_unpaid', 'dt_due'],
                    rows=statements[['id_date', 'id_loan_cntrct', 'id_micro_loan_stmt', 'id_prdct', 'prdct_name',
                                     'tm_cntrct_create', 'dt_cntrct_due', 'id_idnty', 'micro_loan_stmt_stts', 'totl_amnt',
                                   'prncpl_amnt', 'acces_fee', 'intrst_fee', 'maintnanc_fee', 'rollovr_fee',
                                   'penlty_fee', 'outstndg_amnt', 'prncpl_amnt_unpaid', 'acces_fee_unpaid',
                                   'intrst_fee_unpaid', 'maintnanc_fee_unpaid', 'rollovr_fee_unpaid',
                                   'penlty_fee_unpaid', 'dt_due']].replace({np.NAN: None, 'nan--': None, 'nan': None}).itertuples(index=False, name=None),
                    commit_every=0
                )

                # Upload statements to MS Teams
                upload_statements_ms_teams(str(datetime.datetime.strptime(file_date, '%Y%m%d').date()))

                context['ti'].xcom_push(key='is_missing', value=False)
            else:
                context['ti'].xcom_push(key='is_missing', value=True)

        if tasks_to_run is not None:
            if context['task'].task_id in tasks_to_run:  # only specified tasks should run
                execute_task()
            else:
                context['ti'].xcom_push(key='is_missing', value=False)
        else:  # all tasks should run
            execute_task()

    def update_warehouse_company_names(**context):
        """
        Extracts store number and company name from the first name field of clients from Bloom 2.0's client dimension.
        It then updates the company name for any clients that have a null company name and a non-null company name in the client opt-in view.
        """

        # Retrieves clients with a null company name and non-null company name in the client opt-in view
        clients = warehouse_hook.get_pandas_df(
            sql="""
                select cd.surrogate_id, coiv.company_name
                from bloomlive.client_dimension cd
                left join bloomlive.client_opt_in_view coiv on cd.store_number = coiv.store_number
                where cd.bloom_version = '2' and cd.company_name is null and coiv.company_name is not null
            """
        )

        updates = []
        # Loops through each client and generates an update query to update the company name in the client dimension
        for index, client in clients.replace({np.NAN: None}).iterrows():
            company_name = client['company_name'].replace("'", "''")
            updates.append(
                f"UPDATE bloomlive.client_dimension SET company_name = '{company_name}' WHERE surrogate_id = {client['surrogate_id']} returning current_timestamp::varchar, '{company_name}', null, surrogate_id"
            )

        # If there are updates to be made, runs the queries and logs the updates
        if len(updates) > 0:
            updated = warehouse_hook.run(sql=updates, handler=lambda x: x.fetchall())
            store_company_name_updates(
                updated=updated,
                source='DUMPS',
                airflow_hook=MySqlHook(mysql_conn_id='mysql_airflow', schema='bloom_pipeline')
            )


    def update_warehouse_national_ids(**context):
        """
        Update national IDs in the warehouse by converting.

        Args:
            **context: Dictionary containing task metadata and variables.

        Returns:
            None
        """

        # Get the clients with long-format national IDs in the warehouse
        with_long_format_national_id_in_warehouse = warehouse_hook.get_pandas_df(
            sql="""
                with rnked as (
                    select *, rank() over (partition by cust_idnty_id order by opt_in_date asc) rnk from (
                        select csv1.national_id, csv1.surrogate_id, csv1.store_number, cadd.cust_id_nmbr, cadd.cust_idnty_id, cadd.opt_in_date from bloomlive.client_summary_view csv1
                        left join bloomlive.client_activity_data_dump cadd on csv1.national_id = cadd.cust_idnty_id
                        where length(csv1.national_id ) > 15 and csv1.national_id  ~'^[0-9\.]+$' and cadd.is_opt_in is true and csv1.bloom_version = '2'
                        and csv1.national_id is not null and cadd.cust_id_nmbr is not null and not cadd.cust_id_nmbr = '000000000' and cadd.is_opt_in is true
                    ) ids
                ) select * from rnked where rnk = 1
            """
        )

        # Get the clients without national IDs in the warehouse
        without_national_id_in_warehouse = warehouse_hook.get_pandas_df(
            sql="""
                select cd.surrogate_id, cadd.cust_id_nmbr from bloomlive.client_dimension cd 
                inner join bloomlive.client_activity_data_dump cadd on cd.store_number = cadd.mpsa_orga_shrt 
                where cd.bloom_version = '2' and cd.national_id is null and cadd.cust_id_nmbr is not null and cadd.opt_in_date is not null and cd.store_number is not null 
            """
        )

        # Prepare the updates for the national IDs in the warehouse
        updates = []
        for index, row in without_national_id_in_warehouse.replace({np.NAN: "null"}).iterrows():
            # For clients without national IDs, set the national ID to the customer ID number
            updates.append(
                f"UPDATE bloomlive.client_dimension SET national_id = '{row['cust_id_nmbr']}' WHERE surrogate_id = {row['surrogate_id']} returning current_timestamp::varchar, '{row['cust_id_nmbr']}', null, surrogate_id"
            )

        for index, row in with_long_format_national_id_in_warehouse.replace({np.NAN: "null"}).iterrows():
            # For clients with long-format national IDs, set the national ID to the customer ID number
            updates.append(
                f"update bloomlive.client_dimension set national_id = '{row['cust_id_nmbr']}' where surrogate_id = {row['surrogate_id']}  returning current_timestamp::varchar, '{row['cust_id_nmbr']}', '{row['national_id']}', surrogate_id"
            )

        if len(updates) > 0:
            # Execute the updates and log the results
            updated = warehouse_hook.run(sql=updates, handler=lambda x: x.fetchall())
            store_national_id_updates(
                updated=updated,
                source='DUMPS',
                airflow_hook=MySqlHook(mysql_conn_id='mysql_airflow', schema='bloom_pipeline')
            )

    common_params = {
        'remote_files_directory_path': default_args['remote_files_directory_path'],
        'local_files_directory_path': default_args['local_files_directory_path']
    }

    # TASKS
    t1 = BashOperator(
        task_id='purge_previous_files_on_airflow_server',
        bash_command="rm -f {{ local_files_directory_path }}/*.csv;",
        retries=0
    )
    t2 = SSHOperator(
        task_id='copy_files_from_safaricom_server_to_gateway_server',
        ssh_conn_id='ssh_gateway_server',
        command="""
            set -e  
            rm -f {{ remote_files_directory_path }}/*.csv;
            sshpass -p '%s' scp %s@%s:~/*{{ dag_run.conf["files_date"] if "files_date" in dag_run.conf else (macros.datetime.today() - macros.timedelta(days=1)) | ds_nodash }}* {{ remote_files_directory_path }};
            files=({{ remote_files_directory_path }}/*{{ dag_run.conf["files_date"] if "files_date" in dag_run.conf else (macros.datetime.today() - macros.timedelta(days=1)) | ds_nodash }}*);
            echo ${files[@]};
        """ % (
            Variable.get('safaricom_bloom_sftp_password'),
            Variable.get('safaricom_bloom_sftp_username'),
            Variable.get('safaricom_bloom_sftp_host')
        ),
        retries=10,
        cmd_timeout=20
    )
    t3 = CustomSFTPOperator(
        task_id="copy_files_from_gateway_server_to_airflow_server",
        ssh_conn_id="ssh_gateway_server",
        local_filepath="",
        remote_filepath="",
        operation="get",
        create_intermediate_dirs=True,
        dag=dag,
        retries=0,
        template_dict=common_params
    )
    t4 = PythonOperator(
        task_id='load_contract_details',
        provide_context=True,
        python_callable=load_contract_details,
        templates_dict=common_params
    )
    t20 = PythonOperator(
        task_id='load_daily_closed_contracts',
        provide_context=True,
        python_callable=load_daily_closed_contracts,
        templates_dict=common_params
    )
    t5 = PythonOperator(
        task_id='load_daily_disbursements',
        provide_context=True,
        python_callable=load_daily_disbursements,
        templates_dict=common_params
    )
    t6 = PythonOperator(
        task_id='load_daily_repayments',
        provide_context=True,
        python_callable=load_daily_repayments,
        templates_dict=common_params
    )
    t7 = PythonOperator(
        task_id='load_daily_active',
        provide_context=True,
        python_callable=load_daily_active,
        templates_dict=common_params
    )
    t8 = PythonOperator(
        task_id='load_daily_deactive',
        provide_context=True,
        python_callable=load_daily_deactive,
        templates_dict=common_params
    )
    t9 = PythonOperator(
        task_id='load_daily_outstanding',
        provide_context=True,
        python_callable=load_daily_outstanding,
        templates_dict=common_params
    )
    t10 = PythonOperator(
        task_id='load_statement_details',
        provide_context=True,
        python_callable=load_statement_details,
        templates_dict=common_params
    )
    t17 = PythonOperator(
        task_id='trigger_data_dumps_bloom_data_recon',
        provide_context=True,
        python_callable=trigger_data_dumps_bloom_data_recon,
    )
    t18 = PythonOperator(
        task_id='update_warehouse_national_ids',
        provide_context=True,
        python_callable=update_warehouse_national_ids,
    )
    t19 = PythonOperator(
        task_id='update_warehouse_company_names',
        provide_context=True,
        python_callable=update_warehouse_company_names,
    )

    t1 >> t2 >> t3 >> t4 >> t20 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t17 >> t18 >> t19
