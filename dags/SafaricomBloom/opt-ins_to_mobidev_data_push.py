import os
import sys
import pytz
import pendulum
import datetime
import numpy as np
import pandas as pd
from airflow import DAG
from datetime import timedelta
import json
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from io import BytesIO

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure
from utils.office365_api import get_access_token, read_file, get_children
from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure,
}

end_point_url = 'http://10.176.0.53/bloomAutomation/bloomapi/optintomobidev.php'
headers = {
    'Content-Type': 'application/json',
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'Opt-ins_to_Mobidev_Data_Push',
        default_args=default_args,
        catchup=False,
        start_date=datetime.datetime(2023, 10, 31, 12, 50, tzinfo=local_tz),
        tags=['data transform'],
        description='Extract column values and clean already settled data',
        user_defined_macros=default_args,
        max_active_runs=1,
        schedule_interval=None,
) as dag:
    # DOCS
    dag.doc_md = """
    ### Extract data from warehouse and submit to Mobidev

    This DAG extracts data from warehouse using the sharepoint file containing the customer store numbers
    
    - Endpoint shared by digital:

    `curl --location 'http://10.176.0.53/bloomAutomation/bloomapi/optintomobidev.php' \
    --header 'Content-Type: application/json' \
    --data '{
        "businessName":"Stanley Test businessName",
        "storeNumber":"222222222",
        "telephone":"25471475003456",
        "optinDateTime":"20220519113438"
    }' `    
    
    """

    def read_data_to_push():
        """read data for repush from sharepoint file"""
        payload = {
            'site': {'name': 'Data Dumps', 'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be'},
            'folders': [
                {'name': 'Mobidev',
                 'id': '01BJPCNIFWNF7EBMNX4RCKMTH6NS76JLBI'}
            ]
        }

        # Get access token to authenticate the API call and get list of files in the folder
        access_token = get_access_token()
        files = get_children(token=access_token, site_id=payload['site']['id'],
                             item_id=payload['folders'][0]['id'])

        file_exists = False
        for file in files:
            if 'Opt_Ins Repush to Mobidev'.upper() in str(file['name']).upper():
                file = file
                file_exists = True
                break

        if not file_exists:
            raise FileNotFoundError("The specified file was not found in the list of files.")

        data = pd.read_excel(
            read_file(site=payload['site']['id'], file_id=file['id'], access_token=access_token),
            engine="openpyxl")

        return data.iloc[:, 0].astype(str).to_list()

    def extract_optin_data():
        store_numbers = read_data_to_push()
        opt_in_data = warehouse_hook.get_pandas_df(sql=f"""
                SELECT 
                    business_name as businessName, 
                    store_number as storeNumber, 
                    mobile_number as telephone, 
                    TO_CHAR(opt_in_date_time, 'YYYYMMDDHH24MISS') as optinDateTime
                FROM bloomlive.raw_tep_client_activity
                WHERE store_number IN {tuple(store_numbers)} AND is_opt_in
                """)

        json_data = opt_in_data.to_json(orient='records')  # Convert DB DataFrame to JSON
        parsed_data = pd.read_json(json_data)  # Parse the JSON data

        # Iterate over the records and print them
        for index, row in parsed_data.iterrows():
            data_to_submit = row.to_dict()
            data = {
                "businessName": str(data_to_submit["businessname"]),
                "storeNumber": str(data_to_submit["storenumber"]),
                "telephone": str(data_to_submit["telephone"]),
                "optinDateTime": str(data_to_submit["optindatetime"])
            }

            response = requests.post(end_point_url, headers=headers, data=json.dumps(data))

            if response.status_code == 200:
                print("Request was successful. >> " + response.text)
            else:
                print(f"Request failed with status code {response.status_code}.")


    submit_data_to_mobidev = PythonOperator(
        task_id='opt-ins_to_mobidev_data_push',
        dag=dag,
        provide_context=True,
        python_callable=extract_optin_data
    )

    notify_teams = MSTeamsWebhookOperator(
        task_id='notify_teams',
        http_conn_id='msteams_webhook_url',
        message="""Opt-ins Data Push to Mobidev""",
        subtitle="""Opt-ins data successfully pushed to mobidev.""".strip(),
        dag=dag,
        retries=0
    )

    submit_data_to_mobidev >> notify_teams
