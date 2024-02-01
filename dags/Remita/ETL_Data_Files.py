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
    'on_failure_callback': on_failure
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_Remita_Data',
        default_args=default_args,
        catchup=False,
        start_date=datetime.datetime(2023, 10, 31, 12, 50, tzinfo=local_tz),
        tags=['data transform'],
        description='Extract column values and clean already settled data',
        user_defined_macros=default_args,
        max_active_runs=1,
        schedule_interval='0 8 * * *' if Variable.get('DEBUG') == 'FALSE' else None,
) as dag:
    # DOCS
    dag.doc_md = """
    ### Remita Data ETL 
    This DAG extracts remita companies data from excel files and loads to DWH
    """


    def extract_data(file_name):
        """read data for repush from sharepoint file"""
        data_source = {
            'site': {'name': 'Data Dumps', 'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be'},
            'folders': [
                {'folder_path': 'Documents/Data Sharing/NIGERIA/Remita',
                 'id': '01BJPCNID245FF4HTYSZHZNDQRDWVTGKCJ'}
            ]
        }

        # Get access token to authenticate the API call and get list of files in the folder
        access_token = get_access_token()
        files = get_children(token=access_token, site_id=data_source['site']['id'],
                             item_id=data_source['folders'][0]['id'])

        file_exists = False
        for file in files:
            if file_name.upper() in str(file['name']).upper():
                file = file
                file_exists = True
                break

        if not file_exists:
            raise FileNotFoundError("The specified file was not found in the list of files.")

        data = pd.read_excel(
            read_file(site=data_source['site']['id'], file_id=file['id'], access_token=access_token),
            engine="openpyxl")

        data = data.iloc[0 if file_name.upper() != 'Deposit Money Banks'.upper() else 1:, 1:].reset_index(drop=True)
        data.columns = data.iloc[0]
        data.drop([0], inplace=True)
        data = data.apply(lambda x: x.astype(str).str.replace("  ", " "), axis=0)

        return data


    def load_data(**context):
        dmbs = extract_data("Deposit Money Banks")  # Deposit Money banks Data
        fgn_mdas = extract_data("FGN_MDAs Business_Names")  # Federal Businesses
        multinational_bs = extract_data("Multinational Business_Names")  # Multinational Business

        existing_multinational_bs = warehouse_hook.get_pandas_df(sql=f"""
                SELECT 
                    business_name
                FROM remita.company_dimension
                WHERE is_multinational
                """)

        existing_fgn_mdas = warehouse_hook.get_pandas_df(sql=f"""
                SELECT 
                    business_name
                FROM remita.company_dimension
                WHERE is_federal_govt
                """)

        existing_dmbs = warehouse_hook.get_pandas_df(sql=f"""
                SELECT 
                    instituition_name
                FROM remita.deposit_money_banks
                """)

        dmbs = dmbs[~dmbs['NAME OF INSTITUTION'].str.strip().isin(existing_dmbs['instituition_name'].str.strip())]
        fgn_mdas = fgn_mdas[
            ~fgn_mdas['BUSINESS NAMES'].str.strip().isin(existing_fgn_mdas['business_name'].str.strip())]
        multinational_bs = multinational_bs[~multinational_bs['BUSINESS NAMES'].str.strip().isin(
            existing_multinational_bs['business_name'].str.strip())]

        dmbs.rename(columns={'NAME OF INSTITUTION': 'instituition_name',
                             'HEAD OFFICE ADDRESS': 'address'}, inplace=True)
        dmbs = dmbs[['instituition_name', 'address']]

        fgn_mdas.rename(columns={'BUSINESS NAMES': 'business_name'}, inplace=True)
        fgn_mdas = fgn_mdas[['business_name']]
        fgn_mdas["is_federal_govt"] = True
        fgn_mdas["is_multinational"] = False

        multinational_bs.rename(columns={'BUSINESS NAMES': 'business_name'}, inplace=True)
        multinational_bs = multinational_bs[['business_name']]
        multinational_bs["is_multinational"] = True
        multinational_bs["is_federal_govt"] = False

        # Add federal and multinational companies to DWH
        for data in [fgn_mdas, multinational_bs]:
            if not data.empty:
                warehouse_hook.insert_rows(
                    table='remita.company_dimension',
                    target_fields=[
                        'business_name', 'is_multinational', 'is_federal_govt'
                    ],
                    rows=tuple(data[[
                        'business_name', 'is_multinational', 'is_federal_govt'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
                    commit_every=10
                )

        # Add new dmbs to DWH
        if not dmbs.empty:
            warehouse_hook.insert_rows(
                table='remita.deposit_money_banks',
                target_fields=[
                    'instituition_name', 'address'
                ],
                rows=tuple(dmbs[[
                    'instituition_name', 'address'
                ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
                commit_every=10
            )


    extract_load_data = PythonOperator(
        task_id='extract_transform_load_remita_data',
        dag=dag,
        provide_context=True,
        python_callable=load_data
    )

    notify_teams = MSTeamsWebhookOperator(
        task_id='notify_on_teams',
        http_conn_id='msteams_webhook_url',
        message="""Remita Data""",
        subtitle="""Remita data successfully extracted and loaded to DWH.""".strip(),
        dag=dag,
        retries=0
    )

    extract_load_data >> notify_teams
