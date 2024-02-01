import os
import sys
import logging
import getpass
import datetime
import pendulum
import numpy as np
import pandas as pd
from airflow import DAG
from datetime import timedelta
from airflow.models import Variable
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure, get_bloom_whitelist
from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
gateway_server_ssh_hook = SSHHook(ssh_conn_id="ssh_gateway_server")
log_format = "%(asctime)s: %(message)s"
logging.basicConfig(format=log_format, level=logging.WARNING, datefmt="%H:%M:%S")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure if Variable.get('DEBUG') == 'FALSE' else None,
    'remote_file_path': f'/root/data/safaricom_bloom/limit_refresh/bloom_current_whitelist.csv',
    'local_file_path': f"{os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))}/data/safaricom_bloom/limit_refresh/bloom_current_whitelist.csv"
}

local_tz = pendulum.timezone("Africa/Nairobi")


with DAG(
        'EL_live_bloom_whitelist',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 12 * * *',
        start_date=datetime.datetime(2023, 1, 30, 16, 20, tzinfo=local_tz),
        tags=['Extract Load'],
        description='Get deployed bloom whitelist',
        user_defined_macros=default_args,
        max_active_runs=1,
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    This dag retrieves the whitelist being shared with safaricom
    #### 
    """

    def save_current_whitelist(**context):
        airflow_hook = MySqlHook(mysql_conn_id='mysql_airflow', schema='bloom_pipeline')
        data = pd.read_csv(f"{context['templates_dict']['local_file_path']}")[1:-1]
        data.columns = [col.strip() for col in data.columns]
        data.rename(columns=lambda x: x.strip(), inplace=True)

        if data.shape[0] > 0:
            max_id2 = int(airflow_hook.get_pandas_df(
                sql="""select coalesce(max(id), 0) as id from bloom_pipeline.current_whitelist"""
            ).iloc[0]['id'])

            data['Store_Number'] = data['Store_Number'].apply(lambda x: x.strip())

            airflow_hook.run(sql="""DELETE FROM bloom_pipeline.current_whitelist_mock""")
            airflow_hook.insert_rows(
                table='bloom_pipeline.current_whitelist_mock',
                target_fields=[
                    'Store_Number', 'Asante_Blacklist_Flag', 'Asante_Credit_Limit_1_Day',
                    'Asante_Credit_Limit_7_Day', 'Asante_Credit_Limit_21_Day', 'CreatedOn_Date', 'ModifiedOn_Date'
                ],
                replace=False,
                rows=tuple(data[[
                    'Store_Number', 'Asante_Blacklist_Flag',
                    'Asante_Credit_Limit_1_Day', 'Asante_Credit_Limit_7_Day', 'Asante_Credit_Limit_21_Day',
                    'CreatedOn_Date', 'ModifiedOn_Date'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
                commit_every=2000
            )

            data = get_bloom_whitelist(
                data=data.copy(),
                warehouse_hook=PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
            )

            airflow_hook.insert_rows(
                table='bloom_pipeline.current_whitelist',
                target_fields=[
                    'bloom_version', 'Store_Number', 'mobile_number', 'primary_contact', 'first_name', 'middle_name', 'last_name', 'company_name', 'Asante_Blacklist_Flag', 'Asante_Credit_Limit_1_Day',
                    'Asante_Credit_Limit_7_Day', 'Asante_Credit_Limit_21_Day', 'Till_Suspended', 'limit_reason', 'communication',
                    'suspension_date', 'CreatedOn_Date', 'ModifiedOn_Date', 'secondary_contact', 'alternate_mobile_no1', 'alternate_mobile_no2'
                ],
                replace=False,
                rows=tuple(data[[
                    'bloom_version', 'Store_Number', 'mobile_number', 'primary_contact', 'first_name', 'middle_name', 'last_name', 'company_name', 'Asante_Blacklist_Flag',
                    'Asante_Credit_Limit_1_Day', 'Asante_Credit_Limit_7_Day', 'Asante_Credit_Limit_21_Day', 'is_suspended', 'proposed_summary_narration', 'communication_to_customer',
                    'start_date', 'CreatedOn_Date', 'ModifiedOn_Date', 'secondary_contact', 'alternate_mobile_no1', 'alternate_mobile_no2'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None)),
                commit_every=1000
            )

            airflow_hook.run(
                sql="""DELETE FROM bloom_pipeline.current_whitelist where id <= %(max_id)s""",
                parameters={'max_id': max_id2}
            )


    common_params = {
        'local_file_path': default_args['local_file_path']
    }

    t1 = SSHOperator(
        task_id='copy_current_whitelist_from_safaricom_server_to_gateway_server',
        ssh_hook=gateway_server_ssh_hook,
        command="""
            /opt/mssql-tools/bin/sqlcmd -S %s,%s -U %s -P '%s' -d '%s' -Q "SELECT Store_Number, Asante_Blacklist_Flag, Asante_Credit_Limit_1_Day, Asante_Credit_Limit_7_Day, Asante_Credit_Limit_21_Day, CreatedOn_Date, ModifiedOn_Date FROM dbo.PltAsanteFinanceList;" -o {{ remote_file_path }} -s"," -w 700
            """ % (
            Variable.get('safaricom_bloom_partner_scoring_results_database_host'),
            Variable.get('safaricom_bloom_partner_scoring_results_database_port'),
            Variable.get('safaricom_bloom_partner_scoring_results_database_username'),
            Variable.get('safaricom_bloom_partner_scoring_results_database_password'),
            Variable.get('safaricom_bloom_partner_scoring_results_database_name')
        ),
        retries=5,
        cmd_timeout=300,
        do_xcom_push=False
    )
    t2 = SFTPOperator(
        task_id="copy_files_from_gateway_server_to_airflow_server",
        ssh_conn_id="ssh_gateway_server",
        local_filepath=default_args['local_file_path'],
        remote_filepath=default_args['remote_file_path'],
        operation="get",
        create_intermediate_dirs=False,
        dag=dag,
        retries=0,
    )
    t3 = PythonOperator(
        task_id='save_current_whitelist',
        provide_context=True,
        python_callable=save_current_whitelist,
        templates_dict=common_params
    )
    t4 = MSTeamsWebhookOperator(
        task_id='send_current_whitelist_saved_ms_teams_notification',
        http_conn_id='msteams_webhook_url',
        message="""Bloom 2.0""",
        subtitle="The current list has been saved",
        dag=dag,
        retries=0
    )

    t1 >> t2 >> t3 >> t4
