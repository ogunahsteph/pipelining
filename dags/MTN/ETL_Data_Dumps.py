import os
import shutil
import tempfile
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
from airflow.providers.sftp.hooks.sftp import SFTPHook

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import utils.warehouse as warehouse
from utils.common import on_failure
from utils.office365_api import get_access_token, read_file, get_children
from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


WAREHOUSE = warehouse.WAREHOUSE()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    # 'on_failure_callback': on_failure
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_MTN_Data_Dumps',
        default_args=default_args,
        catchup=False,
        start_date=datetime.datetime(2023, 10, 31, 12, 50, tzinfo=local_tz),
        tags=['data transform'],
        description='Extract column values and clean already settled data',
        user_defined_macros=default_args,
        max_active_runs=1,
        schedule_interval= None if Variable.get('DEBUG') == 'FALSE' else None,
) as dag:
    # DOCS
    dag.doc_md = """
    ### Remita Data ETL 
    This DAG extracts MTN Dumps data from excel files and loads to DWH
    """

    s3_bucket = Variable.get('mtn_uganda_s3_bucket')

    def Create_PySpace(PySpace, Remove_if_Exists=True):
        if Remove_if_Exists is True:
            if os.path.exists(PySpace):
                shutil.rmtree(PySpace, ignore_errors=True)
        if not os.path.exists(PySpace):
            os.makedirs(PySpace)
        return PySpace

    PY_Space = os.path.join(tempfile.gettempdir(), 'MTN_PY_Space')


    def list_files_in_directory(**kwargs):
        sftp_hook = SFTPHook(ftp_conn_id='mtn_sftp_server')
        remote_path = '/var/sftp/uploads'
        ti = kwargs['ti']
        ti.xcom_push('file_list', sftp_hook.list_directory(remote_path))


    def upload_to_s3(file_name, local_path, s3_bucket, s3_key, **kwargs):
        s3_hook = S3Hook(aws_conn_id='aws_connection')
        s3_hook.load_file(filename=local_path, bucket_name=s3_bucket, replace=True, key=s3_key)
        print(f"Uploaded {file_name} to s3://{s3_bucket}/{s3_key}")


    def copy_file(export_path, file_name, **kwargs):
        if not file_name.endswith('.tmp'):
            print(file_name)
            sftp_hook = SFTPHook(ftp_conn_id='mtn_sftp_server')
            remote_path = f'/var/sftp/uploads/{file_name}'
            local_path = os.path.join(export_path, file_name)
            sftp_hook.retrieve_file(remote_path, local_path)
            print("Copied: " + str(file_name))
            upload_to_s3(file_name = file_name, local_path=local_path,
                         s3_bucket=s3_bucket, s3_key=file_name)

    def upload_to_dwh():
        files_to_process = os.listdir(PY_Space)
        columns = ["kyc_msisdn", "dw_part", "dw_cpart", "kyc_momo_id", "kyc_id_type", "kyc_id_num", "kyc_dob",
                 "kyc_gender", "kyc_last_name", "kyc_first_name", "mom_registration_date", "gsm_nrd",
                 "mom_deposit_qty_6m", "mom_deposit_qty_3m", "mom_deposit_qty_1m", "mom_deposit_amt_6m",
                 "mom_deposit_amt_3m", "mom_deposit_amt_1m", "mom_deposit_uniq_acceptors_qty_6m",
                 "mom_deposit_uniq_acceptors_qty_3m", "mom_deposit_uniq_acceptors_qty_1m", "mom_deposit_max_amt_6m",
                 "mom_deposit_max_amt_3m", "mom_deposit_max_amt_1m", "mom_deposit_last_date", "mom_withdrawal_qty_6m",
                 "mom_withdrawal_qty_3m", "mom_withdrawal_qty_1m", "mom_withdrawal_amt_6m", "mom_withdrawal_amt_3m",
                 "mom_withdrawal_amt_1m", "mom_withdrawal_uniq_acceptors_qty_6m",
                 "mom_withdrawal_uniq_acceptors_qty_3m", "mom_withdrawal_uniq_acceptors_qty_1m",
                 "mom_withdrawal_max_amt_6m", "mom_withdrawal_max_amt_3m", "mom_withdrawal_max_amt_1m",
                 "mom_withdrawal_last_date", "mom_bill_payment_qty_6m", "mom_bill_payment_qty_3m",
                 "mom_bill_payment_qty_1m", "mom_bill_payment_amt_6m", "mom_bill_payment_amt_3m",
                 "mom_bill_payment_amt_1m", "mom_bill_payment_uniq_acceptors_qty_6m",
                 "mom_bill_payment_uniq_acceptors_qty_3m", "mom_bill_payment_uniq_acceptors_qty_1m",
                 "mom_bill_payment_max_amt_6m", "mom_bill_payment_max_amt_3m", "mom_bill_payment_max_amt_1m",
                 "mom_bill_payment_last_date", "mom_p2p_send_qty_6m", "mom_p2p_send_qty_3m", "mom_p2p_send_qty_1m",
                 "mom_p2p_send_amt_6m", "mom_p2p_send_amt_3m", "mom_p2p_send_amt_1m", "mom_send_uniq_acceptors_qty_6m",
                 "mom_send_uniq_acceptors_qty_3m", "mom_send_uniq_acceptors_qty_1m", "mom_p2p_send_max_amt_6m",
                 "mom_p2p_send_max_amt_3m", "mom_p2p_send_max_amt_1m", "mom_p2p_send_last_date",
                 "mom_p2p_received_qty_6m", "mom_p2p_received_qty_3m", "mom_p2p_received_qty_1m",
                 "mom_p2p_received_amt_6m", "mom_p2p_received_amt_3m", "mom_p2p_received_amt_1m",
                 "mom_p2p_received_uniq_acceptors_qty_6m", "mom_p2p_received_uniq_acceptors_qty_3m",
                 "mom_p2p_received_uniq_acceptors_qty_1m", "mom_p2p_received_max_amt_6m", "mom_p2p_received_max_amt_3m",
                 "mom_p2p_received_max_amt_1m", "mom_p2p_received_last_date", "mom_bank_pull_qty_6m",
                 "mom_bank_pull_qty_3m", "mom_bank_pull_qty_1m", "mom_bank_pull_amt_6m", "mom_bank_pull_amt_3m",
                 "mom_bank_pull_amt_1m", "mom_bank_pull_uniq_acceptors_qty_6m", "mom_bank_pull_uniq_acceptors_qty_3m",
                 "mom_bank_pull_uniq_acceptors_qty_1m", "mom_bank_pull_max_amt_6m", "mom_bank_pull_max_amt_3m",
                 "mom_bank_pull_max_amt_1m", "mom_bank_pull_last_date", "mom_bank_push_qty_6m", "mom_bank_push_qty_3m",
                 "mom_bank_push_qty_1m", "mom_bank_push_max_amt_6m", "mom_bank_push_max_amt_3m",
                 "mom_bank_push_max_amt_1m", "mom_bank_push_uniq_acceptors_qty_6m",
                 "mom_bank_push_uniq_acceptors_qty_3m", "mom_bank_push_uniq_acceptors_qty_1m", "mom_bank_push_amt_6m",
                 "mom_bank_push_amt_3m", "mom_bank_push_amt_1m", "mom_bank_push_last_date", "gsm_npt", "gsm_nau_amt_1m",
                 "gsm_nau_amt_3m", "gsm_nau_amt_6m", "gsm_dab_qty_1m", "gsm_dab_qty_3m", "gsm_dab_qty_6m",
                 "gsm_la_days_qty", "gsm_active_days_qty_1m", "gsm_active_days_qty_3m", "gsm_active_days_qty_6m",
                 "gsm_nta_amt_1m", "gsm_nta_amt_3m", "gsm_nta_amt_6m", "gsm_topup_method", "gsm_network_town",
                 "gsm_times_blocked_po_qty_1m", "gsm_times_blocked_po_qty_3m", "gsm_times_blocked_po_qty_6m",
                 "gsm_po_limit_amt", "gsm_out_calls_qty_1m", "gsm_out_calls_qty_3m", "gsm_out_calls_qty_6m",
                 "gsm_uniq_out_bnum_qty_1m", "gsm_uniq_out_bnum_qty_3m", "gsm_uniq_out_bnum_qty_6m",
                 "gsm_in_calls_qty_1m", "gsm_in_calls_qty_3m", "gsm_in_calls_qty_6m", "gsm_uniq_in_anum_qty_1m",
                 "gsm_uniq_in_anum_qty_3m", "gsm_uniq_in_anum_qty_6m", "gsm_avg_topup_amt_1m", "gsm_avg_topup_amt_3m",
                 "gsm_avg_topup_amt_6m", "gsm_device_make", "gsm_dat_vol_amt_1m", "gsm_dat_vol_amt_3m",
                 "gsm_dat_vol_amt_6m", "gsm_last_bundle_purchase", "gsm_days_bund_less_2mb_1m",
                 "gsm_days_bund_less_2mb_3m", "gsm_days_bund_less_2mb_6m", "gsm_avg_bund_size_1m",
                 "gsm_avg_bund_size_3m", "gsm_avg_bund_size_6m", "gsm_me2u_received_qty_1m", "gsm_me2u_received_qty_3m",
                 "gsm_me2u_received_qty_6m", "gsm_me2u_received_amt_1m", "gsm_me2u_received_amt_3m",
                 "gsm_me2u_received_amt_6m", "atc_loans_qty_1m", "atc_loans_qty_3m", "atc_loans_qty_6m",
                 "atc_loans_amt_tot", "atc_loans_first_date", "atc_loans_last_date", "atc_loans_overdue",
                 "atc_loans_tot_term", "atc_loans_max_term", "atc_loans_last_overdue", "batch_id", "create_dt",
                 "kyc_msisdn_random", "ewp_account_holder_type_cd", "opco_cd", "p_ucid", "gsm_imsi_nr", "gsm_imei_nr",
                 "gsm_imsi_qty_1m", "gsm_imei_qty_1m", "source", "gsm_p_usid", "gsm_s_ucid", "gsm_s_usid",
                 "max_completeness_date", "date_key", "kyc_title", "status_code", "kyc_id_expiry_date", "tbl_dt"]

        for file in files_to_process:
            if file.endswith('.csv'):
                for upload_data in pd.read_csv(os.path.join(PY_Space, file), dtype='str', chunksize=10000):
                    # upload_data
                    if not upload_data.empty:
                        WAREHOUSE.insert_new_records_to_dwh(
                            data=upload_data,
                            warehouse_db='mtn_uganda.kyc_data_dumps',
                            columns=columns
                        )
                        WAREHOUSE.update_dwh_data(
                            mambu_data=upload_data,
                            warehouse_db='mtn_uganda.kyc_data_dumps',
                            data_columns=columns
                        )


    def copy_and_upload_files(**kwargs):
        ti = kwargs['ti']
        file_list = ti.xcom_pull(task_ids='list_files_in_directory', key='file_list')
        export_path = Create_PySpace(PY_Space)
        for file_name in file_list:
            copy_file(export_path, file_name, **kwargs)
        print(export_path)


    list_files = PythonOperator(
        task_id='list_files_in_directory',
        python_callable=list_files_in_directory,
        dag=dag,
    )

    copy_files_task = PythonOperator(
        task_id='copy_files_from_mtn_server',
        python_callable=copy_and_upload_files,
        provide_context=True,
        dag=dag,
    )

    upload_data = PythonOperator(
        task_id='data_process_and_upload_to_dwh',
        python_callable=upload_to_dwh,
        provide_context=True,
        dag=dag,
    )

    list_files >> copy_files_task >> upload_data