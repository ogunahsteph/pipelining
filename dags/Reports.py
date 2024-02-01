import sys
import os
import json
import logging
import pendulum
import datetime
import numpy as np
import pandas as pd
from airflow import DAG
from datetime import timedelta
from io import StringIO
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utils.common import on_failure
from utils.office365_api import upload_file
from utils.aws_api import save_file_to_s3


warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
airflow_hook = MySqlHook(mysql_conn_id='mysql_airflow', schema='bloom_pipeline')
gateway_hook = MySqlHook(mysql_conn_id='gateway_server', schema='bloom_staging2')

log_format = "%(asctime)s: %(message)s"
logging.basicConfig(format=log_format, level=logging.WARNING, datefmt="%H:%M:%S")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure if Variable.get('DEBUG') == 'FALSE' else None,
    'execution_timeout': datetime.timedelta(minutes=5)
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'Reports',
        default_args=default_args,
        catchup=False,
        schedule_interval='0 * * * *' if Variable.get('DEBUG') == 'FALSE' else None,
        start_date=datetime.datetime(2021, 12, 7, 22, 50, tzinfo=local_tz),
        tags=['data transform'],
        description='Extract column values and clean already settled data',
        user_defined_macros=default_args,
        max_active_runs=1,
) as dag:
    def extract_data(df):
        dicts = []

        def get_data(x):
            try:
                raw = json.loads(
                    x['raw_request'].replace('\n', '').replace('\r', '').replace('"{', '{').replace('}"', '}'))
                resp = json.loads(
                    x['response_message'].replace('\n', '').replace('\r', '').replace('"{', '{').replace('}"', '}'))[
                    'responseMessage']
                raw['id'] = x['id']
                raw['response_message'] = resp
                raw['status_code'] = x['status_code']
                raw['request_time'] = x['request_time']
                dicts.append(raw)
            except json.JSONDecodeError as s:
                logging.warning(x)

        df.apply(lambda x: get_data(x), axis=1)

        return pd.DataFrame(dicts)

    def send_mail(mail_body, mail_subject, mail_to):
        import smtplib
        from airflow.models import Variable
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart

        username = "asantesupport@asantefinancegroup.com"
        password = Variable.get('asante_support_email_password')
        mail_from = "asantesupport@asantefinancegroup.com"
        mail_to = mail_to
        mail_subject = mail_subject
        mail_body = mail_body

        mimemsg = MIMEMultipart()
        mimemsg['From'] = mail_from
        mimemsg['To'] = mail_to
        mimemsg['Subject'] = mail_subject
        mimemsg.attach(MIMEText(mail_body, 'plain'))
        connection = smtplib.SMTP(host='smtp.office365.com', port=587)
        connection.starttls()
        connection.login(username, password)
        connection.send_message(mimemsg)
        connection.quit()


    def TEP_data_report(**context):
        data = warehouse_hook.get_pandas_df(
            sql="""
                SELECT
                    greatest(application_date_time, transaction_date_time) as "time",
                    coalesce(daily_total_opt_ins_count, 0) as daily_total_opt_ins_count,
                    coalesce(daily_total_disbursements_count, 0) as daily_total_disbursements_count,
                    coalesce(daily_total_repayments_count, 0) as daily_total_repayments_count,
                    coalesce(daily_total_disbursements_sum, 0) as daily_total_disbursements_sum,
                    coalesce(daily_total_repayments_sum,0) as daily_total_repayments_sum
                FROM (
                    SELECT count(*) as daily_total_opt_ins_count
                    FROM bloomlive.raw_tep_client_activity rtca
                    WHERE opt_in_date_time IS NOT NULL
                    AND DATE(opt_in_date_time) = current_date
                ) opt_ins_cnt,
                (
                    SELECT count(*) as daily_total_disbursements_count
                    FROM bloomlive.raw_tep_disbursements rtd
                    WHERE DATE(application_date_time) = current_date
                ) disb_cnt,
                (
                    SELECT count(*) as daily_total_repayments_count
                    FROM bloomlive.raw_tep_repayments rtr
                    WHERE DATE(transaction_date_time) = current_date
                ) reps_cnt,
                (
                    SELECT COALESCE(SUM(loan_amount), 0) AS daily_total_disbursements_sum, COALESCE(MAX(application_date_time), current_date) AS application_date_time
                    FROM bloomlive.raw_tep_disbursements rtd
                    WHERE DATE(application_date_time) = current_date
                ) lns_val,
                (
                    SELECT coalesce(sum(paid_amount),0) as daily_total_repayments_sum, COALESCE(MAX(transaction_date_time), current_date) as transaction_date_time
                    FROM bloomlive.raw_tep_repayments rtr
                    WHERE DATE(transaction_date_time) = current_date
                ) reps_val
            """
        )
        if data.shape[0] > 0:
            send_mail(
                mail_body=f"""
                    TEP Data Summary
    
                    -------------------------------------
                    Daily Summary: TEP data for {data.iloc[0]['time'].date()} as at {data.iloc[0]['time'].strftime('%H:%M:%S')} EAT
    
                    Total Opt Ins (Count) - {data.iloc[0]['daily_total_opt_ins_count']}
                    Total Disbursements (Count) - {data.iloc[0]['daily_total_disbursements_count']}
                    Total Repayments (Count) - {data.iloc[0]['daily_total_repayments_count']}
                    Total Disbursements (Value) - Ksh.{'{:,}'.format(round(data.iloc[0]['daily_total_disbursements_sum'], 3))}
                    Total Repayments (Value) - Ksh.{'{:,}'.format(round(data.iloc[0]['daily_total_repayments_sum'], 3))}
    
                """,
                mail_subject="TEP Data Alerts",
                mail_to="bloom2.0_migration@asantefinancegroup.com"
            )

    def closed_on_safaricom_open_on_mifos(**context):
        site = {
            'name': 'Data Dumps',
            'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be',
            'description': 'daily safaricom data dumps',
            'folder': '01BJPCNIBETLWC6I3GTBA2FZI34LFRIEU5'
        }

        # cleared on safaricom open on mifos
        data = warehouse_hook.get_pandas_df(
            sql="""
                SELECT concat('_', cntrct_id) as loan_contract_id, safaricom_loan_balance_date as closed_on_timestamp, lftmsv.loan_surrogate_id, lftmsv.loan_mifos_id, lftmsv.bloom_version, lftmsv.receipt_number
                FROM bloomlive.loans_fact_table_materialized_summary_view lftmsv
                where lftmsv.safaricom_loan_balance = 0 and lftmsv.principal_outstanding > 0
            """
        )
        data2 = warehouse_hook.get_pandas_df(
            sql="""
            select 
            lftmsv.loan_mifos_id, lftmsv.bloom_version,
            date(dt_complte) as closed_on_date_cc,
            lftmsv.loan_status as loan_status_mf,
            lftmsv.principal_amount as prcpl_mf, dccd.prncpl_amnt as prncpl_cc,
            t1.total_repayment_cc, lftmsv.total_repayment as total_repayment_mf,
            case when t1.total_repayment_cc - lftmsv.total_repayment > 0 then t1.total_repayment_cc - lftmsv.total_repayment else 0 end as missing_repay, 
            case when lftmsv.total_repayment - t1.total_repayment_cc > 0 then lftmsv.total_repayment - t1.total_repayment_cc else 0 end as overpayment,
            t1.total_penalty_fee_cc, penalty_charges_charged as total_penalty_fee_mf, 
            case when t1.total_penalty_fee_cc - penalty_charges_charged > 0 then t1.total_penalty_fee_cc - penalty_charges_charged else 0 end as missing_penalty_fee, 
            case when penalty_charges_charged - t1.total_penalty_fee_cc > 0 then penalty_charges_charged - t1.total_penalty_fee_cc else 0 end as overcharged_penalty,
            t1.total_acces_fee_cc as total_intrst_fee_cc, interest_charged as total_intrst_fee_mf,
            case when t1.total_acces_fee_cc - interest_charged > 0 then t1.total_acces_fee_cc - interest_charged else 0 end as missing_interest,
            t1.total_rollovr_fee_cc, fee_charges_charged as total_rollovr_fee_mf,
            case when t1.total_rollovr_fee_cc - fee_charges_charged > 0 then t1.total_rollovr_fee_cc - fee_charges_charged else 0 end as missing_roll_over,
            case when fee_charges_charged - t1.total_rollovr_fee_cc > 0 then fee_charges_charged - t1.total_rollovr_fee_cc else 0 end as overcharged_roll_over,
            CASE
                WHEN
                    -- Check if dccd.prdct contains a '-' (dash)
                    POSITION('-' IN dccd.prdct) > 0
                THEN
                    -- Split by '-' (dash) and calculate dpd_cc
                    current_date - (date(dccd.dt_creatd) + CAST(SPLIT_PART(SPLIT_PART(dccd.prdct, '-', 1), ' ', -1) AS INT))
                ELSE
            current_date - (date(dccd.dt_creatd) + SPLIT_PART(dccd.prdct, ' ', 2)::int)
            end as dpd_cc,
                case when current_date - expected_matured_on_date < 0 then 0
                    else current_date - expected_matured_on_date
                end as dpd_mf
            FROM (
                SELECT cntrct_id, 
                       SUM(prncpl_amnt + acces_fee + intrst_fee + maintnanc_fee + rollovr_fee + penlty_fee) as total_repayment_cc,
                       SUM(acces_fee) as total_acces_fee_cc,
                       SUM(penlty_fee) as total_penalty_fee_cc,
                       SUM(intrst_fee) as total_intrst_fee_cc,
                       SUM(rollovr_fee) as total_rollovr_fee_cc
                FROM bloomlive.daily_closed_contracts_dump
                GROUP BY cntrct_id
            ) t1 INNER JOIN bloomlive.daily_closed_contracts_dump dccd ON t1.cntrct_id = dccd.cntrct_id
            left join bloomlive.loans_fact_table_materialized_summary_view lftmsv on dccd.cntrct_id = lftmsv.cntrct_id 
            where lftmsv.loan_status = 300
            order by closed_on_date_cc desc
            """
        )

        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)
        upload_file(file_bytes=csv_buffer.getvalue(), file_name='cleared_on_safaricom_open_on_mifos_full_list.csv', site=site)

        csv_buffer.seek(0)
        data2.to_csv(csv_buffer, index=False)
        upload_file(file_bytes=csv_buffer.getvalue(), file_name='closed_contracts_open_on_mifos.csv', site=site)



    def bloom2_data_posting_summary(**context):

        if datetime.datetime.now().hour in (23, 10, 14, 16, 17):
            report = warehouse_hook.get_pandas_df(
                sql="""
                    with sub as (
                        select 
                            td.surrogate_id as mifos_transaction_surrogate_id, -- rtd.surrogate_id, rtr.surrogate_id, tdd.surrogate_id,
                            td.created_date as mifos_created_date,
                            td.receipt_number,
                            td.amount as td_amount,
                            case
                                when td.transaction_type_enum = 1 then true else false
                            end as td_is_disbursement,
                            case
                                when td.transaction_type_enum = 2 then true else false
                            end as td_is_repayment,
                            rtr.surrogate_id as rtr_surrogate_id,
                            rtd.surrogate_id as rtd_surrogate_id,
                            tdd.surrogate_id as tdd_surrogate_id,
                            rtr.status_code as tep_repayment_status_code,
                            rtr.paid_amount as rtr_paid_amount,
                            rtd.status_code as tep_disbursement_status_code,
                            rtd.loan_amount as rtd_loan_amount,
                            tdd.status_code as data_dump_status_code,
                            tdd.is_disbursement as tdd_is_disbursement,
                            tdd.is_repayment as tdd_is_repayment,
                            tdd.trxn_amnt as tdd_trxn_amnt,
                            case
                                when (rtd.surrogate_id is not null or rtr.surrogate_id is not null) then false else true
                            end as is_missing_in_tep_data,
                            case
                                when (rtd.surrogate_id is not null or rtr.surrogate_id is not null or tdd.surrogate_id is not null) then false else true
                            end as is_missing_in_data_dumps
                        from bloomlive.transactions_dimension td -- mifos
                        left join bloomlive.raw_tep_disbursements rtd on td.receipt_number = rtd.fund_movement_transaction_id 
                        left join bloomlive.raw_tep_repayments rtr on td.receipt_number = rtr.fund_movement_transaction_id 
                        left join bloomlive.transactions_data_dump tdd on td.receipt_number = tdd.id_trxn_linkd -- data dumps 
                    ) select 
                        dts.dt as "date", 
                        opt_in_volume_processed_successfully_on_tep,
                        opt_in_volume_failed_on_tep,
                        opt_in_volume_processed_from_dumps_missing_on_tep, 
                        opt_in_volume_processed_from_dumps_failed_on_tep,
                        opt_in_volume_failed_on_tep_failed_on_dumps_processed_manually,
                        opt_in_volume_missing_on_tep_missing_on_dumps_processed_manually,
                        coalesce(disb_volume_processed_successfully_on_tep, 0) as disb_volume_processed_successfully_on_tep,
                        coalesce(disb_value_processed_successfully_on_tep, 0) as disb_value_processed_successfully_on_tep,
                        coalesce(disb_volume_failed_on_tep, 0) as disb_volume_failed_on_tep,
                        coalesce(disb_value_failed_on_tep, 0) as disb_value_failed_on_tep,
                        coalesce(disb_volume_processed_from_dumps_missing_on_tep, 0) as disb_volume_processed_from_dumps_missing_on_tep, 
                        coalesce(disb_value_processed_from_dumps_missing_on_tep, 0) as disb_value_processed_from_dumps_missing_on_tep,
                        coalesce(disb_volume_processed_from_dumps_failed_on_tep, 0) as disb_volume_processed_from_dumps_failed_on_tep,
                        coalesce(disb_value_processed_from_dumps_failed_on_tep, 0) as disb_value_processed_from_dumps_failed_on_tep, 	--
                        coalesce(rep_volume_processed_successfully_on_tep, 0) as rep_volume_processed_successfully_on_tep,
                        coalesce(rep_value_processed_successfully_on_tep, 0) as rep_value_processed_successfully_on_tep,
                        coalesce(rep_volume_failed_on_tep, 0) as rep_volume_failed_on_tep,
                        coalesce(rep_value_failed_on_tep, 0) as rep_value_failed_on_tep,
                        coalesce(rep_volume_processed_from_dumps_missing_on_tep, 0) as rep_volume_processed_from_dumps_missing_on_tep, 
                        coalesce(rep_value_processed_from_dumps_missing_on_tep, 0) as rep_value_processed_from_dumps_missing_on_tep,
                        coalesce(rep_volume_processed_from_dumps_failed_on_tep, 0) as rep_volume_processed_from_dumps_failed_on_tep,
                        coalesce(rep_value_processed_from_dumps_failed_on_tep, 0) as rep_value_processed_from_dumps_failed_on_tep,
                        coalesce(disb_volume_failed_on_tep_failed_on_dumps_processed_manually, 0) as disb_volume_failed_on_tep_failed_on_dumps_processed_manually,
                        coalesce(disb_value_failed_on_tep_failed_on_dumps_processed_manually, 0) as disb_value_failed_on_tep_failed_on_dumps_processed_manually,
                        coalesce(disb_volume_missing_on_tep_missing_on_dumps_processed_manually, 0) as disb_volume_missing_on_tep_missing_on_dumps_processed_manually,
                        coalesce(disb_value_missing_on_tep_missing_on_dumps_processed_manually, 0) as disb_value_missing_on_tep_missing_on_dumps_processed_manually,
                        coalesce(rep_volume_failed_on_tep_failed_on_dumps_processed_manually, 0) as rep_volume_failed_on_tep_failed_on_dumps_processed_manually,
                        coalesce(rep_value_failed_on_tep_failed_on_dumps_processed_manually, 0) as rep_value_failed_on_tep_failed_on_dumps_processed_manually,
                        coalesce(rep_volume_missing_on_tep_missing_on_dumps_processed_manually, 0) as rep_volume_missing_on_tep_missing_on_dumps_processed_manually,
                        coalesce(rep_value_missing_on_tep_missing_on_dumps_processed_manually, 0) as rep_value_missing_on_tep_missing_on_dumps_processed_manually
                    from (
                        select dt::date 
                        from generate_series(current_date - 30, current_date, '1 day'::interval) dt
                    ) dts 
                    ------------------------------------------------------START TEP DISBURSEMENTS
                    left join ( -- disbursements volume processed successfully on TEP
                        select date(mifos_created_date), count(rtd_loan_amount) as disb_volume_processed_successfully_on_tep from sub 
                        where sub.tep_disbursement_status_code = '200' and rtd_loan_amount is not null
                        group by date(mifos_created_date)
                    ) dvopsot on dvopsot."date" = dts.dt left join ( -- disbursements value processed successfully on TEP
                        select date(mifos_created_date), sum(rtd_loan_amount) as disb_value_processed_successfully_on_tep from sub 
                        where sub.tep_disbursement_status_code = '200' and rtd_loan_amount is not null
                        group by date(mifos_created_date)
                    ) dvapsot on dvapsot."date" = dts.dt left join ( -- disbursements volume failed on TEP
                        select date(mifos_created_date), count(rtd_loan_amount) as disb_volume_failed_on_tep from sub 
                        where sub.tep_disbursement_status_code = '400' and rtd_loan_amount is not null
                        group by date(mifos_created_date)
                    ) dvofot on dvofot."date" = dts.dt left join ( -- disbursements value failed on TEP
                        select date(mifos_created_date), sum(rtd_loan_amount) as disb_value_failed_on_tep from sub 
                        where sub.tep_disbursement_status_code = '400' and rtd_loan_amount is not null
                        group by date(mifos_created_date)
                    ) dvafot on dvafot."date" = dts.dt left join ( -- disbursements volume processed successfully on dumps missing from TEP disbursements 
                        select date(mifos_created_date), count(tdd_trxn_amnt) as disb_volume_processed_from_dumps_missing_on_tep from sub 
                        where is_missing_in_tep_data is true and data_dump_status_code = '200' and tdd_is_disbursement is true
                        group by date(mifos_created_date)
                    ) dvopsodmftd on dvopsodmftd."date" = dts.dt left join ( -- disbursements value processed successfully on dumps missing from TEP disbursements 
                        select date(mifos_created_date), sum(tdd_trxn_amnt) as disb_value_processed_from_dumps_missing_on_tep from sub 
                        where is_missing_in_tep_data is true and data_dump_status_code = '200' and tdd_is_disbursement is true
                        group by date(mifos_created_date)
                    ) dvapsodmftd on dvapsodmftd."date" = dts.dt left join ( -- disbursements volume failed on TEP processed successfully from dumps
                        select date(mifos_created_date), count(tdd_trxn_amnt) as disb_volume_processed_from_dumps_failed_on_tep from sub 
                        where is_missing_in_tep_data is false and sub.tep_disbursement_status_code = '400' 
                        and data_dump_status_code = '200' and  tdd_is_disbursement is true
                        group by date(mifos_created_date)
                    ) dvofotpsfd on dvofotpsfd."date" = dts.dt left join ( -- disbursements value failed on TEP processed successfully from dumps
                        select date(mifos_created_date), sum(tdd_trxn_amnt) as disb_value_processed_from_dumps_failed_on_tep from sub 
                        where sub.tep_disbursement_status_code = '400' and data_dump_status_code = '200' and  tdd_is_disbursement is true
                        group by date(mifos_created_date)
                    ) dvafotpsfd on dvafotpsfd."date" = dts.dt 
                    ------------------------------------------------------START TEP REPAYMENTS
                    left join ( -- repayments volume processed successfully on TEP
                        select date(mifos_created_date), count(rtr_paid_amount) as rep_volume_processed_successfully_on_tep from sub 
                        where sub.tep_repayment_status_code = '200' and rtr_paid_amount is not null
                        group by date(mifos_created_date)
                    ) rvopsot on rvopsot."date" = dts.dt left join ( -- repayments value processed successfully on TEP
                        select date(mifos_created_date), sum(rtr_paid_amount) as rep_value_processed_successfully_on_tep from sub 
                        where sub.tep_repayment_status_code = '200' and rtr_paid_amount is not null
                        group by date(mifos_created_date)
                    ) rvapsot on rvapsot."date" = dts.dt left join ( -- repayments volume failed on TEP
                        select date(mifos_created_date), count(rtr_paid_amount) as rep_volume_failed_on_tep from sub 
                        where sub.tep_repayment_status_code = '400' and rtr_paid_amount is not null
                        group by date(mifos_created_date)
                    ) rvofot on rvofot."date" = dts.dt left join ( -- repayments value failed on TEP
                        select date(mifos_created_date), sum(rtr_paid_amount) as rep_value_failed_on_tep from sub 
                        where sub.tep_repayment_status_code = '400' and rtr_paid_amount is not null
                        group by date(mifos_created_date)
                    ) rvafot on rvafot."date" = dts.dt left join ( -- repayments volume processed successfully on dumps missing from TEP repayments 
                        select date(mifos_created_date), count(tdd_trxn_amnt) as rep_volume_processed_from_dumps_missing_on_tep from sub 
                        where is_missing_in_tep_data is true and data_dump_status_code = '200' and tdd_is_repayment is true
                        group by date(mifos_created_date)
                    ) rvopsodmftd on rvopsodmftd."date" = dts.dt left join ( -- repayments value processed successfully on dumps missing from TEP repayments 
                        select date(mifos_created_date), sum(tdd_trxn_amnt) as rep_value_processed_from_dumps_missing_on_tep from sub 
                        where is_missing_in_tep_data is true and data_dump_status_code = '200' and tdd_is_repayment is true
                        group by date(mifos_created_date)
                    ) rvapsodmftd on rvapsodmftd."date" = dts.dt left join ( -- repayments volume failed on TEP processed successfully from dumps
                        select date(mifos_created_date), count(*) as rep_volume_processed_from_dumps_failed_on_tep from sub 
                        where sub.tep_repayment_status_code = '400' and data_dump_status_code = '200' and  tdd_is_repayment is true
                        group by date(mifos_created_date)
                    ) rvofotpsfd on rvofotpsfd."date" = dts.dt left join ( -- repayments value failed on TEP processed successfully from dumps
                        select date(mifos_created_date), sum(tdd_trxn_amnt) as rep_value_processed_from_dumps_failed_on_tep from sub 
                        where sub.tep_repayment_status_code = '400' and data_dump_status_code = '200' and  tdd_is_repayment is true
                        group by date(mifos_created_date)
                    ) rvafotpsfd on rvafotpsfd."date" = dts.dt 
                    ------------------------------------------------------START MANUAL DISBURSEMENTS
                    left join ( -- disbursements volume failed on TEP, failed on Dumps, processed manually 
                        select date(mifos_created_date), count(td_amount) as disb_volume_failed_on_tep_failed_on_dumps_processed_manually from sub 
                        where tep_disbursement_status_code = '400' and data_dump_status_code = '400' and td_is_disbursement is true
                        group by date(mifos_created_date)
                    ) dvofotfodpm on dvofotfodpm."date" = dts.dt left join ( -- disbursements value failed on TEP, failed on Dumps, processed manually
                        select date(mifos_created_date), sum(td_amount) as disb_value_failed_on_tep_failed_on_dumps_processed_manually from sub 
                        where tep_disbursement_status_code = '400' and data_dump_status_code = '400' and td_is_disbursement is true 
                        group by date(mifos_created_date)
                    ) dvafotfodpm on dvafotfodpm."date" = dts.dt left join ( -- disbursements volume missing on TEP, missing on Dumps, processed manually 
                        select date(mifos_created_date), count(td_amount) as disb_volume_missing_on_tep_missing_on_dumps_processed_manually from sub 
                        where is_missing_in_tep_data is true and is_missing_in_data_dumps is true and td_is_disbursement is true
                        group by date(mifos_created_date)
                    ) dvomotmodpm on dvomotmodpm."date" = dts.dt left join ( -- disbursements value missing on TEP, missing on Dumps, processed manually
                        select date(mifos_created_date), sum(td_amount) as disb_value_missing_on_tep_missing_on_dumps_processed_manually from sub 
                        where is_missing_in_tep_data is true and is_missing_in_data_dumps is true and td_is_disbursement is true
                        group by date(mifos_created_date)
                    ) dvamotmodpm on dvamotmodpm."date" = dts.dt
                    ------------------------------------------------------START MANUAL REPAYMENTS
                    left join ( -- repayments volume failed on TEP, failed on Dumps, processed manually 
                        select date(mifos_created_date), count(td_amount) as rep_volume_failed_on_tep_failed_on_dumps_processed_manually from sub 
                        where tep_repayment_status_code = '400' and data_dump_status_code = '400' and td_is_repayment is true
                        group by date(mifos_created_date)
                    ) rvofotfodpm on rvofotfodpm."date" = dts.dt left join ( -- repayments value failed on TEP, failed on Dumps, processed manually
                        select date(mifos_created_date), sum(td_amount) as rep_value_failed_on_tep_failed_on_dumps_processed_manually from sub 
                        where tep_repayment_status_code = '400' and data_dump_status_code = '400' and td_is_repayment is true 
                        group by date(mifos_created_date)
                    ) rvafotfodpm on rvafotfodpm."date" = dts.dt left join ( -- repayments volume missing on TEP, missing on Dumps, processed manually 
                        select date(mifos_created_date), count(td_amount) as rep_volume_missing_on_tep_missing_on_dumps_processed_manually from sub 
                        where is_missing_in_tep_data is true and is_missing_in_data_dumps is true and td_is_repayment is true
                        group by date(mifos_created_date)
                    ) rvomotmodpm on rvomotmodpm."date" = dts.dt left join ( -- repayments value missing on TEP, missing on Dumps, processed manually
                        select date(mifos_created_date), sum(td_amount) as rep_value_missing_on_tep_missing_on_dumps_processed_manually from sub 
                        where is_missing_in_tep_data is true and is_missing_in_data_dumps is true and td_is_repayment is true
                        group by date(mifos_created_date)
                    ) rvamotmodpm on rvamotmodpm."date" = dts.dt left join (
                            with sub as (
                            select 
                                csv2.surrogate_id as mifos_opt_in_surrogate_id, -- rtd.surrogate_id, rtr.surrogate_id, tdd.surrogate_id,
                                csv2.submitted_on_date as mifos_submitted_on_date,
                                csv2.store_number,
                                rtca.surrogate_id as rtca_surrogate_id,
                                cadd.surrogate_id as cadd_surrogate_id,
                                rtca.status_code as tep_opt_in_status_code,
                                cadd.status_code as data_dump_status_code,
                                cadd.is_opt_in as cadd_is_opt_in,
                                rtca.is_opt_in as tep_is_opt_in,
                                case
                                    when (rtca.surrogate_id is not null) then false else true
                                end as is_missing_in_tep_data,
                                case
                                    when (cadd.surrogate_id is not null) then false else true
                                end as is_missing_in_data_dumps
                            from bloomlive.client_summary_view csv2 -- mifos
                            left join bloomlive.raw_tep_client_activity rtca on csv2.store_number = rtca.store_number 
                            left join bloomlive.client_activity_data_dump cadd on csv2.store_number = cadd.mpsa_orga_shrt 	
                        ) select 
                            dts.dt,
                            coalesce(opt_in_volume_processed_successfully_on_tep, 0) as opt_in_volume_processed_successfully_on_tep,
                            coalesce(opt_in_volume_failed_on_tep, 0) as opt_in_volume_failed_on_tep,
                            coalesce(opt_in_volume_processed_from_dumps_missing_on_tep, 0) as opt_in_volume_processed_from_dumps_missing_on_tep, 
                            coalesce(opt_in_volume_processed_from_dumps_failed_on_tep, 0) as opt_in_volume_processed_from_dumps_failed_on_tep,
                            coalesce(opt_in_volume_failed_on_tep_failed_on_dumps_processed_manually, 0) as opt_in_volume_failed_on_tep_failed_on_dumps_processed_manually,
                            coalesce(opt_in_volume_missing_on_tep_missing_on_dumps_processed_manually, 0) as opt_in_volume_missing_on_tep_missing_on_dumps_processed_manually
                        from (
                            select dt::date 
                            from generate_series(current_date - 30, current_date, '1 day'::interval) dt
                        ) dts 
                        ------------------------------------------------------START TEP DISBURSEMENTS
                        left join ( -- opt-ins volume processed successfully on TEP
                            select date(mifos_submitted_on_date), count(rtca_surrogate_id) as opt_in_volume_processed_successfully_on_tep from sub 
                            where sub.tep_opt_in_status_code = '200' and rtca_surrogate_id is not null and tep_is_opt_in is true
                            group by date(mifos_submitted_on_date)
                        ) oivopsot on oivopsot."date" = dts.dt left join ( -- opt in volume failed on TEP
                            select date(mifos_submitted_on_date), count(rtca_surrogate_id) as opt_in_volume_failed_on_tep from sub 
                            where sub.tep_opt_in_status_code = '400' and rtca_surrogate_id is not null and tep_is_opt_in is true
                            group by date(mifos_submitted_on_date)
                        ) oivofot on oivofot."date" = dts.dt left join ( -- opt_in volume processed successfully on dumps missing from TEP opt ins 
                            select date(mifos_submitted_on_date), count(cadd_surrogate_id) as opt_in_volume_processed_from_dumps_missing_on_tep from sub 
                            where is_missing_in_tep_data is true and data_dump_status_code = '200' and cadd_is_opt_in is true
                            group by date(mifos_submitted_on_date)
                        ) oivopsodmftoi on oivopsodmftoi."date" = dts.dt left join ( -- opt_ins volume failed on TEP processed successfully from dumps
                            select date(mifos_submitted_on_date), count(cadd_surrogate_id) as opt_in_volume_processed_from_dumps_failed_on_tep from sub 
                            where is_missing_in_tep_data is false and sub.tep_opt_in_status_code = '400' and tep_is_opt_in is true and data_dump_status_code = '200' and cadd_is_opt_in is true
                            group by date(mifos_submitted_on_date)
                        ) oivopfdfot on oivopfdfot."date" = dts.dt left join ( -- opt in volume failed on TEP, failed on Dumps, processed manually 
                            select date(mifos_submitted_on_date), count(mifos_opt_in_surrogate_id) as opt_in_volume_failed_on_tep_failed_on_dumps_processed_manually from sub 
                            where tep_opt_in_status_code = '400' and data_dump_status_code = '400' and tep_is_opt_in is true and cadd_is_opt_in is true
                            group by date(mifos_submitted_on_date)
                        ) oivofotfodpm on oivofotfodpm."date" = dts.dt left join ( -- opt in volume missing on TEP, missing on Dumps, processed manually 
                            select date(mifos_submitted_on_date), count(mifos_opt_in_surrogate_id) as opt_in_volume_missing_on_tep_missing_on_dumps_processed_manually from sub 
                            where is_missing_in_tep_data is true and is_missing_in_data_dumps is true and tep_is_opt_in is true and cadd_is_opt_in is true
                            group by date(mifos_submitted_on_date)
                        ) oivomotmodpm on oivomotmodpm."date" = dts.dt
                    ) opt_ins on opt_ins.dt = dts.dt
                    order by dts.dt desc
                """
            )

            report = report.iloc[0]
            send_mail(
                mail_body=f"""
                    Data Posting Summary

                    -------------------------------------
                    Summary for {report['date']} as at {datetime.datetime.now().strftime('%H:%M:%S')}

                    Disbursements volume processed successfully on tep - {report['disb_volume_processed_successfully_on_tep']}
                    Disbursements volume failed on tep - {report['disb_volume_failed_on_tep']}
                    Disbursements volume processed from dumps failed on tep - {report['disb_volume_processed_from_dumps_failed_on_tep']}
                    Disbursements volume failed on tep, failed on dumps, procesed manually - {report['disb_volume_failed_on_tep_failed_on_dumps_processed_manually']}
                    Disbursements volume processed from dumps missing on tep - {report['disb_volume_processed_from_dumps_missing_on_tep']}
                    Disbursements volume missing on tep, missing on dumps, processed manually - {report['disb_volume_missing_on_tep_missing_on_dumps_processed_manually']}
                    
                    Disbursements value processed successfully on tep - Ksh.{'{:,}'.format(round(report['disb_value_processed_successfully_on_tep'], 3))}
                    Disbursements value failed on tep - Ksh.{'{:,}'.format(round(report['disb_value_failed_on_tep'], 3))}
                    Disbursements value processed from dumps failed on tep - Ksh.{'{:,}'.format(round(report['disb_value_processed_from_dumps_failed_on_tep'], 3))}
                    Disbursements value failed on tep, failed on_dumps, processed manually - Ksh.{'{:,}'.format(round(report['disb_value_failed_on_tep_failed_on_dumps_processed_manually'], 3))}
                    Disbursements value processed from dumps missing on tep - Ksh.{'{:,}'.format(round(report['disb_value_processed_from_dumps_missing_on_tep'], 3))}
                    Disbursements value missing on tep, missing on dumps, processed manually - Ksh.{'{:,}'.format(round(report['disb_value_missing_on_tep_missing_on_dumps_processed_manually'], 3))}

                    Repayments volume processed successfully on tep - {report['rep_volume_processed_successfully_on_tep']}
                    Repayments volume failed on tep - {report['rep_volume_failed_on_tep']}
                    Repayments volume processed from dumps failed on tep - {report['rep_volume_processed_from_dumps_failed_on_tep']}
                    Repayments volume failed on tep, failed on dumps, processed_manually - {report['rep_volume_failed_on_tep_failed_on_dumps_processed_manually']}
                    Repayments volume processed from dumps missing on tep - {report['rep_volume_processed_from_dumps_missing_on_tep']}
                    Repayments volume missing on tep, missing on dumps, processed manually - {report['rep_volume_missing_on_tep_missing_on_dumps_processed_manually']}

                    Repayments value processed successfully on tep - Ksh.{'{:,}'.format(round(report['rep_value_processed_successfully_on_tep'], 3))}
                    Repayments value failed on tep - Ksh.{'{:,}'.format(round(report['rep_value_failed_on_tep'], 3))}
                    Repayments value processed from dumps failed on tep - Ksh.{'{:,}'.format(round(report['rep_value_processed_from_dumps_failed_on_tep'], 3))}
                    Repayments value failed on tep, failed on dumps, processed manually - Ksh.{'{:,}'.format(round(report['rep_value_failed_on_tep_failed_on_dumps_processed_manually'], 3))}
                    Repayments value processed from dumps missing on tep - Ksh.{'{:,}'.format(round(report['rep_value_processed_from_dumps_missing_on_tep'], 3))}
                    Repayments value missing on tep, missing on dumps, processed manually - Ksh.{'{:,}'.format(round(report['rep_value_missing_on_tep_missing_on_dumps_processed_manually'], 3))}
                    
                    opt in volume processed successfully on tep - {report['opt_in_volume_processed_successfully_on_tep']},
                    opt in volume failed on tep - {report['opt_in_volume_failed_on_tep']},
                    opt in volume processed from dumps, failed on tep - {report['opt_in_volume_processed_from_dumps_failed_on_tep']},
                    opt in volume failed on tep, failed on dumps, processed manually - {report['opt_in_volume_failed_on_tep_failed_on_dumps_processed_manually']},
                    opt in volume processed from dumps, missing on tep - {report['opt_in_volume_processed_from_dumps_missing_on_tep']}, 
                    opt in volume missing on tep, missing on dumps, processed manually - {report['opt_in_volume_missing_on_tep_missing_on_dumps_processed_manuall']}
                """,
                mail_subject="Bloom2.0 Data Posting Report",
                mail_to="bloom2.0datarecon@netorgft4232141.onmicrosoft.com"
            )


    def bloom_full_disbursements(**context):
        report = warehouse_hook.get_pandas_df(
            sql="""
                SELECT 
                    receipt_number, store_number, transaction_amount, transaction_date_time, 
                    "source", concat('_', loan_contract_id) as loan_contract_id, loan_mifos_id, 
                    concat('ContractID_', loan_contract_id) as ContractID_loan_contract_id
                from bloomlive.disbursements_view dv
            """
        )
        csv_buffer = StringIO()
        report.to_csv(csv_buffer, index=False)

        upload_file(file_name=f'full_disbursements.csv', file_bytes=csv_buffer.getvalue(), site={
            'name': 'Safaricom Bloom',
            'description': 'Bloom Data',
            'id': 'ea835111-82a4-467d-81ed-3c5994f4d899',
            'folder': '01YAZIWUMASE2VCOJK4BEL77QYPNBOYUOX'}
        )

    def bloom_tep_repayments(**context):
        start_date = (datetime.datetime.today() - datetime.timedelta(days=1))
        end_date = datetime.datetime.today().date()

        current_date = start_date.date()

        while current_date <= end_date:
            report = warehouse_hook.get_pandas_df(
                sql="""
                    SELECT surrogate_id, request_id, concat('cntrct_id_', loan_contract_id) as loan_contract_id,
                    due_date, loan_amount, loan_request_date, loan_balance, store_number, sender_name, recipient_name,
                    transaction_date_time, paid_amount, notification_description, lender_id, fund_movement_transaction_id
                    from bloomlive.raw_tep_repayments where  date(transaction_date_time) = %(current_date)s
                """,
                parameters={'current_date': current_date}
            )

            csv_buffer = StringIO()
            report.to_csv(csv_buffer, index=False)

            upload_file(file_name=f"tep_repayments_{str(current_date)}.csv", file_bytes=csv_buffer.getvalue().encode('utf-8'), site={
                'name': 'Data Dumps',
                'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be',
                'description': 'daily safaricom data dumps',
                'folder': '01BJPCNIBETLWC6I3GTBA2FZI34LFRIEU5'
            })

            current_date += datetime.timedelta(days=1)


    def bloom_tep_disbursements(**context):
        start_date = (datetime.datetime.today() - datetime.timedelta(days=1))
        end_date = datetime.datetime.today().date()

        current_date = start_date.date()

        while current_date <= end_date:
            report = warehouse_hook.get_pandas_df(
                sql="""
                    SELECT surrogate_id, request_id, concat('cntrct_id_', loan_number) as loan_number,
                    loan_amount, store_number, store_name, product, credit_limit_balance, loan_disbursement_transaction_id,
                    application_date_time, mobile_number, fund_movement_transaction_id
                    from bloomlive.raw_tep_disbursements where date(application_date_time) = %(current_date)s
                """,
                parameters={'current_date': current_date}
            )

            csv_buffer = StringIO()
            report.to_csv(csv_buffer, index=False)

            upload_file(file_name=f"tep_disbursements_{str(current_date)}.csv",
                        file_bytes=csv_buffer.getvalue().encode('utf-8'), site={
                    'name': 'Data Dumps',
                    'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be',
                    'description': 'daily safaricom data dumps',
                    'folder': '01BJPCNIBETLWC6I3GTBA2FZI34LFRIEU5'
                })

            current_date += datetime.timedelta(days=1)

    def bloom_tep_opt_ins(**context):
        start_date = (datetime.datetime.today() - datetime.timedelta(days=1))
        end_date = datetime.datetime.today().date()

        current_date = start_date.date()

        while current_date <= end_date:
            report = warehouse_hook.get_pandas_df(
                sql="""
                    SELECT surrogate_id, request_id, customer_name, customer_identity_id, store_number,
                    business_name, mobile_number, opt_in_date_time, national_id
                    from bloomlive.raw_tep_client_activity where is_opt_in and date(opt_in_date_time) = %(current_date)s
                """,
                parameters={'current_date': current_date}
            )
            if report.shape[0] > 0:
                csv_buffer = StringIO()
                report.to_csv(csv_buffer, index=False)

                upload_file(file_name=f"tep_opt_ins_{str(current_date)}.csv",
                            file_bytes=csv_buffer.getvalue().encode('utf-8'), site={
                        'name': 'Data Dumps',
                        'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be',
                        'description': 'daily safaricom data dumps',
                        'folder': '01BJPCNIBETLWC6I3GTBA2FZI34LFRIEU5'
                    })

            current_date += datetime.timedelta(days=1)


    def solv_loan_book_report(**context):
        from airflow.providers.mysql.hooks.mysql import MySqlHook

        mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-safaricom')
        current_date = str(datetime.datetime.today().date()).replace('-', '')
        prdcts = warehouse_hook.get_pandas_df(sql="""select mifos_id, name, currency_code from solv_bat.product_dimension""")

        report = mifos_hook.get_pandas_df(
            sql="""
                SELECT
                ml.id, client_id, ml.account_no, product_id, mpl.name, mc.mobile_no, display_name, loan_status_id, 
                loan_officer_id, mpl.currency_code, approved_principal, ml.annual_nominal_interest_rate, ml.repay_every,
                disbursedon_date, expected_maturedon_date, principal_disbursed_derived as 'Total Loan Amount',
                principal_repaid_derived as 'Total Principal Repaid', interest_charged_derived as 'Total Interest Amount',
                interest_repaid_derived as 'Total Interest Repaid', fee_charges_charged_derived as 'Total Fees Amount',
                fee_charges_repaid_derived as 'Total Fees Repaid', penalty_charges_charged_derived as 'Total Penalties Amount',
                penalty_charges_repaid_derived as 'Total Penalties Repaid', interest_waived_derived as 'Total Interest Waived',
                fee_charges_waived_derived as 'Total Fees Waived', penalty_charges_waived_derived as 'Total Penalties Waived',
                total_outstanding_derived as 'Outstanding Loan Balance'
                from `mifostenant-tanda`.m_loan ml
                inner join `mifostenant-tanda`.m_client mc on ml.client_id = mc.id
                inner join `mifostenant-tanda`.m_product_loan mpl on ml.product_id = mpl.id
                where ml.product_id in %(pi)s and ml.disbursedon_date > '2022-05-31'
            """,
            parameters={'pi': tuple(prdcts['mifos_id'].tolist())}
        )
        csv_buffer = StringIO()
        report.to_csv(path_or_buf=csv_buffer, index=False)
        upload_file(file_name=f'SOLVLOANBOOK{current_date}.csv', file_bytes=csv_buffer.getvalue().encode('utf-8'), site={
            'name': 'DataDumps',
            'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be',
            'description': 'SOLV LOAN BOOK',
            'folder': '01BJPCNIG4PVM62Q36LZA23MSNVJ657DSJ'
        })

    def bloom_full_repayments(**context):
        report = warehouse_hook.get_pandas_df(
            sql="""
                SELECT 
                    receipt_number, store_number, transaction_amount, transaction_date_time, 
                    "source", loan_contract_id, loan_mifos_id, concat('ContractID_', loan_contract_id) as ContractID_loan_contract_id
                from bloomlive.repayments_view rv
            """
        )

        csv_buffer = StringIO()
        report.to_csv(csv_buffer, index=False)

        upload_file(file_name=f'full_repayments.csv', file_bytes=csv_buffer.getvalue(), site={
            'name': 'Safaricom Bloom',
            'description': 'Bloom Data',
            'id': 'ea835111-82a4-467d-81ed-3c5994f4d899',
            'folder': '01YAZIWUMASE2VCOJK4BEL77QYPNBOYUOX'}
        )


    def safaricom_loan_balances_summary():
        report = warehouse_hook.get_pandas_df(
            sql="""
                select 
                    lftmsv.loan_mifos_id::varchar, concat('cntrct_', lftmsv.cntrct_id) as cntrct_id, lftmsv.receipt_number, lftmsv.store_number, safaricom_loan_balance,
                    safaricom_loan_balance_date
                from bloomlive.loans_fact_table_materialized_summary_view lftmsv
                where lftmsv.bloom_version = '2' and safaricom_loan_balance is not null
            """
        )

        csv_buffer = StringIO()
        report.to_csv(csv_buffer, index=False)

        upload_file(file_name=f'safaricom_balances.csv', file_bytes=csv_buffer.getvalue(), site={
            'name': 'Safaricom Bloom',
            'description': 'Bloom Data',
            'id': 'ea835111-82a4-467d-81ed-3c5994f4d899',
            'folder': '01YAZIWUMASE2VCOJK4BEL77QYPNBOYUOX'}
        )

    def till_activity_data_report(**context):
        dt = warehouse_hook.get_pandas_df(
            sql="""
                SELECT extract(year from transaction_time) AS year, extract(month from transaction_time) AS month, extract(day from transaction_time) AS day, extract(hour from transaction_time) AS hour, 
                count(*) as total_count, sum(amount) as total_sum
                FROM bloomlive.till_activity_dimension
                WHERE transaction_time >= NOW() - INTERVAL '3 months'
                GROUP BY year, month, day, hour
                ORDER BY year, month, day, hour;
            """
        )
        airflow_hook.run(sql="""delete from bloom_pipeline.till_activity_monitoring""")

        airflow_hook.insert_rows(
            table='bloom_pipeline.till_activity_monitoring',
            target_fields=[
                'year', 'month', 'day', 'hour', 'total_count', 'total_sum'
            ],
            rows=tuple(dt.replace({np.NAN: None})[[
                'year', 'month', 'day', 'hour', 'total_count', 'total_sum'
            ]].itertuples(index=False, name=None)),
            commit_every=1000
        )
        csv_buffer = StringIO()
        dt.to_csv(csv_buffer, index=False)
        s3_bucket_name = Variable.get('AFSG_aws_s3_dw_dev_bucket_name')

        save_file_to_s3(
            s3_file_key=f"Bloom/Reports/till_activity/past_3_months.csv",
            bucket_name=s3_bucket_name,
            file_bytes=csv_buffer.getvalue()
        )


    t1 = PythonOperator(
        task_id='TEP_data_report',
        provide_context=True,
        python_callable=TEP_data_report
    )
    t2 = PythonOperator(
        task_id='closed_on_safaricom_open_on_mifos',
        provide_context=True,
        python_callable=closed_on_safaricom_open_on_mifos
    )
    t3 = PythonOperator(
        task_id='bloom2_data_posting_summary',
        provide_context=True,
        python_callable=bloom2_data_posting_summary
    )
    t4 = PythonOperator(
        task_id='safaricom_loan_balances_summary',
        provide_context=True,
        python_callable=safaricom_loan_balances_summary
    )
    t5 = PythonOperator(
        task_id='till_activity_data_report',
        provide_context=True,
        python_callable=till_activity_data_report
    )
    t6 = PythonOperator(
        task_id='bloom_full_repayments',
        provide_context=True,
        python_callable=bloom_full_repayments
    )
    t7 = PythonOperator(
        task_id='bloom_full_disbursements',
        provide_context=True,
        python_callable=bloom_full_disbursements
    )
    t8 = PythonOperator(
        task_id='bloom_tep_repayments',
        provide_context=True,
        python_callable=bloom_tep_repayments
    )
    t9 = PythonOperator(
        task_id='solv_loan_book_report',
        provide_context=True,
        python_callable=solv_loan_book_report
    )
    t10 = PythonOperator(
        task_id='bloom_tep_disbursements',
        provide_context=True,
        python_callable=bloom_tep_disbursements
    )
    t11 = PythonOperator(
        task_id='bloom_tep_opt_ins',
        provide_context=True,
        python_callable=bloom_tep_opt_ins
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t11

