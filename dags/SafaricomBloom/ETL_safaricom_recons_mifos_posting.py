import os
import sys
import json
import datetime
import requests
import pendulum
import logging
import pandas as pd
from airflow import DAG
from urllib import parse
from airflow.models import Variable
from concurrent.futures import ThreadPoolExecutor
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator
from utils.common import on_failure

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
gateway_hook = MySqlHook(mysql_conn_id='gateway_server', schema='bloom_staging')
mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-safaricom')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'on_failure_callback': on_failure if Variable.get('DEBUG') == 'FALSE' else None,
    'timedelta_minus_days': 0
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_safaricom_recons_mifos_posting',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        start_date=datetime.datetime(2022, 3, 24, 14, 30, tzinfo=local_tz),
        tags=['Reconciliations'],
        description='Post data dumps to mifos',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    DAG is triggered remotely by ETL_safaricom_data_dumps DAG
    DAG posts opt ins and disbursements by posting to middleware api provided by the Engineering team.
    """

    common_params = {'timedelta_minus_days': '{{timedelta_minus_days}}',}

    def generate_request_payload(row):
        return json.dumps({
            'requestId': row['store_number'], 'optinDateTime': row['registration_date'],
            'customerName': ' '.join(row['first_name'].split()) if not pd.isnull(row['first_name']) else row['first_name'], 'customerIdentityId': row['national_id'],
            'storeNumber': row['store_number'], 'businessName': ' '.join(row['business_name'].split()) if not pd.isnull(row['business_name']) else row['business_name'],
            'mobileNumber': row['primary_phone_no']
        })

    def get_opt_in_results(data: pd.DataFrame) -> pd.DataFrame:
        if data.shape[0] > 0:
            return gateway_hook.get_pandas_df(
                sql="""
                    select * from (
                        select count(*) as success from bloom_staging.opt_ins_data_dump 
                        where status_code = 200 and id in %(ids)s
                    ) as success,
                    (
                        select count(*) as exceptions from bloom_staging.opt_ins_data_dump
                        where status_code = 400 and id in %(ids)s
                    ) as exceptions,
                    (
                        select count(*) as skipped from bloom_staging.opt_ins_data_dump
                        where status_code is null and id in %(ids)s
                    ) as skipped
                """,
                parameters={'ids': tuple(data['id'].tolist())}
            )

        return pd.DataFrame()

    def get_statement_results(data: pd.DataFrame, charge_type: str):
        status_code = f'status_code_{charge_type}'

        if data.shape[0] > 0:
            return gateway_hook.get_pandas_df(
                sql=f"""
                    select * from (
                        select count(*) as success from bloom_staging.statement_details_data_dump
                        where {status_code} = 200 and id in %(ids)s
                    ) as success,
                    (
                        select count(*) as exceptions from bloom_staging.statement_details_data_dump
                        where {status_code} = 400 and id in %(ids)s
                    ) as exceptions,
                    (
                        select count(*) as skipped from bloom_staging.statement_details_data_dump
                        where {status_code} is null and id in %(ids)s
                    ) as skipped
                """,
                parameters={'ids': tuple(data['id'].tolist()), 'status_code': status_code}
            )

        return pd.DataFrame()


    def get_transaction_results(data: pd.DataFrame) -> pd.DataFrame:
        if data.shape[0] > 0:
            return gateway_hook.get_pandas_df(
                sql="""
                    select * from (
                        select count(*) as success from bloom_staging.transactions_data_dump tdd where id in %(ids)s
                        and status_code = 200
                    ) as success,
                    (
                        select count(*) as exceptions from bloom_staging.transactions_data_dump tdd where id in %(ids)s
                        and status_code = 400
                    ) as exceptions,
                    (
                        select count(*) as skipped from bloom_staging.transactions_data_dump tdd where id in %(ids)s
                        and status_code is null
                    ) as skipped
                """,
                parameters={'ids': tuple(data['id'].tolist())}
            )

        return pd.DataFrame()

    def post_opt_ins(**context):
        tasks_to_run = context['dag_run'].conf.get('tasks_to_run', None)

        def post_data(data):
            data['store_number'] = data['store_number'].apply(lambda x: str(x).replace('.0', '') if not pd.isnull(x) else x)
            data['national_id'] = data['national_id'].apply(lambda x: str(x).replace('.0', '') if not pd.isnull(x) else x)
            data['primary_phone_no'] = data['primary_phone_no'].apply(lambda x: str(x).replace('.0', '') if not pd.isnull(x) else x)

            for index, row in data.iterrows():
                json_data = generate_request_payload(row)

                response = requests.post(
                    url="http://3.123.63.62/bloomAutomation/bloomapi/optIn.php",
                    data=json_data,
                    headers={'Content-type': 'application/json', 'Accept': 'text/plain'}
                )

                data.loc[index, 'status_code'] = response.status_code
                data.loc[index, 'raw_request'] = json_data
                data.loc[index, 'mifos_response'] = response.text

                # ADD LOGS
                log_format = "%(asctime)s: %(message)s"
                logging.basicConfig(format=log_format, level=logging.WARNING, datefmt="%H:%M:%S")

                try:
                    failure_reason = json.loads(response.text.split('\n')[-1])[
                        'responseMessage'] if response.status_code == 400 else None
                except IndexError:
                    failure_reason = json.loads(response.text)[
                        'responseMessage'] if response.status_code == 400 else None
                except json.decoder.JSONDecodeError:
                    failure_reason = json.loads(response.text)[
                        'responseMessage'] if response.status_code == 400 else None

                gateway_hook.run(
                    sql="""
                            update bloom_staging.opt_ins_data_dump
                            set processed_date = %(processed_date)s, raw_request = %(raw_request)s,
                            mifos_response = %(mifos_response)s, status_code = %(status_code)s,
                            failure_reason = %(failure_reason)s
                            where id = %(id)s
                        """,
                    parameters={
                        'processed_date': datetime.datetime.now(), 'raw_request': json_data,
                        'mifos_response': response.text, 'status_code': response.status_code,
                        'failure_reason': failure_reason,
                        'id': row['id']
                    }
                )

        def execute_task():
            data = gateway_hook.get_pandas_df(
                sql="""
                    select 
                        id, store_number, first_name, national_id, primary_phone_no, 
                        business_name, registration_date
                    from (
                        select *, rank() over (partition by store_number order by primary_phone_no, national_id ) rnk
                        from bloom_staging.opt_ins_data_dump oidd 
                        order by primary_phone_no, national_id
                    ) as rnked
                    where rnked.rnk = 1 and status_code is null and store_number != '-1'
                    and national_id not like '%None%'
                """
            )
            data.drop_duplicates(subset=['store_number'], inplace=True)

            data.reset_index(inplace=True)
            chunks = [data.iloc[data.index[i:i + 1]].copy() for i in range(0, data.shape[0])]

            with ThreadPoolExecutor(max_workers=10) as executor:
                results = executor.map(post_data, chunks)

            dag_id = context['dag_run'].dag_id
            task_id = context['task_instance'].task_id
            context['task_instance'].xcom_push(key=dag_id, value=True)

            # update warehouse
            sns0 = tuple((str(x) for x in warehouse_hook.get_pandas_df(
                sql="""
                    select mpsa_orga_shrt from bloomlive.client_activity_data_dump where (status_code is null or (failure_reason is null and status_code != 200)) and is_opt_in 
                """
            )['mpsa_orga_shrt'].drop_duplicates().tolist()))

            if len(sns0) > 0:
                sns = gateway_hook.get_pandas_df(
                    sql="""
                        WITH rnked as (
                            select warehouse_key, store_number, status_code, failure_reason, rank() over (partition by store_number order by coalesce(status_code, '9') asc) rnk
                            from bloom_staging.opt_ins_data_dump oidd where warehouse_key is not null
                        ) select store_number, status_code, failure_reason from rnked where rnk = 1 and store_number in %(sns)s
                    """,
                    parameters={
                        'sns': sns0
                    }
                ).drop_duplicates(subset=['store_number'])

                status_code_groups = sns.groupby('status_code')
                failure_reason_groups = sns.groupby('failure_reason')

                for i in status_code_groups.groups.keys():
                    grp = status_code_groups.get_group(i)

                    warehouse_hook.run(
                        sql="""update bloomlive.client_activity_data_dump set status_code = %(sc)s where mpsa_orga_shrt in %(sns)s""",
                        parameters={'sc': str(i), 'sns': tuple(grp['store_number'].dropna().tolist())}
                    )

                for i in failure_reason_groups.groups.keys():
                    grp = failure_reason_groups.get_group(i)

                    warehouse_hook.run(
                        sql="""update bloomlive.client_activity_data_dump set failure_reason = %(fr)s where mpsa_orga_shrt in %(sns)s""",
                        parameters={'fr': str(i), 'sns': tuple(grp['store_number'].dropna().tolist())}
                    )

        if tasks_to_run is not None:
            if context['task'].task_id in tasks_to_run:  # only specified tasks should run
                execute_task()
            else:
                context['ti'].xcom_push(key='is_missing', value=False)
        else:  # all tasks should run
            execute_task()


    def post_disbursements(**context):
        tasks_to_run = context['dag_run'].conf.get('tasks_to_run', None)

        def post_data(data):
            for index, row in data.iterrows():
                json_data = json.dumps({
                    'requestId': row['receipt_number'], 'loanNumber': row['receipt_number'],
                    'loanAmount': row['loan_principal'], 'storeNumber': row['store_number'],
                    'storeName': row['other_party_info'], 'product': f"Asante {row['loan_term']} day",
                    'creditLimitBalance': 0, 'loanDisbursementTransactionId': row['receipt_number'],
                    'applicationDateTime': str(row['initiation_time']), 'mobileNumber': row['store_number']
                })

                response = requests.post(
                    url="http://3.123.63.62/bloomAutomation/bloomapi/loanRequest.php",
                    data=json_data,
                    headers={'Content-type': 'application/json', 'Accept': 'text/plain'}
                )

                data.loc[index, 'status_code'] = response.status_code
                data.loc[index, 'raw_request'] = json_data
                data.loc[index, 'mifos_response'] = response.text

                # ADD LOGS
                log_format = "%(asctime)s: %(message)s"
                logging.basicConfig(format=log_format, level=logging.WARNING, datefmt="%H:%M:%S")

                try:
                    failure_reason = json.loads(response.text.split('\n')[-1])[
                        'responseMessage'] if response.status_code == 400 else None
                except IndexError:
                    failure_reason = json.loads(response.text)[
                        'responseMessage'] if response.status_code == 400 else None
                except json.decoder.JSONDecodeError:
                    failure_reason = json.loads(response.text)[
                        'responseMessage'] if response.status_code == 400 else None

                if response.status_code == 400 and 'Loan has already been posted' in failure_reason:
                    failure_reason = 'Loan has already been posted'

                gateway_hook.run(
                    sql="""
                            update bloom_staging.transactions_data_dump
                            set processed_date = %(processed_date)s, raw_request = %(raw_request)s,
                            mifos_response = %(mifos_response)s, 
                            status_code = %(status_code)s, 
                            failure_reason = %(failure_reason)s
                            where id = %(id)s
                        """,
                    parameters={
                        'processed_date': datetime.datetime.now(), 'raw_request': json_data,
                        'mifos_response': response.text, 'status_code': response.status_code,
                        'failure_reason': failure_reason,
                        'id': row['id']
                    }
                )

        def execute_task():
            data = gateway_hook.get_pandas_df(
                sql="""
                        select
                            id, receipt_number, initiation_time, store_number, loan_term,
                            loan_principal, other_party_info, status_code, record_created_on_timestamp
                        from (
                            select id, receipt_number, initiation_time, store_number, loan_term,
                            loan_principal, other_party_info, status_code, record_created_on_timestamp, rank() over (partition by receipt_number order by initiation_time, id asc) rnk
                            from bloom_staging.transactions_data_dump where is_disbursement = 1
                        ) as rnked where rnk = 1 and status_code is null and store_number != '-1'
                        order by initiation_time desc
                    """
            ).drop_duplicates(subset=['receipt_number'])
            # ).drop_duplicates(subset=['receipt_number', 'initiation_time', 'store_number', 'loan_term',
            #                 'loan_principal', 'other_party_info'])

            data['receipt_number'] = data['receipt_number'].apply(lambda x: x.strip() if not pd.isnull(x) else x)

            data.reset_index(inplace=True)
            chunks = [data.iloc[data.index[i:i + 1]].copy() for i in range(0, data.shape[0])]


            with ThreadPoolExecutor(max_workers=10) as executor:
                results = executor.map(post_data, chunks)

            dag_id = context['dag_run'].dag_id
            task_id = context['task_instance'].task_id
            context['task_instance'].xcom_push(key=dag_id, value=True)

            # update warehouse
            rns0 = tuple(warehouse_hook.get_pandas_df(
                sql="""
                    select id_trxn_linkd from bloomlive.transactions_data_dump where status_code is null or (failure_reason is null and status_code != 200) 
                """
            )['id_trxn_linkd'].drop_duplicates().tolist())
            logging.warning(len(rns0))

            if len(rns0) > 0:
                rns = gateway_hook.get_pandas_df(
                    sql="""
                        WITH rnked as (
                            select warehouse_key, receipt_number, status_code, failure_reason, rank() over (partition by receipt_number order by coalesce(status_code, '9') asc) rnk
                            from bloom_staging.transactions_data_dump tdd where warehouse_key is not null
                        ) select receipt_number, status_code, failure_reason from rnked where rnk = 1 and receipt_number in %(rns)s
                    """,
                    parameters={
                        'rns': rns0
                    }
                ).drop_duplicates(subset=['receipt_number'])

                chunk_size = 1000
                size = rns.shape[0]+1
                for num in range(0, size, chunk_size):
                    start_index = num if num == 0 else num + 1
                    end_index = num + chunk_size if num + chunk_size < size else size

                    status_code_groups = rns[start_index:end_index].groupby('status_code')
                    failure_reason_groups = rns[start_index:end_index].groupby('failure_reason')


                    for i in status_code_groups.groups.keys():
                        grp = status_code_groups.get_group(i)

                        warehouse_hook.run(
                            sql="""update bloomlive.transactions_data_dump set status_code = %(sc)s where id_trxn_linkd in %(refs)s and status_code is null""",
                            parameters={'sc': str(i), 'refs': tuple(grp['receipt_number'].dropna().tolist())}
                        )


                    for i in failure_reason_groups.groups.keys():
                        grp = failure_reason_groups.get_group(i)

                        warehouse_hook.run(
                            sql="""update bloomlive.transactions_data_dump set failure_reason = %(fr)s where id_trxn_linkd in %(refs)s and failure_reason is null""",
                            parameters={'fr': str(i), 'refs': tuple(grp['receipt_number'].dropna().tolist())}
                        )


        if tasks_to_run is not None:
            if context['task'].task_id in tasks_to_run:  # only specified tasks should run
                execute_task()
            else:
                context['ti'].xcom_push(key='is_missing', value=False)
        else:  # all tasks should run
            execute_task()

    def post_repayments(**context):
        tasks_to_run = context['dag_run'].conf.get('tasks_to_run', None)

        def post_data(data):
            for index, row in data.sort_values(by=['transaction_date_time'], ascending=True).iterrows():
                json_data = json.dumps({
                    "requestId": row['receipt_number'], "loanContractId": row['loan_ref'],
                    "loanTerm": row['loan_term'], "loanAmount": row['loan_amount'],
                    "loanRequestDate": str(row['loan_request_date']), "loanBalance": row['loan_balance'],
                    "storeNumber": row['store_number'], "senderName": row['sender_name'],
                    "recipientName": row['recipient_name'], "mpesaRef": row['mpesa_ref'],
                    "transactionDateTime": str(row['transaction_date_time']), "paidAmount": str(row['paid_amount']),
                    "notificationDescription": row['notification_description'], "lenderId": row['lender_id']
                })

                response = requests.post(
                    url="http://3.123.63.62/bloomAutomation/bloomapi/loanRepayment.php",
                    data=json_data,
                    headers={'Content-type': 'application/json', 'Accept': 'text/plain'}
                )

                data.loc[index, 'status_code'] = response.status_code
                data.loc[index, 'raw_request'] = json_data
                data.loc[index, 'mifos_response'] = response.text

                # ADD LOGS
                log_format = "%(asctime)s: %(message)s"
                logging.basicConfig(format=log_format, level=logging.WARNING, datefmt="%H:%M:%S")

                try:
                    failure_reason = json.loads(response.text.split('\n')[-1])['responseMessage'] if response.status_code == 400 else None
                except IndexError:
                    failure_reason = json.loads(response.text)['responseMessage'] if response.status_code == 400 else None
                except json.decoder.JSONDecodeError:
                    failure_reason = json.loads(response.text)[
                        'responseMessage'] if response.status_code == 400 else None

                if response.status_code == 400 and 'Duplicate Repayment' in failure_reason:
                    failure_reason = 'Duplicate Repayment'

                gateway_hook.run(
                    sql="""
                            update bloom_staging.transactions_data_dump
                            set processed_date = %(processed_date)s, raw_request = %(raw_request)s,
                            mifos_response = %(mifos_response)s,
                            status_code = %(status_code)s, failure_reason = %(failure_reason)s
                            where id = %(id)s
                        """,
                    parameters={
                        'processed_date': datetime.datetime.now(), 'raw_request': json_data,
                        'mifos_response': response.text, 'status_code': response.status_code,
                        'failure_reason': failure_reason,
                        'id': row['id']
                    }
                )

        def execute_task():

            data = gateway_hook.get_pandas_df(
                sql="""
                    select
                        id, receipt_number, loan_ref, loan_term,
                        null as loan_amount, paid_in, null as loan_request_date,
                        null as loan_balance, store_number,
                        null as sender_name, null as recipient_name,
                        receipt_number as mpesa_ref, initiation_time as transaction_date_time,
                        paid_in as paid_amount, loan_term as tenure, loan_principal as amount,
                        null as notification_description, null as lender_id
                    from bloom_staging.transactions_data_dump
                    where status_code is null AND is_repayment = 1 and store_number != '-1'
                    order by transaction_date_time desc
                    """
            )
            data.drop_duplicates(subset=['receipt_number'], inplace=True)
            # post_data(data)
            data.reset_index(inplace=True)
            chunks = [data.iloc[data.index[i:i + 1]].copy() for i in range(0, data.shape[0])]

            with ThreadPoolExecutor(max_workers=10) as executor:
                results = executor.map(post_data, chunks)

            dag_id = context['dag_run'].dag_id
            task_id = context['task_instance'].task_id
            context['task_instance'].xcom_push(key=dag_id, value=True)


        if tasks_to_run is not None:
            if context['task'].task_id in tasks_to_run:  # only specified tasks should run
                execute_task()
            else:
                context['ti'].xcom_push(key='is_missing', value=False)
        else:  # all tasks should run
            execute_task()

    def send_ms_teams_notification(**context):
        report = gateway_hook.get_pandas_df(
            sql="""
            SELECT
                transaction_date,
                total_successful_repayments,
                total_successful_disbursements,
                failure_type,
                failure_reason,
                total_failed_transactions
            FROM
            (
                SELECT
                    DATE(initiation_time) AS transaction_date,
                    COUNT(CASE WHEN is_repayment = 1 THEN 1 END) AS total_successful_repayments,
                    COUNT(CASE WHEN is_disbursement = 1 THEN 1 END) AS total_successful_disbursements
                FROM
                    bloom_staging.transactions_data_dump
                WHERE
                    status_code = 200
                GROUP BY
                    DATE(initiation_time)
            ) AS successful_transactions
            LEFT JOIN
            (
                SELECT
                    DATE(initiation_time) AS failed_transaction_date,
                    CASE WHEN is_repayment = 1 THEN 'Repayment' ELSE 'Disbursement' END AS failure_type,
                    failure_reason,
                    COUNT(failure_reason) AS total_failed_transactions
                FROM
                    bloom_staging.transactions_data_dump
                WHERE
                    status_code = 400 and receipt_number not in (
                        select receipt_number from bloom_staging.transactions_data_dump tdd where status_code = 200
                    )
                GROUP BY
                    DATE(initiation_time), failure_type, failure_reason
            ) AS failed_transactions
            ON
                successful_transactions.transaction_date = failed_transactions.failed_transaction_date
            where transaction_date >= DATE_SUB(current_date, INTERVAL 7 DAY)
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
                Data Dumps Recon {date}
                Successful Disbursements: {dt.iloc[0]['total_successful_disbursements'].astype(int)},
                Successful Repayments: {dt.iloc[0]['total_successful_repayments'].astype(int)},
                Failed Disbursements: {dt[dt['failure_type'] == 'Disbursement']['total_failed_transactions'].astype(int).sum()},
                Failed Repayments: {dt[dt['failure_type'] == 'Repayment']['total_failed_transactions'].astype(int).sum()}

                Disbursement Failure reasons
                {disb_fail_reasons}


                Repayment Failure reasons
                {rep_fail_reasons}
                
                {'-' * 50}

            """

        dag_id = context['dag_run'].dag_id
        task_id = context['task_instance'].task_id
        context['task_instance'].xcom_push(key=dag_id, value=True)

        logs_url = "https://airflow.asantefsg.com/data-pipelines/log?dag_id={}&task_id={}&execution_date={}".format(
            dag_id, task_id, parse.quote_plus(context['ts']))

        teams_notification = MSTeamsWebhookOperator(
            task_id="send_ms_summary_to_ms_teams",
            message=f"Bloom Data Dumps Recon: {context['dag_run'].conf.get('recon_type', '')}",
            subtitle=report_str,
            button_text="View log",
            button_url=logs_url,
            http_conn_id='msteams_webhook_url_safaricom_data_dumps_channel' if Variable.get(
                'DEBUG') == 'FALSE' else 'msteams_webhook_url',
        )
        teams_notification.execute(context)

    t1 = PythonOperator(
        task_id='post_opt_ins',
        provide_context=True,
        python_callable=post_opt_ins,
        templates_dict=common_params
    )
    t2 = PythonOperator(
        task_id='post_disbursements',
        provide_context=True,
        python_callable=post_disbursements,
        templates_dict=common_params
    )
    t3 = PythonOperator(
        task_id='post_repayments',
        provide_context=True,
        python_callable=post_repayments,
        templates_dict=common_params
    )
    t4 = PythonOperator(
        task_id='send_ms_teams_notification',
        provide_context=True,
        python_callable=send_ms_teams_notification,
    )

    t1 >> t2 >> t3 >> t4

