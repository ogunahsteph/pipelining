import os
import sys
import json
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
from utils.common import on_failure

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
gateway_hook = MySqlHook(mysql_conn_id='gateway_server', schema='bloom_staging')
mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-safaricom')
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
        'tep_data_bloom_recon',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        start_date=datetime.datetime(2022, 10, 5, 16, 00, tzinfo=local_tz),
        tags=['reconciliation'],
        description='reconcile tep data into mifos',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Prepare data that is on TEP that is missing on MIFOS for reposting

    #### 
    """

    def extract_TEP_raw_data(data):
        dicts = []

        def get_data(x):
            try:
                raw = json.loads(
                    x['raw_request'].replace('\n', '').replace('\r', '').replace('"{', '{').replace('}"', '}'))
                # resp = json.loads(
                #     x['response_message'].replace('\n', '').replace('\r', '').replace('"{', '{').replace('}"', '}'))[
                #     'responseMessage']
                raw['id'] = x['id']
                # raw['response_message'] = resp
                raw['status_code'] = x['status_code']
                raw['request_time'] = x['request_time']
                dicts.append(raw)
            except json.JSONDecodeError as e:
                logging.warning(f'{e}')
            except TypeError as e:
                # Skip this row and continue to the next one
                logging.warning(f'{raw}')

        data.apply(
            lambda x: get_data(x),
            axis=1
        )

        return pd.DataFrame(dicts)


    def recon_missing_TEP_disbursements(**context):
        ttl = gateway_hook.get_pandas_df(
            sql="""
                select count(*) as ttl from bloom_staging2.request_log where menu = 'raw_loan_request'
            """
        ).iloc[0]['ttl']

        chunk_size = 10000

        for num in range(0, ttl, chunk_size):
            tep_loans = gateway_hook.get_pandas_df(
                sql="""
                    select * from bloom_staging2.request_log where menu = 'raw_loan_request'
                    order by id desc limit %(start_index)s, %(chunk_size)s
                """,
                parameters={'start_index': num, 'chunk_size': chunk_size}
            )
            tep_loans = extract_TEP_raw_data(tep_loans)
            tep_loans['applicationDateTime'] = tep_loans['applicationDateTime'].apply(
                lambda x: datetime.datetime.strptime(str(x), '%Y%m%d%H%M%S') if not pd.isnull(x) else x
            )
            existing = mifos_hook.get_pandas_df(
                sql="""
                select mpd.receipt_number from `mifostenant-safaricom`.m_loan ml
                inner join `mifostenant-safaricom`.m_loan_transaction mlt on mlt.loan_id = ml.id
                inner join `mifostenant-safaricom`.m_payment_detail mpd on mpd.id = mlt.payment_detail_id
                where mlt.transaction_type_enum = 1 and mlt.is_reversed = 0 and mpd.receipt_number in %(rns)s
                """,
                parameters={'rns': tuple(tep_loans['fundMovementTransactionID'].dropna().tolist())}
            )
            tep_loans = tep_loans[~tep_loans['fundMovementTransactionID'].isin(existing['receipt_number'].tolist())]

            if tep_loans.shape[0] > 0:
                tep_loans = tep_loans.drop_duplicates(subset=['fundMovementTransactionID', 'status_code']).dropna(
                    subset=['fundMovementTransactionID'])
                tep_loans['product'] = tep_loans['product'].apply(
                    lambda x: str(x).split(' ')[1]
                )
                tep_loans['is_disbursement'] = True

                if tep_loans.shape[0] > 0:
                    already_reconed = gateway_hook.get_pandas_df(
                        sql="""
                            select receipt_number from bloom_staging.transactions_data_dump where is_disbursement = 1
                            and receipt_number in %(refs)s and src = 'TEP'
                        """,
                        parameters={'refs': tuple(tep_loans['fundMovementTransactionID'].tolist())}
                    )

                    tep_loans = tep_loans[~tep_loans['fundMovementTransactionID'].isin(already_reconed['receipt_number'].tolist())]
                    tep_loans['src'] = 'TEP'

                    gateway_hook.insert_rows(
                        table='bloom_staging.transactions_data_dump',
                        target_fields=[
                            'receipt_number', 'initiation_time',
                            'loan_principal', 'other_party_info',
                            'store_number', 'loan_ref', 'loan_date', 'loan_term', 'is_disbursement', 'src'
                        ],
                        rows=tep_loans[[
                            'fundMovementTransactionID', 'applicationDateTime',
                            'loanAmount', 'storeName',
                            'storeNumber', 'fundMovementTransactionID', 'applicationDateTime', 'product', 'is_disbursement', 'src'
                        ]].itertuples(index=False, name=None),
                        commit_every=0
                    )

    def recon_missing_TEP_repayments(**context):
        ttl = gateway_hook.get_pandas_df(
            sql="""
                select count(*) as ttl from bloom_staging2.request_log where menu = 'raw_loan_repayment'
            """
        ).iloc[0]['ttl']

        chunk_size = 10000

        for num in range(0, ttl, chunk_size):
            tep_repayments = gateway_hook.get_pandas_df(
                sql="""
                    select * from bloom_staging2.request_log where menu = 'raw_loan_repayment'
                    order by id desc limit %(start_index)s, %(chunk_size)s
                """,
                parameters={'start_index': num, 'chunk_size': chunk_size}
            )

            tep_repayments = extract_TEP_raw_data(tep_repayments)
            tep_repayments.drop_duplicates(subset=['requestId'], inplace=True)
            tep_repayments['transactionDateTime'] = tep_repayments['transactionDateTime'].apply(
                lambda x: datetime.datetime.strptime(str(x), '%Y%m%d%H%M%S') if not pd.isnull(x) else x
            )
            existing = mifos_hook.get_pandas_df(
                sql="""
                    select mpd.receipt_number from `mifostenant-safaricom`.m_loan ml
                    inner join `mifostenant-safaricom`.m_loan_transaction mlt on mlt.loan_id = ml.id
                    inner join `mifostenant-safaricom`.m_payment_detail mpd on mpd.id = mlt.payment_detail_id
                    where mlt.transaction_type_enum = 2 and mlt.is_reversed = 0 and mpd.receipt_number in %(rns)s
                """,
                parameters={'rns': tuple(tep_repayments['fundMovementTransactionID'].dropna().tolist())}
            )
            tep_repayments = tep_repayments[
                ~tep_repayments['fundMovementTransactionID'].isin(existing['receipt_number'].tolist())]
            tep_repayments = tep_repayments.drop_duplicates(subset=['fundMovementTransactionID', 'status_code']).dropna(
                subset=['fundMovementTransactionID'])

            if tep_repayments.shape[0] > 0:
                already_reconed = gateway_hook.get_pandas_df(
                    sql="""
                        select receipt_number from bloom_staging.transactions_data_dump where is_repayment = 1
                        and receipt_number in %(refs)s and src = 'TEP'
                    """,
                    parameters={'refs': tuple(tep_repayments['fundMovementTransactionID'].tolist())}
                )

                tep_repayments = tep_repayments[~tep_repayments['fundMovementTransactionID'].isin(already_reconed['receipt_number'].tolist())]

                if tep_repayments.shape[0] > 0:
                    lns = warehouse_hook.get_pandas_df(
                        sql="""select cntrct_id, receipt_number as id_trxn_linkd from bloomlive.loans_fact_table_materialized_summary_view where cntrct_id in %(cntrcts)s""",
                        parameters={'cntrcts': tuple(tep_repayments.dropna(subset=['loanContractId'])['loanContractId'].tolist())}
                    )
                    tep_repayments = tep_repayments.merge(
                        lns,
                        left_on='loanContractId',
                        right_on='cntrct_id',
                        how='left'
                    ).dropna(subset=['id_trxn_linkd'])
                    tep_repayments['is_repayment'] = True
                    tep_repayments['record_created_on_timestamp'] = datetime.datetime.now()
                    tep_repayments['src'] = 'TEP'
                    gateway_hook.insert_rows(
                        table='bloom_staging.transactions_data_dump',
                        target_fields=[
                            'loan_ref', 'initiation_time',
                            'paid_in', 'other_party_info',
                            'store_number', 'receipt_number', 'is_repayment', 'record_created_on_timestamp', 'src'
                        ],
                        rows=tep_repayments[[
                            'id_trxn_linkd', 'transactionDateTime',
                            'paidAmount', 'senderName', 'storeNumber', 'fundMovementTransactionID',
                            'is_repayment', 'record_created_on_timestamp', 'src'
                        ]].replace({np.NAN: None}).itertuples(index=False, name=None),
                        commit_every=1000
                    )

    def recon_missing_TEP_opt_ins(**context):
        ttl = gateway_hook.get_pandas_df(
            sql="""
                select count(*) as ttl from bloom_staging2.request_log where menu = 'raw_opt_in'
            """
        ).iloc[0]['ttl']

        chunk_size = 10000

        for num in range(0, ttl, chunk_size):

            tep_opt_ins = gateway_hook.get_pandas_df(
                sql="""
                    select * from bloom_staging2.request_log where menu = 'raw_opt_in'
                    order by id desc limit %(start_index)s, %(chunk_size)s
                """,
                parameters={'start_index': num, 'chunk_size': chunk_size}
            )

            tep_opt_ins = extract_TEP_raw_data(tep_opt_ins)
            tep_opt_ins.drop_duplicates(subset=['requestId'], inplace=True)
            tep_opt_ins['optinDateTime'] = tep_opt_ins['optinDateTime'].apply(
                lambda x: datetime.datetime.strptime(str(x), '%Y%m%d%H%M%S') if not pd.isnull(x) else x
            )
            existing = mifos_hook.get_pandas_df(
                sql="""
                    select middlename from `mifostenant-safaricom`.m_client where middlename in %(mn)s
                """,
                parameters={'mn': tuple(tep_opt_ins['storeNumber'].dropna().tolist())}
            )
            tep_opt_ins = tep_opt_ins[~tep_opt_ins['storeNumber'].isin(existing['middlename'].tolist())]
            tep_opt_ins = tep_opt_ins.drop_duplicates(subset=['storeNumber', 'status_code']).dropna(
                subset=['storeNumber'])

            if tep_opt_ins.shape[0] > 0:
                already_reconed = gateway_hook.get_pandas_df(
                    sql="""
                        select store_number from bloom_staging.opt_ins_data_dump 
                        where store_number in %(sns)s
                    """,
                    parameters={'sns': tuple(tep_opt_ins['storeNumber'].tolist())}
                )

                tep_opt_ins = tep_opt_ins[~tep_opt_ins['storeNumber'].isin(already_reconed['store_number'].tolist())]
                tep_opt_ins['src'] = 'TEP'
                if tep_opt_ins.shape[0] > 0:
                    tep_opt_ins['record_created_on_timestamp'] = datetime.datetime.now()
                    tep_opt_ins.drop_duplicates(subset=['requestId'])
                    gateway_hook.insert_rows(
                        table='bloom_staging.opt_ins_data_dump',
                        target_fields=[
                            'store_number', 'first_name', 'business_name', 'national_id',
                            'primary_phone_no', 'registration_date', 'record_created_on_timestamp', 'src'
                        ],
                        rows=tep_opt_ins[[
                            'storeNumber', 'customerName', 'businessName', 'customerIdentityId',
                            'mobileNumber', 'optinDateTime', 'record_created_on_timestamp', 'src'
                        ]].replace({np.NAN: None}).itertuples(index=False, name=None),
                        commit_every=0
                    )

    def trigger_ETL_safaricom_recons_mifos_posting(**context):
        """
        Trigger posting of data to mifos
        """
        from utils.common import trigger_dag_remotely

        # Triggering the DAG remotely
        trigger_dag_remotely(dag_id='ETL_safaricom_recons_mifos_posting', conf={})


    t1 = PythonOperator(
        task_id='recon_missing_TEP_opt_ins',
        provide_context=True,
        python_callable=recon_missing_TEP_opt_ins,
    )
    t2 = PythonOperator(
        task_id='recon_missing_TEP_disbursements',
        provide_context=True,
        python_callable=recon_missing_TEP_disbursements,
    )
    t3 = PythonOperator(
        task_id='recon_missing_TEP_repayments',
        provide_context=True,
        python_callable=recon_missing_TEP_repayments,
    )
    t4 = PythonOperator(
        task_id='trigger_ETL_safaricom_recons_mifos_posting',
        provide_context=True,
        python_callable=trigger_ETL_safaricom_recons_mifos_posting,
    )

    t1 >> t2 >> t3 >> t4
