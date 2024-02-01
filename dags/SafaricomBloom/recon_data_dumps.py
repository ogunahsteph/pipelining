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
        'data_dumps_bloom_data_recon',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        start_date=datetime.datetime(2023, 8, 12, 12, 00, tzinfo=local_tz),
        tags=['reconciliation'],
        description='reconcile safaricom data dumps transactions and opt ins data into mifos',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Prepare data that is on Mpesa transactions that is missing on MIFOS for reposting

    #### 
    """

    def fix_duplicate_opt_ins(df: pd.DataFrame, hook: object) -> pd.DataFrame:
        """
        Removes already existing store numbers from new opt in data if it exists
        Appends to mobile numbers and/or national ids if they already exist in mifos

        :param df: a dataframe containing new opt in data
        :param hook: a mysql connection object to either mifostenant-bloom2 or mifostenant-safaricom

        :return: a modified pandas dataframe or None
        """

        # remove whitespaces
        df['national_id'] = df['national_id'].apply(
            lambda x: str(x).strip().replace('.0', '') if not pd.isnull(x) else x)
        df['primary_phone_no'] = df['primary_phone_no'].apply(
            lambda x: str(x).strip().replace('.0', '') if not pd.isnull(x) else x)

        # Identify and rank customers who have opted in twice on the same day according to the order of opt ins
        df['mobile_rnk'] = df.sort_values(by=['cust_idnty_id'], ascending=True).groupby(
            ['primary_phone_no', 'registration_date']).cumcount() + 1
        df['id_rnk'] = df.sort_values(by=['cust_idnty_id'], ascending=True).groupby(
            ['national_id', 'registration_date']).cumcount() + 1

        # append registration dates and ranks to mobile numbers and/or national ids
        df['national_id'] = df.apply(
            lambda x: f"{x['national_id']}_{x['registration_date']}_{x['id_rnk']}" if x['id_rnk'] > 1 else x[
                'national_id'],
            axis=1
        )
        df['primary_phone_no'] = df.apply(
            lambda x: f"{x['primary_phone_no']}_{x['registration_date']}_{x['mobile_rnk']}" if x['mobile_rnk'] > 1 else
            x[
                'primary_phone_no'],
            axis=1
        )

        # remove unused columns
        df.drop(columns=['cust_idnty_id', 'mobile_rnk', 'id_rnk'], inplace=True)

        # Identify store numbers in opt in data that are already registered in MIFOS
        existing_store_numbers = hook.get_pandas_df(
            sql="""
                   select trim(middlename) as store_number from m_client
                   where trim(middlename) in %(store_numbers)s
               """,
            parameters={'store_numbers': tuple(df['store_number'].tolist())}
        )['store_number'].tolist()

        # Remove store numbers that are already registered on MIFOS
        df = df[~df['store_number'].isin(existing_store_numbers)]

        # Identify national ids in opt in data that are already registered on MIFOS
        existing_clients = []
        if df['national_id'].dropna().size > 0:
            existing_clients.append(hook.get_pandas_df(
                sql="""
                       select trim(external_id) as external_id, trim(mobile_no) as mobile_no from m_client
                       where trim(external_id) in %(external_ids)s
                   """,
                parameters={
                    'external_ids': tuple([str(x).strip() for x in df['national_id'].dropna().tolist()])
                }
            ))

        # Identify mobile numbers in opt in data that are already registered on MIFOS
        if df['primary_phone_no'].dropna().size > 0:
            existing_clients.append(hook.get_pandas_df(
                sql="""
                       select trim(external_id) as external_id, trim(mobile_no) as mobile_no from m_client
                       where trim(mobile_no) in %(mobile_nos)s
                   """,
                parameters={
                    'mobile_nos': tuple([str(x).strip() for x in df['primary_phone_no'].dropna().tolist()])
                }
            ))

        existing_clients = pd.concat(existing_clients).drop_duplicates() if len(existing_clients) > 0 else pd.DataFrame(
            {'external_id': [], 'mobile_no': []})

        # append opt in registration dates to mobile numbers and/or national ids identified above
        if existing_clients.shape[0] > 0:
            df['national_id'] = df.apply(
                lambda x: f"{x['national_id']}_{x['registration_date']}" if x['national_id'] in existing_clients[
                    'external_id'].tolist() else x['national_id'],
                axis=1
            )
            df['primary_phone_no'] = df.apply(
                lambda x: f"{x['primary_phone_no']}_{x['registration_date']}" if x['primary_phone_no'] in
                                                                                 existing_clients[
                                                                                     'mobile_no'].tolist() else x[
                    'primary_phone_no'],
                axis=1
            )

        return df

    def replace_special_characters(string: str) -> str:
        """
        removes special characters from a string
        param string: the text to be cleaned up
        """
        special_characters = ['½', '¿', '~', '�', 'ï', 'Â', 'Ã¯']

        for char in special_characters:
            string = string.replace(char, '')
        return string


    def share_disbursements(**context):
        # Retrieve the total number of disbursements from the warehouse
        total_disbursements = warehouse_hook.get_pandas_df(
            sql="""
                select count(*) as total from bloomlive.transactions_data_dump where is_disbursement
                and trxn_stts = 'Completed'
            """
        )['total'].iloc[0]

        # Define the chunk size for retrieving disbursements data in batches
        chunk_size = 2500

        # Retrieve disbursements data in batches
        for num in range(0, total_disbursements, chunk_size):
            disbursements = warehouse_hook.get_pandas_df(
                sql="""
                    with rnked as (
                     select cntrct_id, src_id_idnty, src_maintnanc_fee as maintnanc_fee, src_acces_fee, src_prncpl_amnt, src_id_prdct,
                        rank() over (partition by cntrct_id, src_id_idnty order by id_date desc, surrogate_id desc) as rnk
                       from bloomlive.contract_details_data_dump cddd
                    ) 
                    select 
                        tdd.surrogate_id, tdd.id_date, tdd.id_trxn_linkd, rnked.src_prncpl_amnt, 
                        tdd.cd_mpsa_orga_shrt, tdd.ds_mpsa_enty_name, rnked.src_id_prdct,
                        rnked.cntrct_id, rnked.src_id_idnty, rnked.maintnanc_fee, rnked.src_acces_fee
                    from bloomlive.transactions_data_dump tdd
                    left join rnked on tdd.id_loan_cntrct = rnked.cntrct_id and (tdd.trxn_amnt = rnked.src_prncpl_amnt or tdd.trxn_amnt - tdd.maintnanc_fee = rnked.src_prncpl_amnt)
                    where tdd.is_disbursement and tdd.trxn_stts = 'Completed' and rnked.src_prncpl_amnt is not null
                    order by tdd.surrogate_id desc offset %(start_index)s limit %(chunksize)s
                """,
                parameters={'start_index': num, 'chunksize': chunk_size}
            ).drop_duplicates(subset=['id_date', 'id_trxn_linkd', 'src_prncpl_amnt', 'cd_mpsa_orga_shrt',
                                      'ds_mpsa_enty_name', 'src_id_prdct', 'cntrct_id', 'src_id_idnty',
                                      'maintnanc_fee', 'src_acces_fee'])

            # Remove already existing disbursements from the retrieved data
            if disbursements.shape[0] > 0:
                already_existing = tuple([int(x) for x in gateway_hook.get_pandas_df(
                    sql="""
                        select warehouse_key from bloom_staging.transactions_data_dump where warehouse_key in %(surrogate_ids)s
                        and src = 'daily_data_dumps'
                    """,
                    parameters={'surrogate_ids': tuple([int(x) for x in disbursements['surrogate_id'].tolist()]) if
                    disbursements.shape[0] > 1 else f"({disbursements['surrogate_id'].iloc[0]})"}
                )['warehouse_key'].tolist()])

                disbursements.drop(
                    index=disbursements[disbursements['surrogate_id'].isin(already_existing)].index,
                    inplace=True)

                if disbursements.shape[0] > 0:
                    # Rename columns to match the database schema.
                    disbursements.rename(columns={
                        'cd_mpsa_orga_shrt': 'store_number', 'id_trxn_linkd': 'receipt_number',
                        'id_date': 'initiation_time', 'src_prncpl_amnt': 'loan_principal',
                    }, inplace=True)

                    # Add a new column to contain other party information.
                    disbursements['other_party_info'] = disbursements.apply(
                        lambda x: replace_special_characters(f"{x['store_number']} - {x['ds_mpsa_enty_name']}"),
                        axis=1
                    )

                    # Define a function to determine the loan term based on the source ID product.
                    def get_loan_term(x):
                        if x == '10001':
                            return 1
                        elif x == '10002':
                            return 7
                        elif x == '10003':
                            return 21

                    # Add a new column for the loan term.
                    disbursements['loan_term'] = disbursements['src_id_prdct'].apply(
                        lambda x: get_loan_term(x)
                    )

                    # Add a new column for the record created timestamp.
                    disbursements['record_created_on_timestamp'] = datetime.datetime.now()

                    # Add a new column to indicate that this is a disbursement record.
                    disbursements['is_disbursement'] = 1

                    # Replace NaN values with None to avoid issues with database insertion.
                    disbursements.replace({np.NAN: None}, inplace=True)
                    disbursements['record_created_on_timestamp'] = datetime.datetime.now()
                    disbursements['src'] = 'daily_data_dumps'

                    # Insert the processed data into the database.
                    gateway_hook.insert_rows(
                        table='bloom_staging.transactions_data_dump',
                        target_fields=[
                            'warehouse_key', 'receipt_number', 'initiation_time',
                            'loan_principal', 'other_party_info',
                            'store_number', 'loan_ref', 'loan_date', 'loan_term', 'is_disbursement', 'src', 'record_created_on_timestamp'
                        ],
                        rows=disbursements[[
                            'surrogate_id', 'receipt_number', 'initiation_time',
                            'loan_principal', 'other_party_info',
                            'store_number', 'receipt_number', 'initiation_time', 'loan_term', 'is_disbursement', 'src', 'record_created_on_timestamp'
                        ]].itertuples(index=False, name=None),
                        commit_every=0
                    )


    def share_repayments(**context):
        # Fetch the total number of repayments from the database
        total_repayments = warehouse_hook.get_pandas_df(
            sql="""
                with repayments_ranked as
                (
                    select *, rank() over (partition by id_trxn, id_trxn_linkd order by surrogate_id) count_rank
                    from bloomlive.transactions_data_dump tdd
                    where is_repayment is true and trxn_stts = 'Completed'
                )
                select
                   count(*) as total
                from repayments_ranked where count_rank = 1
            """
        )['total'].iloc[0]

        # Process repayments and disbursements data in batches
        chunk_size = 2500

        for num in range(0, total_repayments, chunk_size):
            # Fetch a batch of repayments data from the database
            repayments = warehouse_hook.get_pandas_df(
                sql="""
                with repayments_ranked as
                (
                    select *, rank() over (partition by id_trxn, id_trxn_linkd order by surrogate_id) count_rank
                    from bloomlive.transactions_data_dump tdd
                    where is_repayment is true and trxn_stts = 'Completed'
                )
                select
                   surrogate_id, id_date, id_trxn, dt_trxn_end, ds_mpsa_enty_name,
                   cd_mpsa_orga_shrt, trxn_type, trxn_amnt, maintnanc_fee,
                   trxn_stts, faild_reasn, id_trxn_linkd, id_idnty, src_lendr,
                   id_loan_cntrct
                from repayments_ranked where count_rank = 1
                order by surrogate_id desc offset %(start_index)s limit %(chunksize)s
                """,
                parameters={'start_index': num, 'chunksize': chunk_size}
            )

            # Fetch the list of existing repayments from the gateway table
            already_existing = gateway_hook.get_pandas_df(
                sql="""
                    select warehouse_key from bloom_staging.transactions_data_dump where warehouse_key in %(surrogate_ids)s
                    and src = 'daily_data_dumps'
                """,
                parameters={'surrogate_ids': tuple([int(x) for x in repayments['surrogate_id'].tolist()])}
            )

            # Drop the already existing repayments from the batch
            repayments.drop(
                index=repayments[repayments['surrogate_id'].isin(already_existing['warehouse_key'].tolist())].index,
                inplace=True)

            if repayments.shape[0] > 0:
                disbursements = warehouse_hook.get_pandas_df(
                    sql="""
                            with rnked as (
                             select cntrct_id, src_id_idnty, src_maintnanc_fee as maintnanc_fee, src_acces_fee, src_prncpl_amnt, src_id_prdct,
                                rank() over (partition by cntrct_id, src_id_idnty order by id_date desc, surrogate_id desc) as rnk
                               from bloomlive.contract_details_data_dump cddd
                            ) 
                            select 
                                tdd.surrogate_id, tdd.id_date, tdd.id_trxn_linkd, rnked.src_prncpl_amnt, 
                                tdd.cd_mpsa_orga_shrt, tdd.ds_mpsa_enty_name, rnked.src_id_prdct,
                                rnked.cntrct_id, rnked.src_id_idnty, rnked.maintnanc_fee, rnked.src_acces_fee
                            from bloomlive.transactions_data_dump tdd
                            left join rnked on tdd.id_loan_cntrct = rnked.cntrct_id and tdd.id_idnty = rnked.src_id_idnty and (tdd.trxn_amnt = rnked.src_prncpl_amnt or tdd.trxn_amnt - tdd.maintnanc_fee = rnked.src_prncpl_amnt)
                            where rnked.cntrct_id in %(cntrct_ids)s and tdd.is_disbursement is true and rnked.src_prncpl_amnt is not null
                        """,
                    parameters={'cntrct_ids': tuple(repayments['id_loan_cntrct'].dropna().tolist())}
                ).drop_duplicates(subset=['id_date', 'id_trxn_linkd', 'src_prncpl_amnt', 'cd_mpsa_orga_shrt',
                                          'ds_mpsa_enty_name', 'src_id_prdct', 'cntrct_id', 'src_id_idnty',
                                          'maintnanc_fee', 'src_acces_fee'])

                # Strip whitespace from id_trxn_linkd in both the repayments and disbursements dataframes
                repayments['id_trxn_linkd'] = repayments['id_trxn_linkd'].apply(
                    lambda x: x.strip() if not pd.isnull(x) else x)
                disbursements['id_trxn_linkd'] = disbursements['id_trxn_linkd'].apply(
                    lambda x: x.strip() if not pd.isnull(x) else x)

                # Merge the repayments and disbursements dataframes on contract ID and source ID identity
                repayments = repayments.merge(
                    disbursements,
                    left_on=['id_loan_cntrct', 'id_idnty'],
                    right_on=['cntrct_id', 'src_id_idnty'],
                    how='left'
                )

                # Rename some columns for clarity
                repayments.rename(
                    columns={'id_date_x': 'initiation_time', 'trxn_amnt': 'paid_in',
                             'cd_mpsa_orga_shrt_x': 'store_number',
                             'id_trxn_linkd_x': 'receipt_number', 'id_trxn_linkd_y': 'loan_ref'}, inplace=True)

                repayments['other_party_info'] = repayments.apply(
                    lambda x: replace_special_characters(f"{x['store_number']} - {x['ds_mpsa_enty_name_x']}"),
                    axis=1
                )

                repayments['is_repayment'] = True
                repayments['src'] = 'daily_data_dumps'
                repayments['record_created_on_timestamp'] = datetime.datetime.now()
                repayments.drop_duplicates(subset=['surrogate_id_x'], inplace=True)

                gateway_hook.insert_rows(
                    table='bloom_staging.transactions_data_dump',
                    target_fields=[
                        'warehouse_key', 'loan_ref', 'initiation_time',
                        'paid_in', 'other_party_info',
                        'store_number', 'receipt_number', 'is_repayment', 'record_created_on_timestamp', 'src'
                    ],
                    rows=repayments[[
                        'surrogate_id_x', 'loan_ref', 'initiation_time',
                        'paid_in', 'other_party_info', 'store_number', 'receipt_number',
                        'is_repayment', 'record_created_on_timestamp', 'src'
                    ]].replace({np.NAN: None}).itertuples(index=False, name=None),
                    commit_every=0
            )

    def share_opt_ins(**context):
        # Get existing opt-ins data from the data warehouse
        total_opt_ins = warehouse_hook.get_pandas_df(
            sql="""
                select count(*) as ttl from bloomlive.client_activity_data_dump cadd where opt_in_date is not null"""
        )['ttl'].iloc[0]

        chunk_size = 2500

        for num in range(0, total_opt_ins, chunk_size):
            opt_ins = warehouse_hook.get_pandas_df(
                sql=f"""
                    with rnked as
                    (
                        select surrogate_id, ds_cst_name, enty_name, mpsa_orga_shrt, cust_id_nmbr, mpsa_enty_phne, opt_in_date, cust_idnty_id,
                        rank() over (partition by mpsa_orga_shrt order by opt_in_date desc, surrogate_id desc) rnk
                        from bloomlive.client_activity_data_dump cadd
                        where opt_in_date is not null and mpsa_orga_shrt is not null and trim(mpsa_orga_shrt) != '-1' and ds_cst_name is not null
                        and (cust_id_nmbr is not null or cust_idnty_id is not null)
                        limit {chunk_size} offset {num}
                    )
                    select 
                        surrogate_id, mpsa_orga_shrt as store_number,
                        trim(enty_name) as first_name,
                        trim(ds_cst_name) as business_name,
                        case
                            when cust_id_nmbr is null then trim(cust_idnty_id)
                            else cust_id_nmbr
                        end as national_id,
                        mpsa_enty_phne as primary_phone_no,
                        opt_in_date as registration_date,
                        cust_idnty_id
                    from rnked where rnk = 1
                """
            )

            # Clean up the data, removing special characters and converting empty values to None
            if opt_ins.shape[0] > 1:
                already_existing = gateway_hook.get_pandas_df(
                    sql="""
                        select warehouse_key from bloom_staging.opt_ins_data_dump where src = 'daily_data_dumps'
                        and warehouse_key in %(wks)s
                    """,
                    parameters={"wks": tuple(opt_ins['surrogate_id'].tolist())}
                )
                opt_ins = opt_ins[~opt_ins['surrogate_id'].isin(already_existing['warehouse_key'].tolist())]

                # Apply replace_special_characters function to business_name and first_name columns
                opt_ins['business_name'] = opt_ins['business_name'].apply(
                    lambda x: replace_special_characters(x) if not pd.isnull(x) else x
                )
                opt_ins['first_name'] = opt_ins['first_name'].apply(
                    lambda x: replace_special_characters(x) if not pd.isnull(x) else x
                )

                # Add current timestamp to record_created_on_timestamp column
                opt_ins['record_created_on_timestamp'] = datetime.datetime.now()

                # Remove duplicates from opt_ins DataFrame
                opt_ins = fix_duplicate_opt_ins(opt_ins.copy(), hook=mifos_hook)

                # Remove decimal point from primary_phone_no column
                opt_ins['primary_phone_no'] = opt_ins['primary_phone_no'].apply(
                    lambda x: x.replace('.0', '') if not pd.isnull(x) else x
                )

                opt_ins['src'] = 'daily_data_dumps'

                # Insert transformed data into database table
                gateway_hook.insert_rows(
                    table='bloom_staging.opt_ins_data_dump',
                    target_fields=[
                        'warehouse_key', 'store_number', 'first_name', 'business_name', 'national_id',
                        'primary_phone_no', 'registration_date', 'record_created_on_timestamp', 'src'
                    ],
                    rows=opt_ins[[
                        'surrogate_id', 'store_number', 'first_name', 'business_name', 'national_id',
                        'primary_phone_no', 'registration_date', 'record_created_on_timestamp', 'src'
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
        task_id='share_disbursements',
        provide_context=True,
        python_callable=share_disbursements,
    )
    t2 = PythonOperator(
        task_id='share_repayments',
        provide_context=True,
        python_callable=share_repayments,
    )
    t3 = PythonOperator(
        task_id='share_opt_ins',
        provide_context=True,
        python_callable=share_opt_ins,
    )
    t4 = PythonOperator(
        task_id='trigger_ETL_safaricom_recons_mifos_posting',
        provide_context=True,
        python_callable=trigger_ETL_safaricom_recons_mifos_posting,
    )
    t1 >> t2 >> t3 >> t4
