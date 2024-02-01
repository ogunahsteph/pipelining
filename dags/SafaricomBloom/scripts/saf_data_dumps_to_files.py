import sys
import os
import datetime
import pandas as pd
from io import StringIO
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema='mifostenant-safaricom')
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from utils.common import get_saf_disbursements, saf_dumps_remove_trailing_zeros
from utils.office365_api import upload_file

def upload_opt_ins_ms_teams(file_date):
    # remove duplicated client activity
    warehouse_hook.run(
        sql="""
            delete from bloomlive.client_activity_data_dump where
            surrogate_id in (
                with client_activity_ranked as
                (
                    select *, rank() over (partition by cust_id_nmbr, cust_idnty_id, enty_name, opt_in_date, opt_out_date, mpsa_orga_shrt, mpsa_enty_phne, ds_cst_name order by record_created_on_timestamp desc) id_rank
                    from bloomlive.client_activity_data_dump
                ) select surrogate_id from client_activity_ranked where id_rank > 1
            )
        """
    )
    site = {
        'name': 'Data Dumps',
        'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be',
        'description': 'daily safaricom data dumps',
        'folder': '01BJPCNIBETLWC6I3GTBA2FZI34LFRIEU5'
    }

    # upload data to ms_teams
    data = warehouse_hook.get_pandas_df(
        sql="""
            with opt_ins_ranked as
            (
                select *, rank() over (partition by cust_idnty_id, opt_in_date order by surrogate_id) count_rank
                from bloomlive.client_activity_data_dump cadd
                where opt_in_date is not null 
            )
            select
                cust_id_nmbr,
                cust_idnty_id,
                enty_name,
                opt_in_date,
                mpsa_orga_shrt,
                mpsa_enty_phne,
                ds_cst_name
            from opt_ins_ranked where count_rank = 1 and date(opt_in_date) = %(file_date)s
        """,
        parameters={
            'file_date': file_date
        }
    )

    csv_buffer = StringIO()
    data.to_csv(index=False, path_or_buf=csv_buffer)
    upload_file(file_name=f'opt_ins_{str(file_date)}.csv', file_bytes=csv_buffer.getvalue().encode('utf-8'), site=site)

    upload_all_opt_ins_ms_teams()

def upload_opt_outs_ms_teams(file_date):
    # remove duplicated client activity
    warehouse_hook.run(
        sql="""
            delete from bloomlive.client_activity_data_dump where
            surrogate_id in (
                with client_activity_ranked as
                (
                    select *, rank() over (partition by cust_id_nmbr, cust_idnty_id, enty_name, opt_in_date, opt_out_date, mpsa_orga_shrt, mpsa_enty_phne, ds_cst_name order by record_created_on_timestamp desc) id_rank
                    from bloomlive.client_activity_data_dump
                ) select surrogate_id from client_activity_ranked where id_rank > 1
            )
        """
    )
    site = {
        'name': 'Data Dumps',
        'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be',
        'description': 'daily safaricom data dumps',
        'folder': '01BJPCNIBETLWC6I3GTBA2FZI34LFRIEU5'
    }

    data = warehouse_hook.get_pandas_df(
        sql="""
            with opt_outs_ranked as
            (
                select *, rank() over (partition by cust_idnty_id, opt_out_date order by surrogate_id) count_rank
                from bloomlive.client_activity_data_dump cadd
                where opt_out_date is not null -- and (mpsa_orga_shrt is not null and mpsa_enty_phne is not null)
            )
            select
                cust_id_nmbr,
                cust_idnty_id,
                enty_name,
                opt_out_date
            from opt_outs_ranked where count_rank = 1 and date(opt_out_date) = %(file_date)s
        """,
        parameters={
            'file_date': file_date
        }
    )

    csv_buffer = StringIO()
    data.to_csv(index=False, path_or_buf=csv_buffer)
    upload_file(file_name=f'opt_outs_{str(file_date)}.csv', file_bytes=csv_buffer.getvalue().encode('utf-8'), site=site)

def upload_repayments_ms_teams(file_date):
    # remove duplicated repayments
    warehouse_hook.run(
        sql="""
            delete from bloomlive.transactions_data_dump where
            surrogate_id in (
                with trxns_ranked as
                (
                    select *, rank() over (partition by id_date, id_trxn, id_loan_cntrct, dt_trxn_end,
                    ds_mpsa_enty_name, cd_mpsa_orga_shrt, trxn_type, trxn_amnt, maintnanc_fee, trxn_stts, faild_reasn,
                    id_trxn_linkd, id_idnty, src_lendr order by record_created_on_timestamp desc) id_rank
                    from bloomlive.transactions_data_dump cddd2 where is_repayment is true
                ) select surrogate_id from trxns_ranked where id_rank > 1
            )
        """
    )
    site = {
        'name': 'Data Dumps',
        'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be',
        'description': 'daily safaricom data dumps',
        'folder': '01BJPCNIBETLWC6I3GTBA2FZI34LFRIEU5'
    }

    data = warehouse_hook.get_pandas_df(
        sql="""
            with repayments_ranked as
            (
                select *, rank() over (partition by id_trxn, id_trxn_linkd order by surrogate_id) count_rank
                from bloomlive.transactions_data_dump tdd
                where is_repayment is true
            )
            select
               id_date, id_trxn, dt_trxn_end, ds_mpsa_enty_name,
               cd_mpsa_orga_shrt, trxn_type, trxn_amnt, maintnanc_fee,
               trxn_stts, faild_reasn, id_trxn_linkd, id_idnty, src_lendr,
               id_loan_cntrct
            from repayments_ranked where count_rank = 1 and date(id_date) = %(file_date)s
        """,
        parameters={
            'file_date': str(file_date)
        }
    )

    csv_buffer = StringIO()
    data.to_csv(index=False, path_or_buf=csv_buffer)
    upload_file(file_name=f'repayments_{str(file_date)}.csv', file_bytes=csv_buffer.getvalue().encode('utf-8'), site=site)

def upload_disbursements_ms_teams(file_date):
    saf_dumps_remove_trailing_zeros(
        warehouse_hook=PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
    )
    # remove duplicated disbursements
    warehouse_hook.run(
        sql="""
            delete from bloomlive.transactions_data_dump where
            surrogate_id in (
                with trxns_ranked as
                (
                    select *, rank() over (partition by id_date, id_trxn, id_loan_cntrct, date(dt_trxn_end), src_term_loan,
                    ds_mpsa_enty_name, cd_mpsa_orga_shrt, trxn_type, trxn_amnt, maintnanc_fee, trxn_stts, faild_reasn,
                    id_trxn_linkd, id_idnty, src_lendr, src_crdt_score, src_assgnd_crdt_lmt, src_used_crdt_lmit,
                    src_avail_crdt_lmit order by record_created_on_timestamp desc) id_rank
                    from bloomlive.transactions_data_dump cddd2 where is_disbursement is true
                ) select surrogate_id from trxns_ranked where id_rank > 1
            )
        """
    )
    site = {
        'name': 'Data Dumps',
        'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be',
        'description': 'daily safaricom data dumps',
        'folder': '01BJPCNIBETLWC6I3GTBA2FZI34LFRIEU5'
    }

    # upload data to ms_teams
    data = warehouse_hook.get_pandas_df(
        sql="""
            with disbursements_ranked as
            (
                select *, rank() over (partition by id_trxn, id_trxn_linkd order by surrogate_id) count_rank
                from bloomlive.transactions_data_dump tdd
                where is_disbursement is true
            )
            select
                id_date, id_loan_cntrct, id_trxn, dt_trxn_end, src_term_loan, ds_mpsa_enty_name, cd_mpsa_orga_shrt,
                trxn_type, trxn_amnt, maintnanc_fee, trxn_stts, faild_reasn, id_trxn_linkd,
                id_idnty, src_lendr, src_crdt_score, src_assgnd_crdt_lmt, src_used_crdt_lmit,
                src_avail_crdt_lmit
            from disbursements_ranked where count_rank <= 3 and date(id_date) = %(file_date)s
        """,
        parameters={
            'file_date': file_date
        }
    )

    csv_buffer = StringIO()
    data.to_csv(index=False, path_or_buf=csv_buffer)
    upload_file(file_name=f'disbursements_{str(file_date)}.csv', file_bytes=csv_buffer.getvalue().encode('utf-8'), site=site)

def upload_closed_contracts_ms_teams(file_date):
    saf_dumps_remove_trailing_zeros(
        warehouse_hook=PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
    )
    # remove duplicated daily_closed_contracts_dump
    warehouse_hook.run(
        sql="""
            with rnked as (
                select surrogate_id, rank() over (
                partition by id_date, cntrct_id, prdct, dt_creatd, dt_complte, id_idnty, cntrct_stts, 
                prncpl_amnt, acces_fee, intrst_fee, maintnanc_fee, rollovr_fee, penlty_fee, days 
                order by record_created_on_timestamp desc) rnk
                from bloomlive.daily_closed_contracts_dump dccd
            ) delete from bloomlive.daily_closed_contracts_dump dccd2 where surrogate_id in (select surrogate_id from rnked where rnk > 1)
        """
    )
    site = {
        'name': 'Data Dumps',
        'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be',
        'description': 'daily safaricom data dumps',
        'folder': '01BJPCNIBETLWC6I3GTBA2FZI34LFRIEU5'
    }

    # upload data to ms teams
    data = warehouse_hook.get_pandas_df(
        sql="""
            with contract_details_ranked as
            (
                select *, rank() over (partition by cntrct_id, cntrct_stts order by surrogate_id) count_rank
                from bloomlive.daily_closed_contracts_dump dccd
            )
            select
                id_date, cntrct_id, prdct, dt_creatd, dt_complte, id_idnty, cntrct_stts, prncpl_amnt, 
                acces_fee, intrst_fee, maintnanc_fee, rollovr_fee, penlty_fee, days
            from contract_details_ranked where count_rank = 1 and date(id_date) = %(file_date)s
        """,
        parameters={
            'file_date': file_date
        }
    )

    csv_buffer = StringIO()
    data.to_csv(index=False, path_or_buf=csv_buffer)
    upload_file(file_name=f'daily_closed_contracts_{str(file_date)}.csv', file_bytes=csv_buffer.getvalue().encode('utf-8'), site=site)

def upload_contracts_ms_teams(file_date):
    saf_dumps_remove_trailing_zeros(
        warehouse_hook=PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
    )
    # remove duplicated contract details
    warehouse_hook.run(
        sql="""
            delete from bloomlive.contract_details_data_dump where
            surrogate_id in (
                with contracts_ranked as
                (
                    select *, rank() over (partition by id_date,cntrct_id,src_id_prdct,src_prdct_name,src_tm_cntrct_create,src_dt_cntrct_due,src_id_idnty,src_micro_loan_cntrct_stts,src_totl_amnt,src_prncpl_amnt,src_acces_fee,src_intrst_fee,src_maintnanc_fee,src_rollovr_fee,src_penlty_fee order by record_created_on_timestamp desc) id_rank
                    from bloomlive.contract_details_data_dump cddd2
                ) select surrogate_id from contracts_ranked where id_rank > 1
            )
        """
    )
    site = {
        'name': 'Data Dumps',
        'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be',
        'description': 'daily safaricom data dumps',
        'folder': '01BJPCNIBETLWC6I3GTBA2FZI34LFRIEU5'
    }

    # upload data to ms teams
    data = warehouse_hook.get_pandas_df(
        sql="""
            with contract_details_ranked as
            (
                select *, rank() over (partition by cntrct_id, src_id_prdct, src_tm_cntrct_create, src_id_idnty order by surrogate_id) count_rank
                from bloomlive.contract_details_data_dump cddd
            )
            select
                id_date, cntrct_id, src_id_prdct, src_prdct_name, src_tm_cntrct_create, src_dt_cntrct_due,
                src_id_idnty, src_micro_loan_cntrct_stts, src_totl_amnt, src_prncpl_amnt, src_acces_fee, src_intrst_fee,
                src_maintnanc_fee, src_rollovr_fee, src_penlty_fee
            from contract_details_ranked where count_rank = 1 and date(id_date) = %(file_date)s
        """,
        parameters={
            'file_date': file_date
        }
    )

    csv_buffer = StringIO()
    data.to_csv(index=False, path_or_buf=csv_buffer)
    upload_file(file_name=f'contracts_{str(file_date)}.csv', file_bytes=csv_buffer.getvalue().encode('utf-8'), site=site)

def upload_statements_ms_teams(file_date):
    # remove duplicated statement details
    warehouse_hook.run(
        sql="""
            delete from bloomlive.statement_details_data_dump where
            surrogate_id in (
                with statement_details_ranked as
                (
                    select *, rank() over (partition by id_date, id_loan_cntrct, id_micro_loan_stmt, id_prdct, prdct_name, tm_cntrct_create, dt_cntrct_due, id_idnty, micro_loan_stmt_stts, totl_amnt, prncpl_amnt, acces_fee, intrst_fee, maintnanc_fee, rollovr_fee, penlty_fee, outstndg_amnt, prncpl_amnt_unpaid, acces_fee_unpaid, intrst_fee_unpaid, maintnanc_fee_unpaid, rollovr_fee_unpaid, penlty_fee_unpaid, dt_due order by record_created_on_timestamp desc) id_rank
                    from bloomlive.statement_details_data_dump
                ) select surrogate_id from statement_details_ranked where id_rank > 1
            )
        """
    )
    site = {
        'name': 'Data Dumps',
        'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be',
        'description': 'daily safaricom data dumps',
        'folder': '01BJPCNIBETLWC6I3GTBA2FZI34LFRIEU5'
    }

    disbursements = get_saf_disbursements(
        joining_method="on trxns.id_loan_cntrct = cntrcts.cntrct_id and trxns.id_idnty = cntrcts.src_id_idnty and cntrcts.src_acces_fee = trxns.maintnanc_fee",
        where="where trxns.has_missing_fields is False and trxns.has_correct_cntrct_id is True",
        warehouse_hook=PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
    )[['id_trxn_linkd', 'cntrct_id', 'src_id_idnty']]

    # upload data to ms_teams
    data = warehouse_hook.get_pandas_df(
        sql="""
            select
                surrogate_id,
                id_date,
            case
                when id_prdct = '10001' then 1
                when id_prdct = '10002' then 7
                when id_prdct = '10003' then 21
            end as loan_term,
            id_loan_cntrct, id_micro_loan_stmt, id_prdct, prdct_name, tm_cntrct_create, dt_cntrct_due, id_idnty, micro_loan_stmt_stts, totl_amnt, prncpl_amnt, acces_fee, intrst_fee, maintnanc_fee, rollovr_fee, penlty_fee, outstndg_amnt, prncpl_amnt_unpaid, acces_fee_unpaid, intrst_fee_unpaid, maintnanc_fee_unpaid, rollovr_fee_unpaid, penlty_fee_unpaid, dt_due
            from bloomlive.statement_details_data_dump where date(id_date) = %(file_date)s
        """,
        parameters={'file_date': file_date}
    )

    data = data.merge(
        disbursements,
        left_on=['id_loan_cntrct', 'id_idnty'],
        right_on=['cntrct_id', 'src_id_idnty'],
        how='left'
    ).drop(columns=['cntrct_id', 'surrogate_id'])

    csv_buffer = StringIO()
    data.to_csv(index=False, path_or_buf=csv_buffer)
    upload_file(file_name=f'statement_details_{str(file_date)}.csv', file_bytes=csv_buffer.getvalue().encode('utf-8'), site=site)

    # statements file for ops
    ops_data = data
    ops_data['id_loan_cntrct'] = 'ContractID_' + ops_data['id_loan_cntrct'].astype(str)
    ops_buffer = StringIO()
    ops_data.to_csv(index=False, path_or_buf=ops_buffer)
    upload_file(file_name=f'ops_stmt_details_{str(file_date)}.csv', file_bytes=ops_buffer.getvalue().encode('utf-8'), site=site)

def upload_outstanding_ms_teams(file_date):
    # remove duplicated outstanding loans
    warehouse_hook.run(
        sql="""
            delete from bloomlive.outstanding_loans_data_dump where
            surrogate_id in (
                with outstanding_loans_ranked as
                (
                    select *, rank() over (partition by src_id_date, src_micro_loan, src_cntrct_nmbr, src_outstndng_amnt, src_prncpal_amnt_unpaid, src_micro_loan_nmbr, src_wtch_micro_loan_nmbr, src_substndrd_micro_loan_nmbr, src_lost_micro_loan_nmbr, src_bad_micro_loan_nmbr order by record_created_on_timestamp desc) id_rank
                    from bloomlive.outstanding_loans_data_dump oldd
                ) select surrogate_id from outstanding_loans_ranked where id_rank > 1
            )
        """
    )
    site = {
        'name': 'Data Dumps',
        'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be',
        'description': 'daily safaricom data dumps',
        'folder': '01BJPCNIBETLWC6I3GTBA2FZI34LFRIEU5'
    }

    # upload data to ms_teams
    data = warehouse_hook.get_pandas_df(
        sql="""
            with outstanding_loans_ranked as
            (
                select *, rank() over (partition by src_id_date, src_cntrct_nmbr, src_micro_loan_nmbr order by surrogate_id) count_rank
                from bloomlive.outstanding_loans_data_dump oldd
            )
            select
                src_id_date, src_micro_loan, src_cntrct_nmbr, src_outstndng_amnt, src_prncpal_amnt_unpaid,
                src_micro_loan_nmbr, src_wtch_micro_loan_nmbr, src_substndrd_micro_loan_nmbr, src_lost_micro_loan_nmbr,
                src_bad_micro_loan_nmbr
            from outstanding_loans_ranked where count_rank = 1 and date(src_id_date) = %(file_date)s
        """,
        parameters={
            'file_date': file_date
        }
    )

    csv_buffer = StringIO()
    data.to_csv(index=False, path_or_buf=csv_buffer)
    upload_file(file_name=f'outstanding_{str(file_date)}.csv', file_bytes=csv_buffer.getvalue().encode('utf-8'), site=site)

def upload_all_opt_ins_ms_teams():
    site = {
        'name': 'Data Dumps',
        'id': 'e0a4080f-99bc-4d27-83fb-04cad208a4be',
        'description': 'daily safaricom data dumps',
        'folder': '01BJPCNIBETLWC6I3GTBA2FZI34LFRIEU5'
    }

    data = warehouse_hook.get_pandas_df(
        sql="""
            with opt_ins_ranked as
            (
                select *, rank() over (partition by cust_idnty_id, opt_in_date order by surrogate_id) count_rank
                from bloomlive.client_activity_data_dump cadd
                where opt_in_date is not null
            )
            select cust_id_nmbr, cust_idnty_id, enty_name, opt_in_date, mpsa_orga_shrt, mpsa_enty_phne, ds_cst_name
            from opt_ins_ranked where count_rank = 1
        """
    )

    csv_buffer = StringIO()
    data.to_csv(index=False, path_or_buf=csv_buffer)
    upload_file(file_name=f'opt_ins_full_{datetime.datetime.today().date()}.csv', file_bytes=csv_buffer.getvalue().encode('utf-8'), site=site)
