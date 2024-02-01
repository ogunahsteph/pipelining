import os
import sys
import pytz
import logging
import requests
import datetime
import pendulum
import numpy as np
import pandas as pd
from textwrap import dedent
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from ms_teams_webhook_operator import MSTeamsWebhookOperator


def first_and_last_day_of_month(year, month):
    import calendar
    import datetime

    # Get the number of days in the given month
    num_days = calendar.monthrange(year, month)[1]

    # Create a datetime object for the first day of the month
    first_day = datetime.date(year=year, month=month, day=1)

    # Create a datetime object for the last day of the month
    last_day = datetime.date(year=year, month=month, day=num_days)

    return [first_day, last_day]


def remove_loan_facts_table_duplicates(schema, filters=None):
    warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                                  schema='afsg_ds_prod_postgresql_dwh')

    if filters is not None and 'bloom_version' in filters:
        sql = f"""
            with rnked as (
                select surrogate_id, mifos_id, rank() over (partition by mifos_id, bloom_version order by record_created_on_date asc) rnk
                from {schema}.loans_fact_table lft where is_most_recent_record {filters}
            ) delete from bloomlive.loans_fact_table lft2 where surrogate_id in (select surrogate_id from rnked where rnk > 1)
        """
    else:
        sql = f"""
            with rnked as (
                select surrogate_id, mifos_id, rank() over (partition by mifos_id order by record_created_on_date asc) rnk
                from {schema}.loans_fact_table lft where is_most_recent_record {filters if filters is not None else ''}
            ) delete from {schema}.loans_fact_table lft2 where surrogate_id in (select surrogate_id from rnked where rnk > 1)
        """

    warehouse_hook.run(sql=sql)


def on_failure(context):
    """
    Send MS Teams Alert
    """
    from urllib import parse

    dag_id = context['dag_run'].dag_id
    task_id = context['task_instance'].task_id
    context['task_instance'].xcom_push(key=dag_id, value=True)

    logs_url = "https://airflow.asantefsg.com/data-pipelines/log?dag_id={}&task_id={}&execution_date={}".format(
        dag_id, task_id, parse.quote_plus(context['ts']))

    teams_notification = MSTeamsWebhookOperator(
        task_id="ms_teams_notify_failure", trigger_rule="all_done",
        message="`{}` has failed on task: `{}`".format(dag_id, task_id),
        button_text="View log", button_url=logs_url,
        theme_color="FF0000", http_conn_id='msteams_webhook_url')
    teams_notification.execute(context)


def product_dimension_process_new_products(warehouse_schema, products, loans_fact_table_updates,
                                           expired_products_updates):
    warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                                  schema='afsg_ds_prod_postgresql_dwh')

    # Update loans_fact_table and execute expired_products_updates if needed
    if len(expired_products_updates) > 0:
        warehouse_hook.run(sql=expired_products_updates)
    if len(loans_fact_table_updates) > 0:
        warehouse_hook.run(sql=loans_fact_table_updates)

    # Insert new product records into ubuntu.bloomlive.product_dimension
    if products.shape[0] > 0:
        products['is_most_recent_record'] = True
        target_fields = [
            'mifos_id', 'name', 'short_name', 'description', 'currency_code', 'currency_digits',
            'currency_multiples_of', 'min_principal_amount', 'max_principal_amount',
            'nominal_interest_rate_per_period',
            'annual_nominal_interest_rate', 'repayment_period_frequency', 'amortization_method',
            'interest_period_frequency', 'interest_method', 'interest_calculated_in_period',
            'number_of_repayments', 'grace_on_principal_periods', 'grace_on_interest_periods',
            'grace_interest_free_periods', 'recurring_moratorium_principal_periods', 'days_in_month',
            'days_in_year', 'start_date', 'close_date', 'allow_partial_period_interest_calculation', 'repay_every',
            'min_number_of_repayments', 'grace_on_arrears_ageing', 'overdue_days_for_npa', 'is_most_recent_record'
        ]
        src_fields = [
            'id', 'name', 'short_name', 'description', 'currency_code', 'currency_digits',
            'currency_multiplesof', 'min_principal_amount', 'max_principal_amount',
            'nominal_interest_rate_per_period',
            'annual_nominal_interest_rate', 'repayment_period_frequency_enum', 'amortization_method_enum',
            'interest_period_frequency_enum', 'interest_method_enum', 'interest_calculated_in_period_enum',
            'number_of_repayments', 'grace_on_principal_periods', 'grace_on_interest_periods',
            'grace_interest_free_periods', 'recurring_moratorium_principal_periods', 'days_in_month_enum',
            'days_in_year_enum', 'start_date', 'close_date', 'allow_partial_period_interest_calcualtion', 'repay_every',
            'min_number_of_repayments', 'grace_on_arrears_ageing', 'overdue_days_for_npa', 'is_most_recent_record']

        if warehouse_schema == 'bloomlive':
            src_fields += ['bloom_version']
            target_fields += ['bloom_version']

        # INSERT NEW RECORDS
        warehouse_hook.insert_rows(
            table=f'{warehouse_schema}.product_dimension',
            target_fields=target_fields,
            replace=False,
            rows=tuple(products[src_fields].replace({np.nan: None}).itertuples(index=False, name=None)),
            commit_every=1000
        )


def get_mifos_products_for_dimension(warehouse_schema: str, mifos_hook: MySqlHook, warehouse_filters=None,
                                     mifos_filters=None):
    warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                                  schema='afsg_ds_prod_postgresql_dwh')

    sql = f"""
        SELECT mifos_id FROM {warehouse_schema}.product_dimension WHERE is_most_recent_record 
    """
    existing_records = warehouse_hook.get_pandas_df(
        sql=sql if not warehouse_filters else sql + f"and {warehouse_filters}"
    )['mifos_id'].tolist()

    sql = """
        SELECT id,name,short_name,description,currency_code,currency_digits,currency_multiplesof, 
        min_principal_amount, max_principal_amount,nominal_interest_rate_per_period, 
        annual_nominal_interest_rate, repayment_period_frequency_enum,
        amortization_method_enum, interest_period_frequency_enum, interest_method_enum, 
        interest_calculated_in_period_enum, number_of_repayments, grace_on_principal_periods, 
        grace_on_interest_periods, grace_interest_free_periods, recurring_moratorium_principal_periods,
        days_in_month_enum, days_in_year_enum, start_date, close_date, allow_partial_period_interest_calcualtion, 
        repay_every, min_number_of_repayments, grace_on_arrears_ageing, overdue_days_for_npa 
        FROM m_product_loan 
    """
    if mifos_filters:
        sql += mifos_filters

    products = mifos_hook.get_pandas_df(sql=sql)

    products['allow_partial_period_interest_calcualtion'] = products[
        'allow_partial_period_interest_calcualtion'].apply(
        lambda x: True if x == 1 else False
    )

    loans_fact_table_updates = []
    expired_products_updates = []
    changed_records = []
    for index, row in products.iterrows():
        if row['id'] in existing_records:
            sql = f"""
                select
                    "name",short_name,description,currency_code,currency_digits,
                    currency_multiples_of as currency_multiplesof, min_principal_amount,
                    max_principal_amount,nominal_interest_rate_per_period,annual_nominal_interest_rate,
                    repayment_period_frequency as repayment_period_frequency_enum,
                    amortization_method as amortization_method_enum,interest_period_frequency as interest_period_frequency_enum,interest_method as interest_method_enum,
                    interest_calculated_in_period as interest_calculated_in_period_enum,number_of_repayments,
                    grace_on_principal_periods,grace_on_interest_periods,grace_interest_free_periods,recurring_moratorium_principal_periods,
                    days_in_month as days_in_month_enum,days_in_year as days_in_year_enum,start_date,close_date,
                    allow_partial_period_interest_calculation as allow_partial_period_interest_calcualtion,repay_every,min_number_of_repayments,
                    grace_on_arrears_ageing,overdue_days_for_npa, surrogate_id
                from {warehouse_schema}.product_dimension where mifos_id = {row['id']} and is_most_recent_record
            """

            prev_record = warehouse_hook.get_pandas_df(
                sql=sql if not warehouse_filters else sql + f"and {warehouse_filters}"
            )

            # SCD Type 2 - Data Warehousing
            if not row.drop(labels=['id']).equals(prev_record.drop(columns=['surrogate_id']).iloc[0]):
                # Update previous record
                now = datetime.datetime.now()

                expired_products_updates.append(
                    f"UPDATE {warehouse_schema}.product_dimension SET record_expired_on_timestamp = '{str(now)}', is_most_recent_record = FALSE WHERE surrogate_id = {prev_record['surrogate_id'].iloc[0]}"
                )

                new_surrogate_id = warehouse_hook.run(
                    sql=f"""
                        insert into {warehouse_schema}.product_dimension(
                            mifos_id, name, short_name, description, currency_code, currency_digits,
                            currency_multiples_of, min_principal_amount, max_principal_amount,
                            nominal_interest_rate_per_period, annual_nominal_interest_rate,
                            repayment_period_frequency, amortization_method, interest_period_frequency,
                            interest_method, interest_calculated_in_period, number_of_repayments,
                            grace_on_principal_periods, grace_on_interest_periods, grace_interest_free_periods,
                            recurring_moratorium_principal_periods, days_in_month, days_in_year, start_date,
                            close_date, allow_partial_period_interest_calculation, repay_every,
                            min_number_of_repayments, grace_on_arrears_ageing, overdue_days_for_npa,
                            record_created_on_timestamp, record_expired_on_timestamp, is_most_recent_record
                        ) values %(values)s
                        returning surrogate_id
                    """,
                    handler=lambda x: x.fetchall()[0][0],
                    parameters={'values': tuple(
                        pd.concat([row, pd.Series(
                            [now, None, True],
                            index=['record_created_on_timestamp', 'record_expired_on_timestamp',
                                   'is_most_recent_record']
                        )]).replace({np.NAN: None}).values
                    )}
                )

                loans_fact_table_updates.append(
                    f"update bloomlive.loans_fact_table set product_key = {int(new_surrogate_id)} where product_key = {int(prev_record['surrogate_id'])}"
                )
                changed_records.append(int(new_surrogate_id))
            products.drop(index=index, inplace=True)

    return {
        'products': products,
        'loans_fact_table_updates': loans_fact_table_updates,
        'expired_products_updates': expired_products_updates,
        'changed_records': changed_records,
    }


def el_loan_status_dimension(warehouse_schema: str, mifos_hook: MySqlHook, warehouse_filters=None) -> pd.DataFrame:
    warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                                  schema='afsg_ds_prod_postgresql_dwh')

    sql = f"SELECT mifos_id FROM {warehouse_schema}.loan_status_dimension "
    existing_records = warehouse_hook.get_pandas_df(
        sql=sql if not warehouse_filters else sql + f"where {warehouse_filters}"
    )['mifos_id'].tolist()  # records existing in the warehouse

    if len(existing_records) > 0:
        # extract only those records that are not already stored in the warehouse
        iterables = ', '.join("'{0}'".format(w) for w in existing_records)
        statuses = mifos_hook.get_pandas_df(
            sql="SELECT enum_id,enum_message_property,enum_value FROM r_enum_value "
                "WHERE enum_name = 'loan_status_id' AND enum_id not in ({})".format(
                iterables
            )
        )
    else:
        statuses = mifos_hook.get_pandas_df(
            sql="SELECT enum_id,enum_message_property,enum_value FROM r_enum_value "
                "WHERE enum_name = 'loan_status_id' "
        )

    if statuses.shape[0] > 0:
        target_fields = ['mifos_id', 'message', 'value']
        src_fields = ['enum_id', 'enum_message_property', 'enum_value']

        if warehouse_filters is not None:
            if 'bloom_version' in warehouse_filters:
                statuses['bloom_version'] = warehouse_filters.split(' ')[-1].replace("'", "")
                src_fields.append('bloom_version')
                target_fields.append('bloom_version')

        warehouse_hook.insert_rows(
            table=f'{warehouse_schema}.loan_status_dimension',
            target_fields=target_fields,
            replace=False,
            rows=tuple(statuses[src_fields].replace({np.nan: None}).itertuples(index=False, name=None)),
            commit_every=10
        )


def get_mifos_loan_officer_assignment_history(mifos_tenant: str, warehouse_schema: str, warehouse_hook: PostgresHook,
                                              mifos_hook: MySqlHook, filters=None) -> pd.DataFrame:
    """
    Extracts from mifostenant-safaricom.m_payment_detail and loads into ubuntu.bloomlive.transactions_dimension
    """
    sql = f"""
        SELECT mifos_id FROM {warehouse_schema}.loan_officer_assignment_history
    """

    existing_records = warehouse_hook.get_pandas_df(
        sql=sql if not filters else sql + filters
    )

    if existing_records.shape[0] > 0:
        # extract only those records that are not already stored in the warehouse
        id_list = existing_records['mifos_id'].tolist()
        if len(id_list) == 1:
            condition = f'id NOT IN {str(tuple(id_list)).replace(",", "")}'  # No tuple or trailing comma
        else:
            condition = f'id NOT IN {tuple(id_list)}'

        assignment_history = mifos_hook.get_pandas_df(
            sql=f"""
                select 
                    id as mifos_id, loan_id as loan_mifos_id, loan_officer_id as loan_officer_mifos_id, 
                    start_date, end_date, createdby_id as created_by_id, created_date, 
                    lastmodified_date as last_modified_date, lastmodifiedby_id as last_modified_by_id
                from `mifostenant-{mifos_tenant}`.m_loan_officer_assignment_history
                where {condition}
            """,
        )
    else:
        assignment_history = mifos_hook.get_pandas_df(
            sql=f"""
                select 
                    id as mifos_id, loan_id as loan_mifos_id, loan_officer_id as loan_officer_mifos_id, 
                    start_date, end_date, createdby_id as created_by_id, created_date, 
                    lastmodified_date as last_modified_date, lastmodifiedby_id as last_modified_by_id
                from `mifostenant-{mifos_tenant}`.m_loan_officer_assignment_history
            """
        )

    return assignment_history


def el_staff_dimension(warehouse_schema: str, mifos_hook: MySqlHook, warehouse_filters=None) -> pd.DataFrame:
    warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                                  schema='afsg_ds_prod_postgresql_dwh')

    sql = f"SELECT mifos_id FROM {warehouse_schema}.staff_dimension "
    existing_records = warehouse_hook.get_pandas_df(
        sql=sql if not warehouse_filters else sql + warehouse_filters
    )['mifos_id'].tolist()  # records existing in the warehouse

    if len(existing_records) > 0:
        # extract only those records that are not already stored in the warehouse
        iterables = ', '.join("{0}".format(w) for w in existing_records)
        data = mifos_hook.get_pandas_df(
            sql="""
                SELECT id,is_loan_officer,office_id,firstname,lastname,is_active,joining_date, external_id,
                organisational_role_enum, organisational_role_parent_staff_id
                FROM m_staff WHERE id not in ({})
            """.format(iterables)
        )
    else:
        data = mifos_hook.get_pandas_df(
            sql="""SELECT id,is_loan_officer,office_id,firstname,lastname,is_active,joining_date, external_id,
                organisational_role_enum, organisational_role_parent_staff_id FROM m_staff"""
        )

    data['is_loan_officer'] = data['is_loan_officer'].apply(
        lambda x: True if x == 1 else False
    )
    data['is_active'] = data['is_active'].apply(
        lambda x: True if x == 1 else False
    )

    if data.shape[0] > 0:
        target_fields = [
            'mifos_id', 'is_loan_officer', 'office_id', 'first_name', 'last_name', 'is_active',
            'joining_date', 'external_id', 'organisational_role_enum',
            'organisational_role_parent_staff_id',
        ]
        src_fields = [
            'id', 'is_loan_officer', 'office_id', 'firstname', 'lastname', 'is_active',
            'joining_date', 'external_id', 'organisational_role_enum',
            'organisational_role_parent_staff_id',
        ]

        # Load the data into the staff_dimension table
        if warehouse_filters is not None:
            if 'bloom_version' in warehouse_filters:
                data['bloom_version'] = warehouse_filters.split(' ')[-1].replace("'", "")
                src_fields.append('bloom_version')
                target_fields.append('bloom_version')

        warehouse_hook.insert_rows(
            table=f'{warehouse_schema}.staff_dimension',
            target_fields=target_fields,
            replace=False,
            rows=tuple(data[src_fields].replace({np.nan: None}).itertuples(index=False, name=None)),
            commit_every=1000
        )


def merge_loans_with_warehouse_keys(loans, clients, products, loan_statuses, loan_officers, mifos_hook: MySqlHook):
    loan_type_enums = mifos_hook.get_pandas_df(
        sql="SELECT enum_id, enum_value FROM `r_enum_value` WHERE enum_name = 'loan_type_enum'"
    )
    loan_transaction_processing_strategies = mifos_hook.get_pandas_df(
        sql="SELECT id, code FROM `ref_loan_transaction_processing_strategy` "
    )
    term_period_frequency_enums = mifos_hook.get_pandas_df(
        sql="SELECT enum_id, enum_value FROM `r_enum_value` WHERE enum_name = 'term_period_frequency_enum'"
    )

    # merge loans with warehouse surrogate ids
    loans = loans.astype({
        'client_id': int, 'product_id': int, 'loan_status_id': int
    }).merge(
        clients.astype({'client_mifos_id': int}),
        left_on='client_id',
        right_on='client_mifos_id',
        how='inner'
    ).drop(columns='client_mifos_id').merge(
        products.astype({'product_mifos_id': int}),
        left_on='product_id',
        right_on='product_mifos_id',
        how='inner'
    ).drop(columns='product_mifos_id').merge(
        loan_statuses.astype({'loan_status_mifos_id': int}),
        left_on='loan_status_id',
        right_on='loan_status_mifos_id',
        how='left'
    ).drop(columns='loan_status_mifos_id').merge(
        loan_officers.astype({'loan_officer_mifos_id': int}),
        left_on='loan_officer_id',
        right_on='loan_officer_mifos_id',
        how='left'
    ).drop(columns='loan_officer_mifos_id')

    loans['loan_type_enum'] = loans['loan_type_enum'].apply(
        lambda x: loan_type_enums[loan_type_enums['enum_id'] == x]['enum_value'].item()
    )
    loans['loan_transaction_strategy_id'] = loans['loan_transaction_strategy_id'].apply(
        lambda x: loan_transaction_processing_strategies[loan_transaction_processing_strategies['id'] == x][
            'code'].item()
    )
    loans['term_period_frequency_enum'] = loans['term_period_frequency_enum'].apply(
        lambda x: term_period_frequency_enums[term_period_frequency_enums['enum_id'] == x]['enum_value'].item()
    )
    loans['allow_partial_period_interest_calcualtion'] = loans[
        'allow_partial_period_interest_calcualtion'].apply(
        lambda x: False if x == 0 else True
    )
    loans['interest_recalculation_enabled'] = loans['interest_recalculation_enabled'].apply(
        lambda x: False if x == 0 else True
    )
    loans['is_npa'] = loans['is_npa'].apply(
        lambda x: False if x == 0 else True
    )
    loans['is_topup'] = loans['is_topup'].apply(
        lambda x: False if x == 0 else True
    )
    loans['record_created_on_date'] = datetime.datetime.now()
    loans['record_expired_on_date'] = np.NAN
    loans['is_most_recent_record'] = True

    return loans


def rename_loan_columns(loans):
    return loans.rename(columns={
        'account_no': 'account_number',
        'allow_partial_period_interest_calcualtion': 'allow_partial_period_interest_calculation',
        'approvedon_date': 'approved_on_date', 'approvedon_userid': 'approved_on_user_id',
        'arrearstolerance_amount': 'arrears_tolerance_amount',
        'closedon_date': 'closed_on_date', 'closedon_userid': 'closed_on_user_id',
        'disbursedon_date': 'disbursed_on_date', 'disbursedon_userid': 'disbursed_on_user_id',
        'expected_disbursedon_date': 'expected_disbursed_on_date',
        'expected_firstrepaymenton_date': 'expected_first_repayment_on_date',
        'expected_maturedon_date': 'expected_matured_on_date',
        'fee_charges_charged_derived': 'fee_charges_charged',
        'fee_charges_outstanding_derived': 'fee_charges_outstanding',
        'fee_charges_repaid_derived': 'fee_charges_repaid',
        'fee_charges_waived_derived': 'fee_charges_waived',
        'fee_charges_writtenoff_derived': 'fee_charges_written_off',
        'guarantee_amount_derived': 'guarantee_amount', 'id': 'mifos_id',
        'interest_charged_derived': 'interest_charged',
        'interest_outstanding_derived': 'interest_outstanding',
        'interest_recalcualated_on': 'interest_recalculated_on',
        'interest_repaid_derived': 'interest_repaid', 'interest_waived_derived': 'interest_waived',
        'interest_writtenoff_derived': 'interest_written_off', 'loan_counter': 'counter',
        'loan_product_counter': 'product_counter', 'loan_transaction_strategy_id': 'transaction_strategy',
        'loan_type_enum': 'loan_type', 'maturedon_date': 'matured_on_date',
        'penalty_charges_charged_derived': 'penalty_charges_charged',
        'penalty_charges_outstanding_derived': 'penalty_charges_outstanding',
        'penalty_charges_repaid_derived': 'penalty_charges_repaid',
        'penalty_charges_waived_derived': 'penalty_charges_waived',
        'penalty_charges_writtenoff_derived': 'penalty_charges_written_off',
        'principal_disbursed_derived': 'principal_disbursed',
        'principal_outstanding_derived': 'principal_outstanding',
        'principal_repaid_derived': 'principal_repaid',
        'principal_writtenoff_derived': 'principal_written_off',
        'rejectedon_date': 'rejected_on_date', 'rejectedon_userid': 'rejected_on_user_id',
        'rescheduledon_date': 'rescheduled_on_date', 'rescheduledon_userid': 'rescheduled_on_user_id',
        'submittedon_date': 'submitted_on_date', 'submittedon_userid': 'submitted_on_user_id',
        'term_period_frequency_enum': 'term_period_frequency',
        'total_charges_due_at_disbursement_derived': 'total_charges_due_at_disbursement',
        'total_costofloan_derived': 'total_cost_of_loan',
        'total_expected_costofloan_derived': 'total_expected_cost_of_loan',
        'total_expected_repayment_derived': 'total_expected_repayment',
        'total_outstanding_derived': 'total_outstanding', 'total_overpaid_derived': 'total_overpaid',
        'total_recovered_derived': 'total_recovered',
        'total_repayment_derived': 'total_repayment', 'total_waived_derived': 'total_waived',
        'total_writtenoff_derived': 'total_written_off', 'version': 'loan_version',
        'withdrawnon_date': 'withdrawn_on_date', 'withdrawnon_userid': 'withdrawn_on_user_id',
        'writtenoffon_date': 'written_off_on_date',
        'loan_officer_surrogate_id': 'loan_officer_key', 'client_surrogate_id': 'client_key',
        'loan_status_surrogate_id': 'status_key', 'product_surrogate_id': 'product_key',
    })


def el_transactions_dimension(mifos_tenant, warehouse_schema, transaction_types, office_id, year, month):
    days = first_and_last_day_of_month(year=year, month=month)
    start_date = days[0]
    end_date = days[1]

    reversed_transactions = get_mifos_transactions(
        tenant_name=mifos_tenant,
        office_id=office_id,
        is_reversed=True,
        extra=f"""and date(mlt.transaction_date) between '{start_date}' and '{end_date}'""",
        transaction_types=transaction_types
    )

    transactions = get_mifos_transactions(
        tenant_name=mifos_tenant,
        office_id=office_id,
        is_reversed=False,
        transaction_types=transaction_types,
        extra=f"""and date(mlt.transaction_date) between '{start_date}' and '{end_date}'""",
    )
    if transactions.shape[0] > 0:
        # Store transactions to transactions_dimension table
        store_transactions_dimension(
            reversed_transactions=reversed_transactions,
            transactions=transactions,
            warehouse_schema=warehouse_schema
        )


def store_transactions_dimension(transactions: pd.DataFrame, warehouse_schema: str,
                                 reversed_transactions: pd.DataFrame) -> None:
    """
    Store the transaction data in the transaction_dimension table in the schema provided.

    Parameters:
    transactions (pandas.DataFrame): DataFrame containing the transaction information to be stored
    schema (str): Schema name in which the transaction_dimension table is located

    Returns:
    None

    """
    # Connect to the postgres database using the airflow warehouse hook
    warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                                  schema='afsg_ds_prod_postgresql_dwh')
    if reversed_transactions.shape[0] > 0:
        transactions = transactions[
            ~transactions['transaction_id'].isin(reversed_transactions['transaction_id'].tolist())]

        if 'bloom_version' in transactions.columns:
            warehouse_hook.run(
                sql=f"""
                    delete from {warehouse_schema}.transactions_dimension where transaction_id in %(reversed_trxns)s and bloom_version = %(bloom_version)s
                """,
                parameters={
                    'reversed_trxns': tuple(reversed_transactions['transaction_id'].tolist()),
                    'bloom_version': str(transactions.iloc[0]['bloom_version'])
                }
            )
        else:
            warehouse_hook.run(
                sql=f"""
                    delete from {warehouse_schema}.transactions_dimension where transaction_id in %(reversed_trxns)s
                """,
                parameters={'reversed_trxns': tuple(reversed_transactions['transaction_id'].tolist())}
            )

    if transactions.shape[0] > 0:
        # Define the function to convert 0 or 1 values to boolean values
        def convert_to_bool(x):
            if x == 0:
                return False
            elif x == 1:
                return True
            return x

        # Apply the conversion function to the is_reversed column
        transactions['is_reversed'] = transactions['is_reversed'].apply(lambda x: convert_to_bool(x))

        # Replace empty values with NaN
        transactions.replace(r'^\s*$', np.NAN, regex=True, inplace=True)

        # Set the is_repayment and is_disbursement columns based on the transaction_type_enum column
        transactions.loc[transactions['transaction_type_enum'] == 2, 'is_repayment'] = True
        transactions.loc[transactions['transaction_type_enum'] == 2, 'is_disbursement'] = False
        transactions.loc[transactions['transaction_type_enum'] == 1, 'is_disbursement'] = True
        transactions.loc[transactions['transaction_type_enum'] == 1, 'is_repayment'] = False

        # Insert the new records into the transaction_dimension table in the schema provided
        transactions.rename(columns={'loan_id': 'mifos_loan_id'}, inplace=True)

        warehouse_hook.insert_rows(
            table=f'{warehouse_schema}.transactions_dimension',
            target_fields=transactions.reindex(sorted(transactions.index)).columns.tolist(),
            rows=tuple(
                transactions.reindex(sorted(transactions.index)).replace({np.NAN: None}).itertuples(index=False)),
            replace=True,
            commit_every=2000,
            replace_index=['bloom_version', 'transaction_id'] if warehouse_schema == 'bloomlive' else ['transaction_id']
        )


def store_company_name_updates(updated: list, source: str, airflow_hook: MySqlHook):
    update_logs = pd.DataFrame(
        [result for results in updated for result in results],
        columns=['updated_on_timestamp', 'current_company_name', 'previous_company_name', 'client_surrogate_id']
    )
    update_logs['source'] = source

    utc_timezone = pytz.timezone('UTC')
    eat_timezone = pytz.timezone('Africa/Nairobi')

    update_logs['updated_on_timestamp'] = update_logs['updated_on_timestamp'].apply(
        lambda x: utc_timezone.localize(
            datetime.datetime.strptime(x.split("+")[0], '%Y-%m-%d %H:%M:%S.%f')).astimezone(eat_timezone)
    )
    airflow_hook.insert_rows(
        table='company_name_updates',
        target_fields=update_logs.reindex(sorted(update_logs.index)).columns.tolist(),
        rows=tuple(update_logs.reindex(sorted(update_logs.index)).replace({np.NAN: None}).itertuples(index=False,
                                                                                                     name=None)),
        commit_every=100
    )


def store_national_id_updates(updated: list, source: str, airflow_hook: MySqlHook):
    update_logs = pd.DataFrame(
        [result for results in updated for result in results],
        columns=['updated_on_timestamp', 'current_national_id', 'previous_national_id', 'client_surrogate_id']
    )
    update_logs['source'] = source

    utc_timezone = pytz.timezone('UTC')
    eat_timezone = pytz.timezone('Africa/Nairobi')

    update_logs['updated_on_timestamp'] = update_logs['updated_on_timestamp'].apply(
        lambda x: utc_timezone.localize(
            datetime.datetime.strptime(x.split("+")[0], '%Y-%m-%d %H:%M:%S.%f')).astimezone(eat_timezone)
    )
    airflow_hook.insert_rows(
        table='national_id_updates',
        target_fields=update_logs.reindex(sorted(update_logs.index)).columns.tolist(),
        rows=tuple(update_logs.reindex(sorted(update_logs.index)).replace({np.NAN: None}).itertuples(index=False,
                                                                                                     name=None)),
        commit_every=100
    )


def get_mifos_clients(mifos_tenant: str, warehouse_schema: str, warehouse_filters=None,
                      mifos_filters=None) -> pd.DataFrame or None:
    mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}')
    warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                                  schema='afsg_ds_prod_postgresql_dwh')

    count_sql = "select count(*) as ttl from m_client mc "
    if mifos_tenant in ('default', 'bloom1restructure'):
        clients_sql = """
            SELECT 
                mc.id, mc.account_no, m.customer_id_number as national_id, m.account_number as store_number, 
                mc.office_id, rev.enum_value as status_enum, m.company_name, 
                mc.default_savings_account, mcv.code_value as gender_cv_id, mcv2.code_value as client_type_cv_id, 
                mc.firstname, mc.middlename, mc.lastname, mc.mobile_no, mc.date_of_birth, mc.submittedon_date,
                mc.submittedon_userid, mc.activatedon_userid, mc.activation_date, mc.office_joining_date
            from m_client mc
            left join `mifostenant-default`.Metadata m on mc.id = m.client_id
        """
    else:
        clients_sql = """
            SELECT 
                mc.id, mc.account_no, mc.external_id as national_id, mc.office_id, rev.enum_value as status_enum, 
                mc.default_savings_account, mcv.code_value as gender_cv_id, mcv2.code_value as client_type_cv_id, 
                mc.firstname, mc.middlename, mc.lastname, mc.mobile_no, mc.date_of_birth, mc.submittedon_date,
                mc.submittedon_userid, mc.activatedon_userid, mc.activation_date, mc.office_joining_date
            from m_client mc
        """
    clients_sql += """
        left join m_code_value mcv on mc.gender_cv_id = mcv.id 
        left join r_enum_value rev on mc.status_enum = rev.enum_id 
        left join m_code_value mcv2 on mc.client_type_cv_id = mcv2.id  
        where rev.enum_name = 'status_enum'
    """

    if mifos_filters:
        count_sql += f'where {mifos_filters}'
        clients_sql += f' and {mifos_filters}'

    total_clients = mifos_hook.get_pandas_df(sql=count_sql).iloc[0]['ttl']

    all_new_clients = []
    chunk_size = 10000  # batch size to process per iteration
    for num in range(0, total_clients, chunk_size):
        clients = mifos_hook.get_pandas_df(
            sql=clients_sql + f" order by id desc LIMIT {num}, {chunk_size}"
        )
        iterables = ', '.join("{0}".format(w) for w in clients['id'].tolist())
        existing_records_sql = f"""
            select mifos_id from {warehouse_schema}.client_dimension
            where mifos_id in ({iterables})
        """
        if warehouse_filters:
            existing_records_sql += f'and {warehouse_filters}'

        existing_records = warehouse_hook.get_pandas_df(sql=existing_records_sql)

        new_clients = clients[~clients['id'].isin(existing_records['mifos_id'].tolist())]
        if not new_clients.empty:
            all_new_clients.append(new_clients)

    if len(all_new_clients) > 0:
        return pd.concat(all_new_clients).drop_duplicates(subset=['id'])

    return None


def get_mifos_transactions(tenant_name: str, office_id: int, is_reversed: bool, transaction_types: list,
                           extra: str or None) -> pd.DataFrame:
    """
    Returns a pandas DataFrame containing a list of invalidated loan transactions for the specified tenant and office.

    :param tenant_name: Name of the tenant
    :type tenant_name: str
    :param office_id: ID of the office
    :type office_id: int
    :return: DataFrame containing a list of invalidated loan transactions
    :rtype: pd.DataFrame
    """
    mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{tenant_name}')
    if tenant_name in ['safaricom', 'default', 'bloom1restructure']:
        sql = f"""
            select 
                ml.id as loan_id, mpd.id as payment_id, mpd.account_number, mpt.value as payment_detail_id,
                mpd.receipt_number, mlt.id as transaction_id, mlt.office_id,
                mlt.is_reversed, mlt.transaction_type_enum, mlt.transaction_date, mlt.amount, 
                mlt.principal_portion_derived, mlt.interest_portion_derived, mlt.fee_charges_portion_derived,
                mlt.penalty_charges_portion_derived,
                mlt.submitted_on_date, mlt.created_date, mlt.appuser_id, mlt.outstanding_loan_balance_derived
            from m_loan ml
            inner join m_loan_transaction mlt on mlt.loan_id = ml.id
            inner join m_payment_detail mpd on mpd.id = mlt.payment_detail_id
            left join m_payment_type mpt on mpt.id = mpd.payment_type_id
            where mlt.transaction_type_enum in ({', '.join("{0}".format(w) for w in transaction_types)}) 
            and mlt.is_reversed = {int(is_reversed)}
            and mlt.office_id = {office_id}
        """
    else:
        sql = f"""
            select 
                ml.id as loan_id, mpd.id as payment_id, mpd.account_number, mpt.value as payment_detail_id,
                mpd.receipt_number, mlt.id as transaction_id, mlt.office_id,
                mlt.is_reversed, mlt.transaction_type_enum, mlt.transaction_date, mlt.amount, 
                mlt.principal_portion_derived, mlt.interest_portion_derived, mlt.fee_charges_portion_derived,
                mlt.penalty_charges_portion_derived,
                mlt.submitted_on_date, mlt.created_date, mlt.appuser_id, mlt.outstanding_loan_balance_derived
            from m_loan ml
            inner join m_loan_transaction mlt on mlt.loan_id = ml.id
            left join m_payment_detail mpd on mpd.id = mlt.payment_detail_id
            left join m_payment_type mpt on mpt.id = mpd.payment_type_id
            where mlt.transaction_type_enum in ({', '.join("{0}".format(w) for w in transaction_types)}) 
            and mlt.is_reversed = {int(is_reversed)}
            and mlt.office_id =  {office_id}
        """

    if extra is not None:
        sql += extra
    trxns = mifos_hook.get_pandas_df(sql=sql)

    if tenant_name == 'safaricom':
        trxns['bloom_version'] = '2'
    elif tenant_name == 'default':
        trxns['bloom_version'] = '1'
    elif tenant_name == 'bloom1restructure':
        trxns['bloom_version'] = 'bloom1_restructured'

    return trxns


def get_bloom_whitelist(data, warehouse_hook: PostgresHook, model_version=None):
    logging.warning('START: Fetch client details')
    client_details = warehouse_hook.get_pandas_df(
        sql="""
            with rnked as (
                select bloom_version, store_number::varchar as store_number, mobile_number::varchar, primary_contact, first_name, middle_name, last_name, company_name,
                secondary_contact, alternate_mobile_no1, alternate_mobile_no2,
                rank() over (partition by store_number order by bloom_version desc) rnk
                from bloomlive.client_materialized_summary_view where bloom_version in ('1', '2')
            ) select * from rnked where rnk = 1
        """,
        parameters={'store_numbers': tuple(data['Store_Number'].tolist())}
    )
    logging.warning('FINISH: Fetch client details')

    data = data.merge(
        client_details,
        left_on='Store_Number',
        right_on='store_number',
        how='left'
    )  # .dropna(subset=['store_number'])

    logging.warning('START: Fetch till suspension details')
    till_suspended_details = warehouse_hook.get_pandas_df(
        sql="""
            select store_number as sn, is_suspended, start_date
            from bloomlive.till_suspension_dimension
            where store_number in %(store_numbers)s and is_most_recent_record
        """,
        parameters={'store_numbers': tuple(data['Store_Number'].drop_duplicates().tolist())}
    )
    logging.warning('FINISH: Fetch till suspension details')

    data = data.merge(
        till_suspended_details.drop_duplicates(),
        left_on='Store_Number',
        right_on='sn',
        how='left'
    )

    logging.warning('START: Fetch limit reasons')
    if model_version is not None:
        limit_reasons = warehouse_hook.get_pandas_df(
            sql="""
                select store_number as sn, proposed_summary_narration, communication_to_customer
                from bloomlive.scoring_results_view where model_version = %(model_version)s
                and store_number in %(store_numbers)s
            """,
            parameters={
                'store_numbers': tuple(data['Store_Number'].drop_duplicates().tolist()),
                'model_version': model_version
            }
        )
    else:
        limit_reasons = warehouse_hook.get_pandas_df(
            sql="""
                with rnked as (
                    select store_number as sn, proposed_summary_narration, communication_to_customer,
                    rank() over (partition by store_number order by model_version desc) rnk
                    from bloomlive.scoring_results_view
                ) select * from rnked where rnk = 1 and sn in %(store_numbers)s
            """,
            parameters={'store_numbers': tuple(data['Store_Number'].drop_duplicates().tolist())}
        )
    logging.warning('FINISH: Fetch limit reasons')
    data = data.merge(
        limit_reasons,
        left_on='Store_Number',
        right_on='sn',
        how='left'
    )

    return data


def insert_into_loans_fact_table(warehouse_schema: str, loans: pd.DataFrame, mifos_tenant: str):
    warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                                  schema='afsg_ds_prod_postgresql_dwh')
    # Remove unnecessary columns
    loans.drop(
        columns=['versions_match', 'client_id', 'product_id', 'loan_officer_id', 'loan_status_id'],
        inplace=True
    )

    if mifos_tenant == 'default':
        loans['bloom_version'] = '1'
    elif mifos_tenant == 'safaricom':
        loans['bloom_version'] = '2'
    elif mifos_tenant == 'bloom1restructure':
        loans['bloom_version'] = 'bloom1_restructured'

    # INSERT NEW RECORDS
    warehouse_hook.insert_rows(
        table=f'{warehouse_schema}.loans_fact_table',
        target_fields=loans.reindex(sorted(loans.index)).columns.tolist(),
        replace=False,
        rows=tuple(loans.reindex(sorted(loans.index)).replace({np.NAN: None}).itertuples(index=False)),
        commit_every=1000
    )


def get_loans_fact_table_updates(loans, existing_loans, warehouse_schema) -> list:
    updates = []

    for index, row in loans[~loans['versions_match'].isna()].iterrows():
        expired_on = str(row['record_created_on_date'])
        surrogate_id = int(
            existing_loans[existing_loans['loan_mifos_id'] == row['mifos_id']].iloc[0][
                'loan_surrogate_id']
        )

        query = dedent(f"""
            UPDATE {warehouse_schema}.loans_fact_table SET record_expired_on_date = '{str(expired_on)}',
            is_most_recent_record = FALSE WHERE surrogate_id = {int(surrogate_id)}
        """)
        updates.append(query)

    return updates


def get_model_versions(loans: pd.DataFrame, existing_loans: pd.DataFrame) -> pd.DataFrame:
    """
    Assigns a model version to each loan in the loans DataFrame based on its disbursal date and
    the model versions used for similar loans in the past. Uses the existing_loans DataFrame to
    check if any loans already have a model version assigned to them.

    Args:
        loans (pd.DataFrame): A DataFrame containing the loans to be processed.
        existing_loans (pd.DataFrame): A DataFrame containing existing loans and their model versions.

    Returns:
        pd.DataFrame: The loans DataFrame with an additional column 'model_version' indicating the model version to use for each loan.
    """
    airflow_hook = MySqlHook(mysql_conn_id='mysql_airflow', schema='bloom_pipeline')

    # Assign model version to loans that already have it in the existing_loans DataFrame
    loans['model_version'] = loans.apply(
        lambda x: existing_loans[existing_loans['loan_mifos_id'] == x['mifos_id']].iloc[0]['model_version'] if x[
                                                                                                                   'mifos_id'] in
                                                                                                               existing_loans[
                                                                                                                   'loan_mifos_id'].tolist() else np.NAN,
        axis=1)

    # For loans without a model version, assign the latest available version based on disbursal date
    if loans['model_version'].isna().sum() > 0:
        # Get model versions and their end dates from a SQL database
        model_versions = airflow_hook.get_pandas_df(
            sql="""
                select model_version, end_date from bloom_pipeline.limit_refresh_push_summary
                order by end_date asc
            """
        )

        def get_mv(x):
            # Find the latest end date in the model versions list that is earlier than the loan's disbursal date
            end_date = model_versions[model_versions['end_date'].dt.date < x['disbursed_on_date']]['end_date'].max()
            # If a model version exists for the found end date, return it
            if model_versions[model_versions['end_date'] == end_date]['model_version'].shape[0] > 0:
                return model_versions[model_versions['end_date'] == end_date]['model_version'].iloc[0]

        # Assign the latest model version to loans without a version
        loans.loc[loans['model_version'].isna(), 'model_version'] = loans[loans['model_version'].isna()].apply(
            lambda x: get_mv(x),
            axis=1
        )

    return loans


def el_loans_fact_table(mifos_tenant: str, warehouse_schema: str, warehouse_filters=None) -> None:
    mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}')
    warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                                  schema='afsg_ds_prod_postgresql_dwh')

    """
    Extracts loans from MIFOS and load them into warehouse
    :param context: dictionary of predefined values
    :return: None
    """
    products_sql = f"SELECT mifos_id as product_mifos_id, surrogate_id as product_surrogate_id FROM {warehouse_schema}.product_dimension where is_most_recent_record"
    loan_statuses_sql = f"SELECT mifos_id as loan_status_mifos_id, surrogate_id as loan_status_surrogate_id FROM {warehouse_schema}.loan_status_dimension"
    loan_officers_sql = f"SELECT mifos_id as loan_officer_mifos_id, surrogate_id as loan_officer_surrogate_id FROM {warehouse_schema}.staff_dimension"
    if warehouse_filters is not None:
        products_sql += f" and {warehouse_filters}"
        loan_statuses_sql += f' where {warehouse_filters}'
        loan_officers_sql += f' where {warehouse_filters}'

    loans_count = mifos_hook.get_first(
        sql="SELECT COUNT(*) FROM m_loan"
    )
    products = warehouse_hook.get_pandas_df(sql=products_sql)
    loan_statuses = warehouse_hook.get_pandas_df(sql=loan_statuses_sql)
    loan_officers = warehouse_hook.get_pandas_df(sql=loan_officers_sql)

    chunk_size = 5000  # batch size to process per iteration

    for num in range(0, loans_count[0], chunk_size):
        # Get loans data from MIFOS
        all_loans = get_mifos_loans(
            num=num,
            chunk_size=chunk_size,
            mifos_tenant=mifos_tenant,
            warehouse_schema=warehouse_schema,
            products=products,
            loan_statuses=loan_statuses,
            loan_officers=loan_officers,
            warehouse_filters=warehouse_filters,
        )
        loans = all_loans['loans']
        existing_loans = all_loans['existing_loans']

        # Update loans fact table
        if loans.shape[0] > 0:
            # model version for bloom
            if warehouse_schema == 'bloomlive' and mifos_tenant == 'safaricom':
                loans = get_model_versions(loans.copy(), existing_loans)

            updates = get_loans_fact_table_updates(
                loans=loans, existing_loans=existing_loans, warehouse_schema=warehouse_schema
            )

            # Execute SQL statements to update the loans fact table
            if len(updates) > 0:
                warehouse_hook.run(sql=updates)

            insert_into_loans_fact_table(warehouse_schema=warehouse_schema, loans=loans, mifos_tenant=mifos_tenant)


def get_mifos_loans(num, chunk_size, mifos_tenant, warehouse_schema, products, loan_statuses, loan_officers,
                    warehouse_filters=None) -> dict:
    """
    Extracts loans from MIFOS and load them into warehouse
    :param context: dictionary of predefined values
    :return: None
    """
    mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}')
    warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh',
                                  schema='afsg_ds_prod_postgresql_dwh')

    loans = mifos_hook.get_pandas_df(
        sql="""
            SELECT 
            id, client_id, product_id, loan_officer_id, loan_status_id, 
            external_id, account_no, 
            loan_type_enum, loan_transaction_strategy_id, term_frequency, term_period_frequency_enum, repay_every, 
            principal_amount_proposed, principal_amount, principal_disbursed_derived, principal_repaid_derived, 
            principal_writtenoff_derived, principal_outstanding_derived, interest_charged_derived, 
            interest_repaid_derived, interest_waived_derived, interest_writtenoff_derived, 
            interest_outstanding_derived, fee_charges_charged_derived, fee_charges_repaid_derived, 
            fee_charges_waived_derived, fee_charges_writtenoff_derived, fee_charges_outstanding_derived, 
            penalty_charges_charged_derived, penalty_charges_repaid_derived , penalty_charges_waived_derived, 
            penalty_charges_writtenoff_derived, penalty_charges_outstanding_derived, total_expected_repayment_derived,
            total_repayment_derived, total_expected_costofloan_derived, total_costofloan_derived, 
            total_charges_due_at_disbursement_derived, total_waived_derived, total_writtenoff_derived, 
            total_outstanding_derived, total_overpaid_derived, total_recovered_derived, guarantee_amount_derived, 
            arrearstolerance_amount, interest_rate_differential, fixed_emi_amount, max_outstanding_loan_balance, 
            accrued_till, submittedon_date, approvedon_date, expected_disbursedon_date, disbursedon_date, 
            expected_maturedon_date, maturedon_date, expected_firstrepaymenton_date, closedon_date, 
            rescheduledon_date, rejectedon_date, withdrawnon_date, writtenoffon_date,interest_calculated_from_date, 
            interest_recalcualated_on, allow_partial_period_interest_calcualtion, is_floating_interest_rate, 
            sync_disbursement_with_meeting, interest_recalculation_enabled, create_standing_instruction_at_disbursement, 
            is_npa, is_topup, 
            loan_counter, loan_product_counter, grace_on_arrears_ageing, version, 
            submittedon_userid, 
            closedon_userid,rescheduledon_userid, withdrawnon_userid,disbursedon_userid, approvedon_userid, 
            rejectedon_userid, approved_principal, annual_nominal_interest_rate 
            FROM m_loan order by id desc LIMIT %(start_index)s, %(chunksize)s""",
        parameters={'start_index': num, 'chunksize': chunk_size}
    ).replace({None: np.NAN})

    clients_sql = f"""
        SELECT  mifos_id as client_mifos_id, surrogate_id as client_surrogate_id 
        FROM {warehouse_schema}.client_dimension 
        WHERE mifos_id in %(clients)s
    """

    if warehouse_filters is not None:
        clients_sql += f"and {warehouse_filters}"

    clients = warehouse_hook.get_pandas_df(sql=clients_sql, parameters={'clients': tuple(loans['client_id'].tolist())})

    loans = merge_loans_with_warehouse_keys(
        loans=loans.copy(),
        clients=clients,
        products=products,
        loan_statuses=loan_statuses,
        loan_officers=loan_officers,
        mifos_hook=mifos_hook
    )
    loans = loans[~loans['client_surrogate_id'].isna()]

    if loans.shape[0] > 0:
        if mifos_tenant == 'safaricom':
            warehouse_filters = f", model_version FROM {warehouse_schema}.loans_fact_table_materialized_summary_view WHERE bloom_version = '2' and loan_mifos_id in {tuple(loans['id'].tolist())}"
        elif mifos_tenant == 'default':
            warehouse_filters = f"FROM {warehouse_schema}.loans_fact_table_materialized_summary_view WHERE bloom_version = '1' and loan_mifos_id in {tuple(loans['id'].tolist())}"
        elif mifos_tenant == 'bloom1restructure':
            warehouse_filters = f"FROM {warehouse_schema}.loans_fact_table_materialized_summary_view WHERE bloom_version = 'bloom1_restructured' and loan_mifos_id in {tuple(loans['id'].tolist())}"
        else:
            warehouse_filters = f"FROM {warehouse_schema}.loans_fact_table_summary_view where loan_mifos_id in {tuple(loans['id'].tolist())}"

        existing_loans = warehouse_hook.get_pandas_df(
            sql="""
                SELECT
                loan_mifos_id, loan_surrogate_id, client_surrogate_id as client_key, 
                product_surrogate_id as product_key, loan_officer_surrogate_id as loan_officer_key, 
                loan_status_surrogate_id as status_key,
                external_id, loan_account_number as account_number, loan_type, transaction_strategy, term_frequency, 
                term_period_frequency, repay_every, principal_amount_proposed, principal_amount, 
                principal_disbursed, principal_repaid, principal_written_off,
                principal_outstanding, interest_charged, interest_repaid, interest_waived,
                interest_written_off, interest_outstanding, fee_charges_charged, fee_charges_repaid,
                fee_charges_waived, fee_charges_written_off, fee_charges_outstanding,
                penalty_charges_charged, penalty_charges_repaid, penalty_charges_waived, 
                penalty_charges_written_off, penalty_charges_outstanding, total_expected_repayment,
                total_repayment, total_expected_cost_of_loan, total_cost_of_loan, 
                total_charges_due_at_disbursement, total_waived, total_written_off,
                total_outstanding, total_overpaid, total_recovered, guarantee_amount,
                arrears_tolerance_amount, interest_rate_differential,
                fixed_emi_amount, max_outstanding_loan_balance, accrued_till, submitted_on_date,
                approved_on_date, expected_disbursed_on_date, disbursed_on_date, expected_matured_on_date,
                matured_on_date, expected_first_repayment_on_date, closed_on_date, rescheduled_on_date,
                rejected_on_date, withdrawn_on_date, written_off_on_date, interest_calculated_from_date, 
                interest_recalculated_on, allow_partial_period_interest_calculation, 
                is_floating_interest_rate, counter, product_counter,
                sync_disbursement_with_meeting, interest_recalculation_enabled,
                create_standing_instruction_at_disbursement, is_npa, rejected_on_user_id,
                is_topup, approved_principal, annual_nominal_interest_rate,
                grace_on_arrears_ageing, loan_version, submitted_on_user_id, closed_on_user_id,
                rescheduled_on_user_id, withdrawn_on_user_id, disbursed_on_user_id, approved_on_user_id
            """ + warehouse_filters
        ).replace({None: np.NAN})
        existing_loans['counter'] = existing_loans['counter'].apply(lambda x: int(x) if not pd.isnull(x) else x)

        loans = rename_loan_columns(loans.copy())
        loans['versions_match'] = loans.apply(
            lambda x: compare_loans(
                filtered_row=x.drop(labels=[
                    'mifos_id', 'client_id', 'product_id', 'loan_officer_id', 'loan_status_id',
                    'is_most_recent_record', 'record_created_on_date', 'record_expired_on_date'
                ]),
                prev_loan=existing_loans[
                    existing_loans['loan_mifos_id'] == x['mifos_id']].drop(
                    columns=['loan_mifos_id', 'loan_surrogate_id'] if mifos_tenant != 'safaricom' else ['loan_mifos_id',
                                                                                                        'loan_surrogate_id',
                                                                                                        'model_version']
                ).iloc[0]
            ) if x['mifos_id'] in existing_loans['loan_mifos_id'].tolist() else np.NAN,
            axis=1
        )
        loans = loans[~loans['versions_match'].isin([True])]
        return {'loans': loans, 'existing_loans': existing_loans}

    return {'loans': pd.DataFrame(), 'existing_loans': pd.DataFrame()}


def trigger_dag_remotely(dag_id: str, conf: dict):
    """
    Trigger a DAG remotely by sending a POST request to the Airflow API.

    param dag_id: The ID of the DAG to trigger, e.g. 'step_data_recon'.
    param conf: The configuration to pass to the DAG run, e.g. {"conf": {"tasks_to_run": dumps}}.
    """

    # Configure the time zone
    local_tz = pendulum.timezone("Africa/Nairobi")
    url = f'https://airflow.asantefsg.com/data-pipelines/api/v1/dags/{dag_id}/dagRuns'

    # Combine the default execution date with the given conf
    payload = {
        "execution_date": str(datetime.datetime.now().replace(tzinfo=local_tz)),
        "conf": conf
    }

    # Send the POST request to the Airflow API to trigger the DAG
    response = requests.post(
        url=url,
        headers={'Content-type': 'application/json', 'Accept': 'application/json'},
        json=payload,
        auth=requests.auth.HTTPBasicAuth(
            Variable.get('remote_trigger_username'), Variable.get('remote_trigger_password')),
        verify=False
    )

    # Check the response from the API and log any warnings
    logging.warning(response.text)
    if response.status_code != 200:
        response.raise_for_status()


def compare_loans(filtered_row, prev_loan):
    filtered_row = filtered_row.reindex(sorted(filtered_row.index))
    prev_loan = prev_loan.reindex(sorted(prev_loan.index))

    return filtered_row.equals(prev_loan)


def get_saf_disbursements(joining_method, where, warehouse_hook: PostgresHook):
    disbursements = warehouse_hook.get_pandas_df(
        sql="""
            select
                trxns.has_correct_cntrct_id, trxns.has_missing_fields,
                trxns.surrogate_id, trxns.id_date, trxns.id_trxn_linkd, cntrcts.src_prncpl_amnt,
                trxns.cd_mpsa_orga_shrt, trxns.ds_mpsa_enty_name, cntrcts.src_id_prdct,
                cntrcts.cntrct_id, cntrcts.src_id_idnty, trxns.maintnanc_fee, cntrcts.src_acces_fee
            from
            (
                with transactions_ranked as
                (
                    select *, rank() over (partition by id_date, id_trxn, src_term_loan, cd_mpsa_orga_shrt, trxn_stts, id_trxn_linkd, id_idnty, src_assgnd_crdt_lmt, src_used_crdt_lmit, src_avail_crdt_lmit order by surrogate_id) count_rank
                    from bloomlive.transactions_data_dump tdd
                    where is_disbursement is true and trxn_stts like '%Completed%'
                )
                select * from transactions_ranked where count_rank = 1
            ) as trxns left join
            (
                with contracts_ranked as
                (
                    select *, rank() over (partition by id_date, cntrct_id, src_id_idnty order by surrogate_id) count_rank
                    from bloomlive.contract_details_data_dump cddd
                )
                select * from contracts_ranked where count_rank = 1
            ) as cntrcts
            {}
            {}
        """.format(
            joining_method, where
        )
    )

    disbursements['rnk'] = disbursements.sort_values(by=['surrogate_id'], ascending=False).groupby([
        'id_date', 'id_trxn_linkd', 'cntrct_id', 'src_id_prdct', 'src_id_idnty',
        'src_prncpl_amnt'
    ]).cumcount() + 1

    return disbursements[disbursements['rnk'] == 1]


def cntrct_id_check_saf_disbursements_dump(warehouse_hook: PostgresHook):
    warehouse_hook.run(
        sql="""
            update bloomlive.transactions_data_dump set has_correct_cntrct_id = true
            where id_loan_cntrct is not null and is_disbursement is true
        """
    )

    by_provided_column = get_saf_disbursements(
        joining_method="on trxns.id_loan_cntrct = cntrcts.cntrct_id and trxns.id_idnty = cntrcts.src_id_idnty and cntrcts.src_acces_fee = trxns.maintnanc_fee",
        where="",
        warehouse_hook=warehouse_hook
    )[['id_trxn_linkd', 'cntrct_id', 'surrogate_id']]

    by_workaround = get_saf_disbursements(
        joining_method="on trxns.id_idnty = cntrcts.src_id_idnty and to_char(trxns.id_date, 'YYYYMMDD') = cntrcts.id_date and trxns.trxn_amnt = cntrcts.src_prncpl_amnt + cntrcts.src_acces_fee",
        where="",
        warehouse_hook=warehouse_hook
    )[['id_trxn_linkd', 'cntrct_id', 'surrogate_id']]

    result = by_provided_column.merge(
        by_workaround,
        left_on='id_trxn_linkd',
        right_on='id_trxn_linkd',
        how='inner'
    )

    updates = []
    updates2 = []

    result = result[~result['cntrct_id_y'].isna()]

    for index, row in result[result['cntrct_id_x'] != result['cntrct_id_y']][
        ['surrogate_id_x']].drop_duplicates().iterrows():
        updates.append(int(row['surrogate_id_x']))
    for index, row in result[result['cntrct_id_x'] == result['cntrct_id_y']][
        ['surrogate_id_x']].drop_duplicates().iterrows():
        updates2.append(int(row['surrogate_id_x']))

    warehouse_hook.run(
        sql="""
            update bloomlive.transactions_data_dump set has_correct_cntrct_id = false
            where id_loan_cntrct is null or surrogate_id in %(updates)s
        """,
        parameters={'updates': tuple(updates)}
    )
    warehouse_hook.run(
        sql="""
                update bloomlive.transactions_data_dump set has_correct_cntrct_id = true
                where surrogate_id in %(updates2)s
            """,
        parameters={'updates2': tuple(updates2)}
    )


def saf_dumps_remove_trailing_zeros(warehouse_hook: PostgresHook):
    import pandas as pd

    updates = []

    floats = warehouse_hook.get_pandas_df(
        sql="""
            select surrogate_id, src_lendr from bloomlive.transactions_data_dump tdd where src_lendr like '%.0' and is_disbursement is true
        """
    )
    floats2 = warehouse_hook.get_pandas_df(
        sql="""
            select id_date, surrogate_id from bloomlive.contract_details_data_dump cddd where id_date like '%.0'
        """
    )
    floats3 = warehouse_hook.get_pandas_df(
        sql="""
            select src_id_prdct, surrogate_id from bloomlive.contract_details_data_dump cddd where src_id_prdct like '%.0'
        """
    )

    floats['src_lendr'] = floats['src_lendr'].apply(lambda y: str(y).replace('.0', '') if not pd.isnull(y) else y)
    floats2['id_date'] = floats2['id_date'].apply(lambda y: str(y).replace('.0', '') if not pd.isnull(y) else y)
    floats3['src_id_prdct'] = floats3['src_id_prdct'].apply(
        lambda y: str(y).replace('.0', '') if not pd.isnull(y) else y)

    for index, x in floats.iterrows():
        updates.append(
            f"update bloomlive.transactions_data_dump set src_lendr = {x['src_lendr']} where surrogate_id = {x['surrogate_id']}"
        )
    for index, x in floats2.iterrows():
        updates.append(
            f"update bloomlive.contract_details_data_dump set id_date = {x['id_date']} where surrogate_id = {x['surrogate_id']}"
        )
    for index, x in floats3.iterrows():
        updates.append(
            f"update bloomlive.contract_details_data_dump set src_id_prdct = {x['src_id_prdct']} where surrogate_id = {x['surrogate_id']}"
        )
    if len(updates) > 0:
        warehouse_hook.run(sql=updates)
