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
        SELECT 
            mpl.id as id,
            mpl.name as name,
            mpl.short_name as short_name,
            mpl.description as description,
            mpl.currency_code as currency_code,
            mpl.currency_digits as currency_digits,
            mpl.currency_multiplesof as currency_multiplesof, 
            mpl.min_principal_amount as min_principal_amount, 
            mpl.max_principal_amount as max_principal_amount,
            mpl.nominal_interest_rate_per_period as nominal_interest_rate_per_period, 
            mpl.annual_nominal_interest_rate as annual_nominal_interest_rate, 
            mpl.repayment_period_frequency_enum as repayment_period_frequency_enum,
            mpl.amortization_method_enum as amortization_method_enum, 
            mpl.interest_period_frequency_enum as interest_period_frequency_enum, 
            mpl.interest_method_enum as interest_method_enum, 
            mpl.interest_calculated_in_period_enum as interest_calculated_in_period_enum,
            mpl.number_of_repayments as number_of_repayments, 
            mpl.grace_on_principal_periods as grace_on_principal_periods, 
            mpl.grace_on_interest_periods as grace_on_interest_periods, 
            mpl.grace_interest_free_periods as grace_interest_free_periods, 
            mpl.recurring_moratorium_principal_periods as recurring_moratorium_principal_periods,
            mpl.days_in_month_enum as days_in_month_enum, 
            mpl.days_in_year_enum as days_in_year_enum, 
            mpl.start_date as start_date, 
            mpl.close_date as close_date, 
            mpl.allow_partial_period_interest_calcualtion as allow_partial_period_interest_calcualtion, 
            mpl.repay_every as repay_every, 
            mpl.min_number_of_repayments as min_number_of_repayments, 
            mpl.grace_on_arrears_ageing as grace_on_arrears_ageing, 
            mpl.overdue_days_for_npa as overdue_days_for_npa,
            mf.name as solv_model
        FROM m_product_loan mpl
        LEFT JOIN m_fund mf on mpl.fund_id = mf.id
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
                    grace_on_arrears_ageing,overdue_days_for_npa, solv_model, surrogate_id
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
                            min_number_of_repayments, grace_on_arrears_ageing, overdue_days_for_npa, solv_model,
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
