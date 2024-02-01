import os
import sys
import pendulum
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import on_failure, el_loans_fact_table
from utils.ms_teams_webhook_operator import MSTeamsWebhookOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': on_failure,
}

local_tz = pendulum.timezone("Africa/Nairobi")

with DAG(
        'ETL_interswitch_ug_loans_fact_table',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        start_date=datetime.datetime(2023, 1, 17, 8, 56, tzinfo=local_tz),
        tags=['extract_load_fact_table'],
        description='Load data into interswitch_ug loans fact table',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ####DAG SUMMARY
    Extracts data from MIFOS and adds to ubuntu.interswitch_ug.loans_fact_table following SCD type 2
    DAG is set to run daily at 6pm.

    #### Actions
    <ol>
    <li>Extract from mifostenant-uganda.m_loan</li>
    <li>Check for existing loan instances in ubuntu.interswitch_ug.loans_fact_table</li>
    <li>Update existing instances in ubuntu.interswitch_ug.loans_fact_table</li>
    <li>Insert latest loan details in ubuntu.interswitch_ug.loans_fact_table</li>
    </ol>
    """

    def extract_load_loans_fact_table(**context) -> None:
        """
        Extracts loans from MIFOS and load them into warehouse
        :param context: dictionary of predefined values
        :return: None
        """

        el_loans_fact_table(mifos_tenant='uganda', warehouse_schema='interswitch_ug')

            # warehouse_hook.run(
            #     sql="""
            #         with rnked as (
            #             select surrogate_id, rank() over (partition by
            #             mifos_id, client_key, product_key, loan_officer_key, status_key, external_id, account_number,
            #             loan_type, transaction_strategy, term_frequency, term_period_frequency, repay_every, principal_amount_proposed,
            #             principal_amount, principal_disbursed, principal_repaid, principal_written_off, principal_outstanding, interest_charged,
            #             interest_repaid, interest_waived, interest_written_off, interest_outstanding, fee_charges_charged, fee_charges_repaid,
            #             fee_charges_waived, fee_charges_written_off, fee_charges_outstanding, penalty_charges_charged, penalty_charges_repaid,
            #             penalty_charges_waived, penalty_charges_written_off, penalty_charges_outstanding, total_expected_repayment, total_repayment,
            #             total_expected_cost_of_loan, total_cost_of_loan, total_charges_due_at_disbursement, total_waived, total_written_off,
            #             total_outstanding, total_overpaid, total_recovered, guarantee_amount, arrears_tolerance_amount, interest_rate_differential,
            #             fixed_emi_amount, max_outstanding_loan_balance, accrued_till, submitted_on_date, approved_on_date, expected_disbursed_on_date,
            #             disbursed_on_date, expected_matured_on_date, matured_on_date, expected_first_repayment_on_date, closed_on_date, rescheduled_on_date,
            #             rejected_on_date, withdrawn_on_date, written_off_on_date, interest_calculated_from_date, interest_recalculated_on, allow_partial_period_interest_calculation,
            #             is_floating_interest_rate, sync_disbursement_with_meeting, interest_recalculation_enabled, create_standing_instruction_at_disbursement, is_npa,
            #             is_topup, counter, product_counter, grace_on_arrears_ageing, loan_version, submitted_on_user_id, closed_on_user_id, rescheduled_on_user_id,
            #             withdrawn_on_user_id, disbursed_on_user_id, approved_on_user_id, rejected_on_user_id, is_most_recent_record, approved_principal,
            #             annual_nominal_interest_rate
            #             order by record_created_on_date asc
            #             ) rnk
            #             from interswitch_ug.loans_fact_table lft
            #         ) delete from interswitch_ug.loans_fact_table lft2 where surrogate_id in (select surrogate_id from rnked where rnk > 1);
            #     """
            # )

    def remove_duplicates(**context):
        from utils.common import remove_loan_facts_table_duplicates

        remove_loan_facts_table_duplicates(
            schema='interswitch_ug',
            filters=None
        )

    t1 = PythonOperator(
        task_id='remove_duplicates',
        provide_context=True,
        python_callable=remove_duplicates,
    )

    t2 = PythonOperator(
        task_id='extract_load_loans_fact_table',
        provide_context=True,
        python_callable=extract_load_loans_fact_table,
    )
    t3 = MSTeamsWebhookOperator(
        task_id='send_ms_teams_notification',
        http_conn_id='msteams_webhook_url',
        message="""Interswitch Uganda Pipeline""",
        subtitle="""Loans fact table has been refreshed""".strip(),
        dag=dag,
        retries=0
    )
    t1 >> t2 >> t3

