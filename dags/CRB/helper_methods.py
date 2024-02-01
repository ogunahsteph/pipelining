from utils.office365_api import get_access_token, read_file, get_children
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
from utils.office365_api import upload_file
from airflow.providers.mysql.hooks.mysql import MySqlHook
import io
import time

columns = ['Surname', 'Forename 1', 'Forename 2', 'Forename 3', 'Trading As',
           'Date of Birth', 'Client Number', 'Account Number', 'Gender',
           'Nationality', 'Primary Identification Document Type',
           'Primary Identification Document Number', 'Mobile Phone Number',
           'Account Product Type', 'Instalment Due Date', 'Original Amount',
           'Currency of Facility', 'Current Balance in Kenya Shillings',
           'Current Balance', 'Overdue Balance', 'Overdue Date', 'Number of Days in Arrears',
           'Number of Instalments in Arrears', 'Prudential Risk Classification',
           'Account Status', 'Account Status Date', 'Repayment Period',
           'Disbursement Date', 'Instalment Amount', 'Date of Latest Payment',
           'Last Payment Amount']

current_date = datetime.now().strftime('%Y%m%d')


def exceptions_file_data(product):
    """"""

    payload = {
        'site': {'name': 'CRB Data Dumps', 'id': '578a997f-f196-4c0f-a17c-fc328661aae6'},
        'folders': [
            {'name': 'Upload Files',
             'id': '01MSWQZ7YV4T3QHJBX6ZCIYROH5VEGDUOK'},
            {'name': 'Pipeline Files',
             'id': '01MSWQZ76TRXWUUGP6LNC23GEMXIQDWU2O'},
            {'name': 'Exception Files',
             'id': '01MSWQZ7ZSQ3Y4BNXGWNDZN6M3I2OC4D2N'}
        ]
    }

    # Get access token to authenticate the API call and get list of files in the folder
    access_token = get_access_token()
    files = get_children(token=access_token, site_id=payload['site']['id'],
                         item_id=payload['folders'][1]['id'])
    data = pd.read_excel(
        read_file(site=payload['site']['id'], file_id=files[0]['id'], access_token=access_token), sheet_name=product,
        engine="openpyxl")

    return data.iloc[:, 0].to_list()


def upload_exceptions_data(missing_data_df, file_name):
    """Uploads file to exceptions folder"""
    site = {
        'name': 'CRB Data Dumps',
        'id': '578a997f-f196-4c0f-a17c-fc328661aae6',
        'folder_name': 'Exception Files',
        'folder': '01MSWQZ7ZSQ3Y4BNXGWNDZN6M3I2OC4D2N'
    }

    exceptions_excel_buffer = io.BytesIO()

    try:
        missing_data_df.to_excel(exceptions_excel_buffer, index=False, columns=columns)
    except:
        missing_data_df.to_excel(exceptions_excel_buffer, index=False)

    upload_file(
        file_name=file_name,
        file_bytes=exceptions_excel_buffer.getvalue(),
        site=site)

    return True


def upload_crb_data(export_data, product):
    """Uploads file to exceptions folder"""
    site = {
        'name': 'CRB Data Dumps',
        'id': '578a997f-f196-4c0f-a17c-fc328661aae6',
        'folder_name': 'Upload Files',
        'folder': '01MSWQZ7YV4T3QHJBX6ZCIYROH5VEGDUOK'
    }

    excel_buffer = io.BytesIO()
    export_data.to_excel(excel_buffer, index=False, columns=columns)
    upload_file(file_name=f'CRB-{str(product).title()}-{str(current_date)}.xlsx',
                file_bytes=excel_buffer.getvalue(),
                site=site)

    return True


def calculate_age(date_of_birth):
    today = datetime.now().date()
    date_format = '%Y-%m-%d' if ' ' not in str(date_of_birth) else '%Y-%m-%d %H:%M:%S'

    birth_date = datetime.strptime(str(date_of_birth), date_format).date()
    age = (today - birth_date).days // 365
    return age


def data_cleaning(export_data, product, mapped_columns):
    if product in ['jumia', 'jubilee', 'pronto', 'solv', 'tanda']:
        required_cols = ['mobile_number', 'national_id', 'middle_name', 'first_name', 'last_name',
                         'date_of_birth', 'loan_mifos_id', 'principal_amount', 'disbursed_on_date',
                         'client_mifos_id'
                         ]
    else:
        required_cols = ['mobile_number', 'national_id', 'iprs_surname', 'iprs_first_name', 'iprs_other_name',
                         'idm_date_of_birth', 'loan_mifos_id', 'principal_amount', 'disbursed_on_date',
                         'client_mifos_id'
                         ]
    export_data.rename(columns={'disbursedon_date': 'disbursed_on_date'}, inplace=True)

    required_cols.extend(mapped_columns)
    export_data = export_data[required_cols]

    export_data.rename(columns={'mobile_number': 'Mobile Phone Number', 'iprs_surname': 'Surname',
                                'iprs_first_name': 'Forename 1', 'iprs_other_name': 'Forename 2',
                                'middle_name': 'Forename 2', 'first_name': 'Forename 1', 'last_name': 'Surname',
                                'date_of_birth': 'Date of Birth',
                                'Forename 3': 'Surname', 'idm_date_of_birth': 'Date of Birth',
                                'national_id': 'Primary Identification Document Number',
                                'loan_mifos_id': 'Account Number', 'principal_amount': 'Original Amount',
                                'disbursed_on_date': 'Disbursement Date', 'client_mifos_id': 'Client Number'},
                       inplace=True)
    export_data['Account Product Type'] = 'I'
    export_data['Trading As'] = np.nan
    export_data['Nationality'] = 'KE'
    export_data['Currency of Facility'] = 'KES'
    export_data['Current Balance'] = export_data['Current Balance in Kenya Shillings']
    export_data['Surname'] = export_data['Surname'].replace('', np.nan)

    # Fill missing values and single-character values in 'Surname' with values from 'Forename 2'
    mask = export_data['Forename 2'].str.len() == 1
    export_data.loc[mask, 'Forename 2'] = np.nan

    export_data['Surname'] = export_data.apply(
        lambda row: row['Forename 2'] if pd.isna(row['Surname']) or len(str(row['Surname'])) <= 1 else row[
            'Surname'], axis=1)
    export_data['Forename 3'] = export_data['Surname']

    #If overdue balance is greater than 0 then current balance must be greater than or equal to overdue balance
    export_data['Overdue Balance'] = export_data.apply(
        lambda row: row['Current Balance']
        if row['Overdue Balance'] > row['Current Balance']
        else row['Overdue Balance'],
        axis=1
    )

    #If overdue balance is greater than 0 and overdue date is NA use Account Status Date as overdue date
    export_data['Overdue Date'] = export_data.apply(
        lambda row: row['Account Status Date'] if row['Overdue Balance'] > 0 and pd.isna(row['Overdue Date']) else row[
            'Overdue Date'],
        axis=1
    )

    # Original Amount < 1000 Exception
    amounts_exception = export_data[(export_data['Original Amount'] < 1000) & (export_data['Current Balance'] != 0)]
    export_data = export_data[
        (export_data['Original Amount'] >= 1000) | (export_data['Current Balance'] == 0)]

    # convert Original Amount value to TU
    export_data['Original Amount'] = export_data['Original Amount'].astype(str)
    export_data['Original Amount'] = export_data['Original Amount'].apply(
        lambda x: x if x == '' else f"{float(x):.2f}")
    export_data['Original Amount'] = export_data['Original Amount'].apply(
        lambda x: x.replace('.', '') if '.' in x else x + '00')

    export_data['Current Balance in Kenya Shillings'] = export_data['Current Balance in Kenya Shillings'].astype(
        str)
    export_data['Current Balance in Kenya Shillings'] = export_data['Current Balance in Kenya Shillings'].apply(
        lambda x: x if x == '' else f"{float(x):.2f}")
    export_data['Current Balance in Kenya Shillings'] = export_data['Current Balance in Kenya Shillings'].apply(
        lambda x: x.replace('.', '') if '.' in x else x + '00')

    export_data['Current Balance'] = export_data['Current Balance'].astype(str)
    export_data['Current Balance'] = export_data['Current Balance'].apply(
        lambda x: x if x == '' else f"{float(x):.2f}")
    export_data['Current Balance'] = export_data['Current Balance'].apply(
        lambda x: x.replace('.', '') if '.' in x else x + '00')

    export_data['Overdue Balance'] = export_data['Overdue Balance'].astype(str)
    export_data['Overdue Balance'] = export_data['Overdue Balance'].apply(
        lambda x: x if x == '' else f"{float(x):.2f}")
    export_data['Overdue Balance'] = export_data['Overdue Balance'].apply(
        lambda x: x.replace('.', '') if '.' in x else x + '00')

    export_data['Instalment Amount'] = export_data['Instalment Amount'].astype(str)
    export_data['Instalment Amount'] = export_data['Instalment Amount'].apply(
        lambda x: x if x == '' else f"{float(x):.2f}")
    export_data['Instalment Amount'] = export_data['Instalment Amount'].apply(
        lambda x: x.replace('.', '') if '.' in x else x + '00')

    export_data['Last Payment Amount'] = export_data['Last Payment Amount'].astype(str)
    export_data['Last Payment Amount'] = export_data['Last Payment Amount'].apply(
        lambda x: x if x == '' else f"{float(x):.2f}")
    export_data['Last Payment Amount'] = export_data['Last Payment Amount'].apply(
        lambda x: x if x == '' else (x.replace('.', '') if '.' in x else x + '00')
    )

    # drop test accounts(with 0 as original amounts)
    export_data = export_data[export_data['Original Amount'] != '000']

    # if alphabet exists in ID Number
    export_data['Primary Identification Document Type'] = export_data[
        'Primary Identification Document Number'].apply(
        lambda x: '002' if x and any(c.isalpha() for c in x) else '001')

    # remove special characters if national ID(001)
    try:
        export_data['Primary Identification Document Number'] = export_data.apply(
            lambda row: ''.join(c for c in row['Primary Identification Document Number'] if c.isalnum())
            if row['Primary Identification Document Type'] == '001' else row['Primary Identification Document Number'],
            axis=1)
    except:
        pass

    try:
        export_data['Disbursement Date'] = export_data['Disbursement Date'].apply(
            lambda x: pd.to_datetime(x, format='%Y-%m-%d'))
        export_data['Disbursement Date'] = export_data['Disbursement Date'].dt.strftime('%Y%m%d')
    except:
        pass

    try:
        export_data['Date of Birth'] = pd.to_datetime(
            export_data['Date of Birth'], format='%Y-%m-%d', errors='coerce').fillna('1970-10-10')

        export_data['Date of Birth'] = export_data['Date of Birth'].apply(
            lambda dob: '1970-10-10' if calculate_age(dob) < 18 or calculate_age(dob) > 99 else dob)

        export_data['Date of Birth'] = export_data['Date of Birth'].dt.strftime('%Y%m%d')
    except:
        pass

    # populate account status date
    export_data['Account Status Date'].fillna(export_data['Overdue Date'], inplace=True)
    export_data['Account Status Date'].fillna(export_data['Disbursement Date'], inplace=True)

    # Data clean Up for names to keep only letters in names
    export_data['Surname'] = export_data['Surname'].apply(
        lambda text: ''.join(filter(str.isalpha, str(text))).title() if text is not None else None)
    export_data['Forename 1'] = export_data['Forename 1'].apply(
        lambda text: ''.join(filter(str.isalpha, str(text))).title() if text is not None else None)
    export_data['Forename 2'] = export_data['Forename 2'].apply(
        lambda text: ''.join(filter(str.isalpha, str(text))).title() if text is not None else None)
    export_data['Forename 3'] = export_data['Forename 3'].apply(
        lambda text: ''.join(filter(str.isalpha, str(text))).title() if text is not None else None)

    # replace any Nan strings for Forename2
    export_data['Forename 2'] = export_data['Forename 2'].replace("Nan", np.nan)

    # additional checks on classification and account status
    export_data = export_data.drop_duplicates()

    # Save the DataFrame to a CSV file
    mandatory_columns = ['Surname', 'Forename 1', 'Date of Birth', 'Account Number', 'Gender',
                         'Nationality', 'Primary Identification Document Type',
                         'Primary Identification Document Number', 'Mobile Phone Number',
                         'Account Product Type', 'Original Amount',
                         'Currency of Facility', 'Current Balance in Kenya Shillings',
                         'Current Balance', 'Overdue Balance', 'Number of Days in Arrears',
                         'Number of Instalments in Arrears', 'Prudential Risk Classification',
                         'Account Status', 'Account Status Date', 'Repayment Period',
                         'Disbursement Date', 'Last Payment Amount']

    missing_data_dict = {}
    for column in mandatory_columns:
        missing_data_df = export_data[export_data[column].isna() & (export_data['Current Balance'] != '000')]

        # Add the DataFrame to the dictionary
        missing_data_dict[column] = missing_data_df

    export_data = export_data.dropna(subset=mandatory_columns)

    # phone number with underscores
    export_data['Mobile Phone Number'] = export_data['Mobile Phone Number'].astype(str).apply(lambda x: x.split('_')[0])

    return export_data, amounts_exception, missing_data_dict


def get_repayment_info(loan_id, mifos_tenant: str):
    mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}')

    attempts = 0
    while attempts < 5:
        try:
            result = mifos_hook.get_pandas_df(f"""
                        SELECT *
                        FROM m_loan_transaction
                        WHERE loan_id = {loan_id}
                        AND transaction_type_enum = 2
                        ORDER BY id DESC
                        LIMIT 1
                    """)
            break
        except:
            attempts += 1
            time.sleep(120)

    return result


def get_arrears_info(loan_id, mifos_tenant: str):
    mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}')

    attempts = 0
    while attempts < 5:
        try:
            result = mifos_hook.get_pandas_df(f"""
                SELECT * FROM m_loan_arrears_aging WHERE loan_id={loan_id}
            """)
            break
        except:
            attempts += 1
            time.sleep(120)

    return result


def get_outstanding_loan_balance(product, loan_info):
    today = datetime.now().date()
    cutoff_date = today - timedelta(days=2)

    if product == "bloom2":
        try:
            loan_balance_date = datetime.strptime(str(loan_info['safaricom_loan_balance_date']).split()[0],
                                                  '%Y-%m-%d').date()
            if loan_balance_date > cutoff_date or loan_info['safaricom_loan_balance'] == 0:
                outstanding_loan_balance = loan_info['safaricom_loan_balance']
            else:
                outstanding_loan_balance = loan_info['total_outstanding']
        except:
            outstanding_loan_balance = loan_info['total_outstanding']
    else:
        outstanding_loan_balance = loan_info['total_outstanding']

    if outstanding_loan_balance < 0:
        outstanding_loan_balance = 0

    return outstanding_loan_balance


def get_installment(loan_id, mifos_tenant: str):
    mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}')

    attempts = 0
    while attempts < 5:
        try:
            result = mifos_hook.get_pandas_df(f"""
                SELECT * FROM m_loan_repayment_schedule WHERE loan_id={loan_id} LIMIT 1
            """)
            break
        except:
            attempts += 1
            time.sleep(120)

    return result


def get_next_installment(loan_id, mifos_tenant: str):
    mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}')

    attempts = 0
    while attempts < 5:
        try:
            result = mifos_hook.get_pandas_df(f"""
                SELECT * FROM m_loan_repayment_schedule 
                WHERE loan_id={loan_id} AND completed_derived=0 
                ORDER BY installment ASC LIMIT 1
            """)
            break
        except:
            attempts += 1
            time.sleep(120)

    return result


def get_no_of_days_in_arrears(arrears_info):
    today = datetime.now().date()

    if arrears_info is not None:
        last_arrears_date = datetime.strptime(str(arrears_info['overdue_since_date_derived'][0]), '%Y-%m-%d').date()
        days_difference = (today - last_arrears_date).days

        if days_difference > 0:
            return days_difference
        else:
            return 0
    else:
        return 0


def get_no_of_installment_in_arrears(loan_id, mifos_tenant: str):
    mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}')
    today = datetime.now().date()

    attempts = 0
    while attempts < 5:
        try:
            result = mifos_hook.get_pandas_df(f"""
                SELECT count(*) AS count
                FROM m_loan_repayment_schedule
                WHERE loan_id = {loan_id}
                AND completed_derived = 0
                AND duedate <= '{str(today)}'
            """)
            break
        except:
            attempts += 1
            time.sleep(120)

    return result.iloc[0]['count']


def get_account_status_date(loan_info, arrears_info, is_rescheduled):
    status_date = np.nan

    if loan_info['loan_status'] == 300:
        status_date = loan_info['disbursed_on_date']
    elif loan_info['loan_status'] in [500, 600, 700]:
        if str(loan_info['closed_on_date']) == "" or str(loan_info['closed_on_date']) == "None":
            status_date = loan_info['disbursed_on_date']
        else:
            status_date = loan_info['closed_on_date']

    if arrears_info is not None:
        status_date = arrears_info['overdue_since_date_derived']

    if is_rescheduled:
        status_date = loan_info['disbursed_on_date']

    return status_date


def get_classification(no_of_days_defaulted):
    classification = np.nan

    if no_of_days_defaulted == 0:
        classification = "A"
    elif 0 < no_of_days_defaulted <= 30:
        classification = "B"
    elif 30 < no_of_days_defaulted <= 60:
        classification = "C"
    elif 60 < no_of_days_defaulted <= 90:
        classification = "D"
    elif no_of_days_defaulted > 90:
        classification = "E"

    return classification


def translate_gender(client_info):
    gender = np.nan
    if client_info['gender'] == "Male":
        gender = "M"
    elif client_info['gender'] == "Female":
        gender = "F"

    try:
        if client_info['idm_gender'] == "Male":
            gender = "M"
        elif client_info['idm_gender'] == "Female":
            gender = "F"
    except:
        pass

    return gender


def get_term_duration(loan_id, mifos_tenant: str):
    mifos_hook = MySqlHook(mysql_conn_id='mifos_db', schema=f'mifostenant-{mifos_tenant}')

    attempts = 0
    while attempts < 5:
        try:
            result = mifos_hook.get_pandas_df(f"""
                SELECT * FROM m_loan WHERE id = {loan_id}
            """)
            break
        except:
            attempts += 1
            time.sleep(120)

    term_duration = np.nan
    if not result['term_period_frequency_enum'].empty:
        if result['term_period_frequency_enum'].iloc[0] == 0:
            term_duration = 1
        elif result['term_period_frequency_enum'].iloc[0] == 1:
            term_duration = 1
        elif result['term_period_frequency_enum'].iloc[0] == 2:
            term_duration = result['term_frequency'].iloc[0]

    # 3 month restructure loan
    if mifos_tenant == 'bloom1restructure':
        term_duration = 3

    return term_duration


def get_account_status(loan_info, is_rescheduled, outstanding_loan_balance, no_of_days_defaulted):
    status = np.nan

    if loan_info['loan_status'] == 300:
        if is_rescheduled:
            status = 'G'
        elif no_of_days_defaulted > 60:
            status = 'E'
        else:
            status = 'F'
    elif loan_info['loan_status'] in [600, 700]:
        if outstanding_loan_balance > 0 or outstanding_loan_balance > 0.0:
            status = 'F'
        else:
            status = 'A'

    if outstanding_loan_balance == 0 or outstanding_loan_balance == 0.0:
        status = 'A'

    return status


def process_data(loan_data, product: str, mifos_tenant: str):
    export_data = []

    repayment_info = get_repayment_info(loan_data['loan_mifos_id'], mifos_tenant)
    arrears_info = get_arrears_info(loan_data['loan_mifos_id'], mifos_tenant)
    outstanding_loan_balance = get_outstanding_loan_balance(product, loan_data)
    is_rescheduled = product == 'bloom1-restructured'

    installment = get_installment(loan_data['loan_mifos_id'], mifos_tenant)
    if not installment.empty:
        # Define the values to fill NaN for each column
        fill_values = {'principal_amount': 0,
                       'interest_amount': 0,
                       'fee_charges_amount': 0,
                       'penalty_charges_amount': 0}

        # Fill NaN values in specific columns
        installment.fillna(fill_values, inplace=True)
        installment_amount = (installment['principal_amount'] +
                              installment['interest_amount'] +
                              installment['fee_charges_amount'] +
                              installment['penalty_charges_amount'])[0]
    else:
        installment_amount = 0

    next_installment = get_next_installment(loan_data['loan_mifos_id'], mifos_tenant)
    if not next_installment.empty:
        next_installment_date = next_installment['duedate'][0]
    else:
        next_installment_date = loan_data['expected_matured_on_date']

    if next_installment_date is not None:
        next_installment_date = datetime.strptime(str(next_installment_date), '%Y-%m-%d').strftime('%Y%m%d')
    else:
        next_installment_date = np.nan

    if not repayment_info.empty:
        last_payment_date = datetime.strptime(str(repayment_info['transaction_date'][0]), '%Y-%m-%d').strftime(
            '%Y%m%d')
        last_payment_amount = int(repayment_info['amount'])
    else:
        last_payment_date = np.nan
        last_payment_amount = 0

    if not arrears_info.empty:
        no_of_days_in_arrears = get_no_of_days_in_arrears(arrears_info)
        amount_in_arrears = arrears_info['total_overdue_derived'][0]
    else:
        no_of_days_in_arrears = 0
        amount_in_arrears = 0

    no_of_installments_in_arrears = get_no_of_installment_in_arrears(loan_data['loan_mifos_id'], mifos_tenant)

    if int(no_of_days_in_arrears) > 0 or int(amount_in_arrears) > 0:
        over_due_date = datetime.strptime(str(arrears_info['overdue_since_date_derived'][0]),
                                          '%Y-%m-%d').strftime('%Y%m%d')
    else:
        over_due_date = np.nan

    if outstanding_loan_balance == 0:
        arrears_info = None
        no_of_days_in_arrears = 0
        amount_in_arrears = 0
        no_of_installments_in_arrears = 0
        over_due_date = np.nan

    try:
        account_status_date = datetime.strptime(
            str(get_account_status_date(loan_data, arrears_info, is_rescheduled)),
            '%Y-%m-%d').strftime('%Y%m%d')
    except:
        account_status_date = np.nan

    classification = get_classification(no_of_days_in_arrears)

    gender = translate_gender(loan_data)
    client_data = loan_data.values.tolist()

    push_response_array = [str(get_term_duration(loan_data['loan_mifos_id'], mifos_tenant)), gender,
                           next_installment_date, amount_in_arrears, no_of_days_in_arrears,
                           no_of_installments_in_arrears, over_due_date, classification,
                           get_account_status(loan_data, is_rescheduled, outstanding_loan_balance,
                                              no_of_days_in_arrears),
                           account_status_date, outstanding_loan_balance, installment_amount, last_payment_date,
                           last_payment_amount
                           ]
    client_data.extend(push_response_array)

    export_data.append(client_data)

    return export_data
