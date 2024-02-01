import re
import os
import sys
import logging
import datetime
import pendulum
import numpy as np
import pandas as pd
from airflow import DAG
from io import StringIO
from datetime import timedelta
from xml.etree import ElementTree
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import ast

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from utils.common import on_failure
from utils.transunion_api import TransUnionApi
from utils.office365_api import get_access_token, get_children, read_file, upload_file
import utils.AsanteJson as AsanteJson

AJSON = AsanteJson.JSON()

warehouse_hook = PostgresHook(postgres_conn_id='rds_afsg_ds_prod_postgresql_dwh', schema='afsg_ds_prod_postgresql_dwh')
airflow_hook = MySqlHook(mysql_conn_id='mysql_airflow', schema='monitoring')
log_format = "%(asctime)s: %(message)s"
logging.basicConfig(format=log_format, level=logging.WARNING, datefmt="%H:%M:%S")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    # 'on_failure_callback': on_failure,
}

local_tz = pendulum.timezone("Africa/Nairobi")

SPath = os.path.dirname(os.path.realpath(__file__))
mapping_file = os.path.join(SPath, 'Mapper.xlsx')

with DAG(
        'TransUnion_CRB_checks_Test',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        start_date=datetime.datetime(2023, 7, 28, 12, 00, tzinfo=local_tz),
        tags=['data_checks'],
        description='Conduct CRB checks for Bloom clients',
        user_defined_macros=default_args,
        max_active_runs=1
) as dag:
    # DOCS
    dag.doc_md = """
    ### DAG SUMMARY

    This pipeline retrieves Credit Reference Bureau data for a given dataset from TransUnion CRB.
    The DAG is triggered manually.
    To connect to TransUnion, the DAG makes use of connection details specified in airflow varibles    
    """


    def convert_dict_keys_to_snake_case(input_dict):
        if not isinstance(input_dict, dict):
            return input_dict  # If input is not a dictionary, return the value as is

        output_dict = {}
        for key, value in input_dict.items():
            value = convert_dict_keys_to_snake_case(value)  # Recursive call for nested dictionaries
            # Use regular expression to replace uppercase letters with underscores and lowercase letters
            renamed_key = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', key).lower()
            output_dict[renamed_key] = value
        return output_dict


    def remove_non_required_keys(d):
        keys_to_remove = set()
        for k in d.keys():
            is_parent = any(k != key and key.startswith(k) for key in d.keys())
            if is_parent:
                keys_to_remove.add(k)
        return keys_to_remove


    def remove_repeated_word(input_str):
        words = input_str.split("_")
        cleaned_words = [word for idx, word in enumerate(words) if word not in words[:idx]]
        cleaned_str = "_".join(cleaned_words)
        return cleaned_str


    def flatten_dict(d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)


    def rename_dict_keys(d):
        renamed_dict = {remove_repeated_word(k): v for k, v in d.items()}
        return renamed_dict


    def xml_to_dict(element):
        result = {}
        if element.text:
            result[element.tag] = element.text
        else:
            result[element.tag] = ''
        for child in element:
            child_result = xml_to_dict(child)
            if child.tag in result:
                if isinstance(result[child.tag], list):
                    result[child.tag].append(child_result)
                else:
                    result[child.tag] = [result[child.tag], child_result]
            else:
                result[child.tag] = child_result
        return result


    def store_results(file_name: str, unpacked_data: pd.DataFrame, product_name: str) -> None:
        # Split the file name and extension
        file_name, file_extension = file_name.rsplit('.', 1)
        csv_buffer = StringIO()
        unpacked_data.to_csv(csv_buffer, index=False)

        upload_file(file_name=f'{file_name}_out.csv', file_bytes=csv_buffer.getvalue(), site={
            'name': 'Safaricom Bloom',
            'id': 'ea835111-82a4-467d-81ed-3c5994f4d899',
            'folder': '01YAZIWUKGF6DSN3EJYJBINXVIT6ARZC35'}
                    )

        if product_name == 'product139':
            warehouse_hook.insert_rows(
                table='bloomlive.transunion_high_velocity_crb_checks',
                target_fields=unpacked_data.reindex(sorted(unpacked_data.index)).columns.tolist(),
                replace=True,
                rows=tuple(
                    unpacked_data.reindex(sorted(unpacked_data.index)).replace({np.NAN: None}).itertuples(index=False,
                                                                                                          name=None)),
                replace_index=['national_id', 'src'],
                commit_every=200
            )


    # Function to process column names
    def process_column_names(column_name):
        words = column_name.split(' ')  # Split by underscores
        unique_words = []  # To store unique words
        seen_words = set()  # To keep track of seen words
        for word in words:
            if word not in seen_words:
                unique_words.append(word)
                seen_words.add(word)
        return ' '.join(unique_words).title()  # Apply title case


    def insert_statements(table_name, columns, data_rows):
        for row in data_rows:
            # Properly format each value in the row for the SQL statement
            formatted_values = []
            for val in row:
                if isinstance(val, str):
                    # Handling single quotes in strings
                    formatted_val = "'" + val.replace("'", "''") + "'"
                else:
                    # Convert non-string values to string, handle None as SQL NULL
                    formatted_val = str(val) if val is not None else 'NULL'
                formatted_values.append(formatted_val)

            # Format column names to be enclosed in backticks
            formatted_columns = [f"`{col}`" for col in columns]

            # Join the formatted values and create the SQL statement
            values_str = ', '.join(formatted_values)
            sql = f"INSERT INTO {table_name} ({', '.join(formatted_columns)}) VALUES ({values_str});"

            airflow_hook.run(sql=sql)
    def get_unpacked_data(rslt_dict, product_name, req_columns: list):
        if rslt_dict['response_code'] == '200':
            rslt_dict = rslt_dict['{http://schemas.xmlsoap.org/soap/envelope/}Body'][
                '{http://ws.crbws.transunion.ke.co/}get%sResponse' % (product_name.capitalize())]['return']

            data = AJSON.json_to_dataframe(rslt_dict)
            data.columns = [process_column_names(col) for col in data.columns]

            # insert empty val if column is missing
            for column in req_columns:
                if column not in data.columns:
                    data[column] = np.nan

            data = data.astype(str)

            # Replace NaN with None and convert to tuples
            rows = tuple(data[req_columns].replace({np.nan: None}).itertuples(index=False, name=None))

            # Print the INSERT statements
            insert_statements(f'transunion.{product_name}', req_columns, rows)

            return data


    def get_results_dict(rslt, product_name):
        content_str = rslt['raw_response']
        # Parse the XML content using ElementTree
        root = ElementTree.fromstring(content_str)
        result_dict = xml_to_dict(root)
        result_dict['national_id'] = rslt['national_id']
        result_dict['timestamp'] = pd.to_datetime(rslt['timestamp'])
        result_dict['timestamp'] = result_dict['timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        # result_dict['timestamp'] = result_dict['timestamp'].isoformat()
        result_dict['raw_response'] = rslt['raw_response']
        result_dict['raw_request'] = rslt['raw_request']
        result_dict['response_code'] = result_dict['{http://schemas.xmlsoap.org/soap/envelope/}Body'][
            '{http://ws.crbws.transunion.ke.co/}get%sResponse' % (product_name.capitalize())]['return']['responseCode'][
            'responseCode']

        return result_dict


    def get_report(api: TransUnionApi, chunk: pd.DataFrame, product_name: str) -> list:
        results = []
        for i, r in chunk.iterrows():
            rslt = api.get_product(
                national_id=r['national_id'],
                first_name=r['first_name'],
                middle_name=r['middle_name'],
                surname=r['surname'],
                mobile_number=r['mobile_number'],
                product_name=product_name
            )
            results.append(get_results_dict(rslt=rslt, product_name=product_name))
        return results


    def flatten_lists(df: pd.DataFrame, product_name: str):
        if product_name == 'product103':
            # List of column names to process
            columns_to_process = ['phone_list', 'postal_address_list', 'physical_address_list']

            # Process each column
            for column_name in columns_to_process:
                for idx, row in df.iterrows():
                    data_list = row[column_name]
                    if isinstance(data_list, list):
                        for i, data in enumerate(data_list, start=1):
                            for key, value in data.items():
                                new_column_name = f'{column_name}{i}_{key}'
                                df.at[idx, new_column_name] = value
                    elif isinstance(data_list, dict):
                        for key, value in data_list.items():
                            new_column_name = f'{column_name}1_{key}'
                            df.at[idx, new_column_name] = value

            df.drop(columns=columns_to_process, inplace=True)


    def get_credit_report(**context):
        file_name = context['dag_run'].conf.get('file_name')
        product_name = context['dag_run'].conf.get('product_name')

        if product_name not in ('product139', 'product103', 'product109', 'product141', 'product131', 'product126'):
            raise ValueError("Invalid product name. Allowed values are 'product109' or 'product141'.")

        # read input file
        dt = pd.read_excel(r"/mnt/c/Users/Collins.Ogwena/Documents/asante/Bloom TU CRB Check Sample.xlsx")
        dt.drop_duplicates(subset=['national_id'], inplace=True)  # drop duplicates
        dt['national_id'] = dt['national_id'].astype(str)
        dt = dt[dt['national_id'].str.contains(r'^[0-9\.]+$') & ~(dt['national_id'].str.len() > 8)]
        dt['national_id'] = dt['national_id'].astype(int)
        print('Records: ' + str(len(dt['national_id'])))

        dt = dt.head(1)

        if dt.shape[0] > 0:
            api = TransUnionApi(env='prod')
            dt['result_dict'] = np.NAN
            response_code_mapping = {
                '101': 'General Authentication Error', '102': 'Invalid Infinity Code',
                '103': 'Invalid Authentication Credentials', '104': 'Password expired',
                '106': 'Access Denied', '109': 'Account locked',
                '200': 'Product request processed successfully',
                '202': 'Credit Reference Number not found',
                '203': 'Multiple Credit Reference Number Found',
                '204': 'Invalid report reason', '209': 'Invalid Sector ID',
                '301': 'Insufficient Credit', '402': 'Required input missing',
                '403': 'General Application Error', '404': 'Service temporarily unavailable',
                '408': 'Unable to verify National ID'
            }

            chunk = dt.iloc[0:]
            results = get_report(api=api, chunk=chunk, product_name=product_name)

            results = pd.DataFrame(results)
            results['failure_reason'] = results['response_code'].apply(
                lambda x: response_code_mapping[x] if x != '200' else None
            )
            results['menu'] = product_name
            results['src'] = 'test'

            target_data = results[[
                'national_id', 'timestamp', 'raw_request', 'raw_response', 'response_code', 'failure_reason',
                'menu', 'src'
            ]]

            unpack_raw_responses(target_data, product_name)

        else:
            logging.warning("No national IDS to check")


    def unpack_raw_responses(results, product_name):

        results = [get_results_dict(r, product_name=product_name) for i, r in results.iterrows()]

        mapper = pd.read_excel(mapping_file)
        matching_rows = mapper[mapper['product'] == product_name]

        if not matching_rows.empty:
            target_columns = matching_rows.iloc[0]['columns']
        else:
            raise ValueError("No data columns matching rows found")

        unpacked_data = pd.concat(
            [x for x in [get_unpacked_data(r, product_name, ast.literal_eval(target_columns)) for r in results] if x is not None])

        unpacked_data = unpacked_data.drop_duplicates()
        unpacked_data.replace('', pd.NA, inplace=True)
        unpacked_data.dropna(axis=1, how='all', inplace=True)

        if unpacked_data.shape[0] > 0:
            unpacked_data.to_csv(f'/mnt/c/Users/Collins.Ogwena/Documents/bloom2-{product_name}-transunion_export.csv', index=False)

        # store_results(
        #     file_name=file_name,
        #     unpacked_data=unpacked_data,
        #     product_name=product_name
        # )


    t1 = PythonOperator(
        task_id='get_credit_report',
        provide_context=True,
        python_callable=get_credit_report,
    )
    # t2 = PythonOperator(
    #     task_id='unpack_raw_responses',
    #     provide_context=True,
    #     python_callable=unpack_raw_responses,
    # )
    # t1 >> t2
    t1
