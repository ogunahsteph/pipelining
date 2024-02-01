import io
import logging
import time

import pandas as pd
import requests
import json
from airflow.models import Variable


def get_token():
    res = requests.post(
        url='https://data.asante.helaplus.com/api/session/',
        headers={'Content_Type': 'application/json'},
        json={
            'username': 'henry.mburu@asantefinancegroup.com',
            'password': Variable.get('metabase_password')
        },
    )
    try:
        return json.loads(res.text)['id']
    except json.decoder.JSONDecodeError as e:
        logging.warning(f'ERROR\n-----------------------------------------------\n{res.text}\n-----------------------------------------------')
        raise e


def get_user():
    user = requests.get(
        url='https://data.asante.helaplus.com/api/user/current',
        headers={'Content_Type': 'application/json', "X-Metabase-Session": get_token()},
    )
    return json.loads(user.text)


def get_all_databases():
    dbs = requests.get(
        url='https://data.asante.helaplus.com/api/database/',
        headers={'Content_Type': 'application/json', "X-Metabase-Session": get_token()},
    )
    return json.loads(dbs.text)

def get_database(db_id):
    db = requests.get(
        url=f'https://data.asante.helaplus.com/api/database/{db_id}/schema/',
        headers={'Content_Type': 'application/json', "X-Metabase-Session": get_token()},
    )
    return json.loads(db.text)


def get_question():
    questions = {
        'daily_data': '5b66f89b-b0c3-4244-b485-af1691be10be',
        'last_3_months': '4a4fa972-32ed-4cc1-ad62-58d2e22af508',
        'last_6_months': 'c66cc2c7-0094-43a7-9c6c-50443d89ea91'
    }

    data = json.loads(requests.get(
        url=f'https://data.asante.helaplus.com/api/public/card/{questions["last_6_months"]}/query',
        headers={'Content_Type': 'application/json', "X-Metabase-Session": get_token()},
        timeout=1800
    ).text)
    return data

def get_dataset(query: str) -> pd.DataFrame:
    payload = {
        'query': json.dumps({
            "database": 2,
            "native": {"query": query},
            "type": "native",
            "middleware": {"js-int-to-string?": True, "add-default-userland-constraints?": True}}
        ),
        'visualization_settings': json.dumps({"column_settings": {}, "table.pivot": False, "table.pivot_column": "comments", "table.cell_column": "amount", "table.columns": [{"name": "account_no", "fieldRef": ["field", "account_no", {"base-type": "type/Text"}],"enabled": True},{"name": "amount", "fieldRef": ["field", "amount", {"base-type": "type/Float"}],"enabled": True},{"name": "balance_after", "fieldRef": ["field", "balance_after", {"base-type": "type/Text"}],"enabled": True},{"name": "client_name", "fieldRef": ["field", "client_name", {"base-type": "type/Text"}],"enabled": True},{"name": "comments", "fieldRef": ["field", "comments", {"base-type": "type/Text"}],"enabled": True}, {"name": "created_at", "fieldRef": ["field", "created_at", {"base-type": "type/DateTimeWithLocalTZ"}], "enabled": True},{"name": "id", "fieldRef": ["field", "id", {"base-type": "type/Integer"}], "enabled": True},{"name": "phone", "fieldRef": ["field", "phone", {"base-type": "type/Text"}], "enabled": True},{"name": "status", "fieldRef": ["field", "status", {"base-type": "type/Integer"}],"enabled": True},{"name": "store_number", "fieldRef": ["field", "store_number", {"base-type": "type/Text"}],"enabled": True},{"name": "transaction_id", "fieldRef": ["field", "transaction_id", {"base-type": "type/Text"}],"enabled": True}, {"name": "transaction_time","fieldRef": ["field", "transaction_time", {"base-type": "type/Text"}],"enabled": True},{"name": "type", "fieldRef": ["field", "type", {"base-type": "type/Text"}], "enabled": True},{"name": "updated_at","fieldRef": ["field", "updated_at", {"base-type": "type/DateTimeWithLocalTZ"}],"enabled": True}], "table.column_formatting": []})
    }
    db = requests.post(
        url=f'https://data.asante.helaplus.com/api/dataset/csv',
        headers={
            'Content_Type': 'application/x-www-form-urlencoded',
            "X-Metabase-Session": get_token(),
        },
        data=payload
    )

    retries = 2
    while db.status_code != 200 and retries > 0:
        time.sleep(10)
        logging.warning(f'{db.status_code}: {db.text}')
        db = requests.post(
            url=f'https://data.asante.helaplus.com/api/dataset/csv',
            headers={
                'Content_Type': 'application/x-www-form-urlencoded',
                "X-Metabase-Session": get_token(),
            },
            data=payload
        )
        retries -= 1


    dtypes = {'account_no': str, 'amount': float, 'client_name': str,
              'comments': str, 'id': int, 'phone': str,
              'status': int, 'store_number': str, 'transaction_id': str,
              'type': str}

    parse_dates = ['created_at', 'transaction_time', 'updated_at']

    # read the CSV file content into a string buffer
    data = pd.read_csv(io.StringIO(db.content.decode('utf-8')), dtype=dtypes, parse_dates=parse_dates)
    data['balance_after'] = data['balance_after'].apply(
        lambda x: float(str(x).replace(',', '')) if not pd.isnull(x) else x
    )
    data['balance_after'] = data['balance_after'].astype(float)

    return data

# if __name__ == '__main__':
#     print(get_token())
    # print([[x['name'], x['id']] for x in get_all_databases()['data']])
    # get_question()
    # print(get_dataset(None).columns)
