import json
import asyncio
import requests
import pandas as pd
from pngme.api import AsyncClient


def get_customer(access_token: str, uuid: str, flag: str):
    return asyncio.run(load_to_df(client=AsyncClient(access_token), flag=flag, uuid=uuid))


async def load_to_df(client: AsyncClient, flag: str = None, uuid: str = None):
    # Load to a dataframe
    if flag == 'user':
        df = pd.DataFrame(await client.users.get(search=uuid))
    elif flag in ['institutions', 'transactions', 'alerts', 'balances']:
        institutions = await client.institutions.get(user_uuid=uuid)
        institution_ids = [institution['institution_id'] for institution in institutions]
        df = pd.DataFrame(institutions)
    else:
        df = pd.DataFrame(await client.users.get())

    # Retrieve coroutines
    if flag == 'transactions':
        coroutines = [client.transactions.get(user_uuid=uuid, institution_id=institution_id) for institution_id in
                      institution_ids]
    elif flag == 'alerts':
        coroutines = [client.alerts.get(user_uuid=uuid, institution_id=institution_id) for institution_id in
                      institution_ids]
    elif flag == 'balances':
        coroutines = [client.balances.get(user_uuid=uuid, institution_id=institution_id) for institution_id in
                      institution_ids]

    # Attach user id
    if flag == 'institutions':
        df['uuid'] = uuid
    elif flag in ['transactions', 'alerts', 'balances']:
        coroutines_gather = await asyncio.gather(*coroutines)
        d = dict(zip(institution_ids, coroutines_gather))

        dt = [pd.DataFrame(x[1], index=[x[0]] * len(x[1])) for x in d.items()]
        if len(dt) > 0:
            df = pd.concat(dt)
            df['uuid'] = uuid
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'institution_id'}, inplace=True)

    return df


def get_access_token(env):
    res = requests.get(
        url="https://api.pngme.com/beta/auth",
        headers={"Accept": "application/json"},
        auth=requests.auth.HTTPBasicAuth('henry.mburu@asantefinancegroup.com', 'FFPP7SXcYzjAi!Z')
    )
    if res.status_code == 200:
        auth = json.loads(requests.get(
            url="https://api.pngme.com/beta/auth",
            headers={"Accept": "application/json"},
            auth=requests.auth.HTTPBasicAuth('henry.mburu@asantefinancegroup.com', 'FFPP7SXcYzjAi!Z')
        ).text)
        if env == 'live_asante':
            return [x for x in auth[1]['auth'] if x['type'] == 'production'][0]['api_token']
        elif env == 'test_asante':
            return [x for x in auth[1]['auth'] if x['type'] == 'test'][0]['api_token']
    else:
        res.raise_for_status()


def search_user(search_string, access_token):
    return requests.get(
        url=f"https://api.pngme.com/beta/users?search={search_string}&page=1",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        },
    ).text

def get_institutions(user_uuid, access_token):
    return requests.get(
        url=f"https://api.pngme.com/beta/users/{user_uuid}/institutions",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        },
    ).text


def get_transactions(user_uuid, acct_uuid, utc_starttime, utc_endtime, access_token):
    return requests.get(
        url=f"https://api.pngme.com/beta/users/{user_uuid}/accounts/{acct_uuid}/transactions?utc_starttime={utc_starttime}&page=1",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        },
    ).text


def get_account_balances(user_uuid, account_uuid, utc_starttime, utc_endtime, access_token):
    return requests.get(
        url=f"https://api.pngme.com/beta/users/{user_uuid}/accounts/{account_uuid}/balances?utc_starttime={utc_starttime}&utc_endtime={utc_endtime}",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        },
    ).text


def request_credit_report(user_uuid, access_token):
    return requests.post(
        url=f"https://api.pngme.com/beta/users/{user_uuid}/creditreport",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        },
    ).text


def get_credit_report(user_uuid, credit_report_id, access_token):
    return requests.get(
        url=f"https://api.pngme.com/beta/users/{user_uuid}/creditreport/{credit_report_id}",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        },
    ).text


def get_dti_for_user(user_uuid, utc_starttime, utc_endtime, access_token):
    return requests.get(
        url=f"https://api.pngme.com/beta/users/{user_uuid}/dti?utc_starttime={utc_starttime}&utc_endtime={utc_endtime}",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        },
    ).text


def get_transactions_summary(user_uuid, utc_starttime, utc_endtime, access_token):
    return requests.get(
        url=f"https://api.pngme.com/beta/users/{user_uuid}/transactions-summary?utc_starttime={utc_starttime}",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        },
    ).text


def get_alerts(user_uuid, account_uuid, utc_starttime, utc_endtime, access_token):
    return requests.get(
        url=f"https://api.pngme.com/beta/users/{user_uuid}/accounts/{account_uuid}/alerts?utc_starttime={utc_starttime}&page=1",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        },
    ).text


def get_alert_summary(user_uuid, utc_starttime, utc_endtime, access_token):
    return requests.get(
        url=f"https://api.pngme.com/beta/users/{user_uuid}/alerts-summary?utc_starttime={utc_starttime}",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        },
    ).text

