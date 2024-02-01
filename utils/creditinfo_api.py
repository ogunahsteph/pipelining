import json
import requests
from airflow.models import Variable


def get_env(env: str) -> str:
    """
    Returns a pandas DataFrame containing a list of invalidated loan transactions for the specified tenant and office.

    :param env: The name of the environment, either 'test' or 'live'.
    :type env: str

    :return: The environment configuration based on the input 'env'.
    :rtype: str

    Raises
        ValueError: If the 'env' variable is not 'test' or 'live'.

    """

    if env == 'test':
        return env
    elif env == 'live':
        return ''
    raise ValueError('The environment has not been specified. "env" variable not defined')


def get_strategies(env):
    """
    Get strategies from the Creditinfo API based on the provided environment.

    This function fetches strategies from the Creditinfo API based on the environment.
    If 'env' is 'test', it fetches strategies from the test environment.
    If 'env' is 'live', it fetches strategies from the live environment.

    :param env: The name of the environment, either 'test' or 'live'.
    :type: str

    :return: A dictionary containing the strategies fetched from the Creditinfo API.
    :rtype: dict

    """
    env = get_env(env)
    credentials = 'live' if env == '' else 'test'

    res = requests.get(
        url=f'https://idm{env}.creditinfo.co.ke/api/strategies/',
        auth=requests.auth.HTTPBasicAuth(Variable.get(f'idm_{credentials}_username'),
                                         Variable.get(f'idm_{credentials}_password'))
    )
    return json.loads(res.text)


def get_results(token_id: str, env: str) -> requests.Response:
    """
    Get results from the Creditinfo API based on the provided token ID and environment.

    This function fetches results from the Creditinfo API based on the provided token ID and environment.
    If 'env' is 'test', it fetches results from the test environment.
    If 'env' is 'live', it fetches results from the live environment.

    :token_id: The unique token ID associated with the request.
    :type token_id: str

    :env: The name of the environment, either 'test' or 'live'.
    :type env: str

    :return: The response object from the API containing the results.
    :rtype: requests.Response

    """
    env = get_env(env)
    credentials = 'live' if env == '' else 'test'

    res = requests.get(
        url=f'https://idm{env}.creditinfo.co.ke/api/strategies/{token_id}',
        auth=requests.auth.HTTPBasicAuth(Variable.get(f'idm_{credentials}_username'),
                                         Variable.get(f'idm_{credentials}_password'))
    )
    return res


def get_token(national_id: str, env: str, action: str) -> requests.Response:
    """
    Get a token from the Creditinfo API based on the provided national ID, environment, and action.

    This function requests a token from the Creditinfo API based on the provided national ID, environment, and action.
    If 'env' is 'test', it requests the token from the test environment.
    If 'env' is 'live', it requests the token from the live environment.
    The 'action' parameter specifies the type of request, which can be 'iprs' or 'idm'.

    :param national_id:  The national ID associated with the request.
    :type national_id: str

    :param env: The name of the environment, either 'test' or 'live'.
    :type env: str

    :param action: The type of request to be made. It can be either 'iprs' or 'idm'.
    :type action: str

    :return:  The national ID associated with the request.
    :rtype: requests.Response

    """
    env = get_env(env)
    credentials = 'live' if env == '' else 'test'

    if action == 'iprs':
        res = requests.post(
            url=f'https://idm{env}.creditinfo.co.ke/api/strategies/{Variable.get(f"iprs_{credentials}_strategy_id")}',
            json={
                'CustomFields': {"DocumentType": "NationalId", "DocumentNumber": national_id},
                "Consent": True
            },
            auth=requests.auth.HTTPBasicAuth(Variable.get(f'idm_{credentials}_username'),
                                             Variable.get(f'idm_{credentials}_password'))
        )
    elif action == 'idm':
        res = requests.post(
            url=f'https://idm{env}.creditinfo.co.ke/api/strategies/{Variable.get(f"idm_{credentials}_strategy_id")}',
            json={
                'ConnectorRequest': {"NationalId": national_id},
                "Consent": True
            },
            auth=requests.auth.HTTPBasicAuth(Variable.get(f'idm_{credentials}_username'),
                                             Variable.get(f'idm_{credentials}_password'))
        )
    return res
