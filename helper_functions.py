import os
import requests
import pymunge

BURSAR_URL = os.getenv('HPC_BURSAR_URL', 'http://127.0.0.1:8000/api/v1/')
BURSAR_CERT_PATH = os.getenv('HPC_BURSAR_CERT_PATH', '')
USER = os.getenv('USER', os.getlogin())
SERVICE = 'user/grants_info'
URL = BURSAR_URL + SERVICE


def generate_token(user, service):
    user_service = user + ':' + service
    bytes_user_service = str.encode(user_service)
    try:
        with pymunge.MungeContext() as ctx:
            user_service = ctx.encode(bytes_user_service)
        return user_service
    except ImportError as e:
        print("Pymunge is not installed.")


def get_data():
    user = USER
    header = {
        'x-auth-hpcbursar': generate_token(user, SERVICE)
    }
    try:
        response = requests.get(URL + '/' + user, headers=header, verify=BURSAR_CERT_PATH)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print('You are unauthorized to perform this request!')
        elif e.response.status_code != 200:
            print("Invalid response from server!")
    except requests.exceptions.ConnectionError as e:
        print("No connection")
    except Exception as e:
        raise Exception('Unable to parse server\'s response!')
