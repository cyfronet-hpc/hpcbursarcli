#!/usr/bin/env python3.9

# Copyright 2022 ACC Cyfronet AGH-UST

# Licensed under the Apache License, Version 2.0,
# copy of the license is available in the LICENSE file;

"""
hpc-grants - Show available grant with details.

Usage:
    hpc-grants
    hpc-grants -h | --help
    hpc-grants -v | --version

Options:
    -h --help   Show help.
    -v --version   Show version.
"""
import os
import sys
from collections import OrderedDict

env_lib_dir = 'HPC_BURSAR_LIBDIR'
if env_lib_dir in os.environ.keys():
    sys.path.append(os.environ[env_lib_dir])

from docopt import docopt
import requests
import pymunge
import json

BURSAR_URL = os.getenv('HPC_BURSAR_URL', 'http://127.0.0.1:8000/api/v1/')
BURSAR_CERT_PATH = os.getenv('HPC_BURSAR_CERT_PATH', '')
USER = os.getenv('USER', os.getlogin())
SERVICE = 'user/grants_info'
URL = BURSAR_URL + SERVICE


def generate_token(user, service):
    user_service = user + ':' + service
    bytes_user_service = str.encode(user_service)
    with pymunge.MungeContext() as ctx:
        token = ctx.encode(bytes_user_service)
    return token


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
            sys.exit(1)
        elif e.response.status_code != 200:
            print("Invalid response from server!")
            sys.exit(1)
    except requests.exceptions.ConnectionError as e:
        print("No connection")
        sys.exit(1)
    except Exception as e:
        raise Exception('Unable to parse server\'s response!')


VERSION = '0.1'


def order_allocations(allocs):
    return sorted(allocs, key=lambda x: x['resource'])


def format_number(value, name):
    if name == 'used hours':
        return f'{value:,.2f}'.replace(',', ' ')
    else:
        return f'{value:,}'.replace(',', ' ')


def process_parameter_value(name, value):
    type_suffix = {
        'timelimit': 'h',
        'hours': 'h',
        'capacity': 'GB',
        'used hours': 'h'
    }
    return f'{format_number(value, name)} {type_suffix[name]}'


def process_parameters(params):
    params = params.copy()
    order = ['hours', 'timelimit', 'capacity']
    ordered_params = OrderedDict()
    for type in order:
        if type in params.keys():
            ordered_params[type] = process_parameter_value(type, params[type])
            del params[type]
    if params:
        ordered_params.update(params)
    return ordered_params


def print_grant_info(data):
    print(f"Grant: {data['name']}")
    print(f" status: {data['status']}, start: {data['start']}, end:  end: {data['end']}")
    allocation_usages_dict = {}
    for allocation_usage in data['allocations_usages']:
        allocation_usages_dict[allocation_usage['name']] = allocation_usage

    allocations = order_allocations(data['allocations'])
    if allocations:
        for al in allocations:
            print(f" Allocation: {al['name']}, resource: {al['resource']}")
            print(f"  status: {al['status']}, start: {al['start']}, start: {al['end']},")
            parameters = process_parameters(al['parameters'])
            print('  parameters: ' + ", ".join([f'{key}: {value}' for key, value in parameters.items()]))

            if al['name'] in allocation_usages_dict.keys():
                allocation_usage = allocation_usages_dict[al['name']]
                consumed_resources = allocation_usage['summary']['resources']
                print('  consumed resources: ' + ", ".join(
                    [f"{k}: {process_parameter_value('used hours', v)}" for k, v in consumed_resources.items()]))
    else:
        print('  - No allocations')
    print(f" Group: {data['group']}")
    print(f"  members: {', '.join(sorted(data['group_members']))}")


def main():
    args = docopt(__doc__)
    if args['--version']:
        print(f'hpc-grants version: {VERSION}')
        sys.exit(0)

    data = sorted(get_data(), key=lambda x: x['start'], reverse=True)
    for i in range(len(data)):
        grant = data[i]
        # print(json.dumps(grant, indent=2))
        print_grant_info(grant)
        if i != len(data) - 1:
            print('---')


if __name__ == '__main__':
    main()
