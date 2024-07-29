#!/usr/bin/env python3.9

# Copyright 2022 ACC Cyfronet AGH-UST

# Licensed under the Apache License, Version 2.0,
# copy of the license is available in the LICENSE file;

"""
hpc-grants - Show grants on current cluster.

Usage:
    hpc-grants
    hpc-grants -h | --help
    hpc-grants -v | --version
    hpc-grants [-s | --short] [-a | --active | -e | --empty | -l | --last | -o | --all | -i | --inactive]

Options:
    -h --help       Show help.
    -v --version    Show version.
    -a --active     Show active grants.
    -i --inactive   Show inactive grants.
    -s --short      Show nonverbose mode.
    -o --all        Show all grants on each cluster.
    -e --empty      Show grants on other clusters.
    -l --last       Show grants with end date no older than 1 year and 1 month ago.
"""
import os
import sys
import itertools
from collections import OrderedDict

env_lib_dir = 'HPC_BURSAR_LIBDIR'
if env_lib_dir in os.environ.keys():
    sys.path.append(os.environ[env_lib_dir])

from datetime import datetime, timedelta
from itertools import filterfalse
from docopt import docopt
import requests
import pymunge
import json

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
        response = requests.get(URL + '/' + user, headers=header, verify=False)
        #response = requests.get(URL + '/' + user, headers=header, verify=BURSAR_CERT_PATH)
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


def print_grant_short_info(data):
    print(f"Grant: {data['name']}")
    print(f"  status: {data['status']}, start: {data['start']}, end: {data['end']}")
    print(f"  Group: {data['group']}")
    print(f"   members: {', '.join(sorted(data['group_members']))}")


def print_grant_info(data):
    print(f"Grant: {data['name']}")
    print(f"  status: {data['status']}, start: {data['start']}, end: {data['end']}")
    allocation_usages_dict = {}
    for allocation_usage in data['allocations_usages']:
        allocation_usages_dict[allocation_usage['name']] = allocation_usage

    allocations = order_allocations(data['allocations'])
    if allocations:
        for al in allocations:
            print(f"  Allocation: {al['name']}, resource: {al['resource']}")
            print(f"   status: {al['status']}, start: {al['start']}, end: {al['end']},")
            parameters = process_parameters(al['parameters'])
            print('   parameters: ' + ", ".join([f'{key}: {value}' for key, value in parameters.items()]))

            if al['name'] in allocation_usages_dict.keys():
                allocation_usage = allocation_usages_dict[al['name']]
                consumed_resources = allocation_usage['summary']['resources']
                print('   consumed resources: ' + ", ".join(
                    [f"{k}: {process_parameter_value('used hours', v)}" for k, v in consumed_resources.items()]))
    else:
        print('  - No allocations')
    print(f"  Group: {data['group']}")
    print(f"   members: {', '.join(sorted(data['group_members']))}")


def line_print():
    print('-------------------------------------------------------')


def last(grant):
    date_str = grant['end']
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    present = datetime.now()
    last1r = present - timedelta(days=395)
    if (date_obj > last1r):
        return True
    else:
        return False


def active(grant):
    if grant['status'] == "active":
        return True
    else:
        return False

def all(grant):
    return True

def allocations(data):
    allocations = order_allocations(data['allocations'])
    if allocations:
        return True
    else:
        return False



def main():
    args = docopt(__doc__)

    if args['--version']:
        print(f'hpc-grants version: {VERSION}')
        sys.exit(0)


    data = sorted(get_data(), key=lambda x: x['start'], reverse=True)


    if args['--all']:
        filtered_grants = list(filter(all, data))

    elif args['--empty']:
        filtered_grants = list(filterfalse(allocations, data))

    elif (args['--active'] and args['--inactive']):
        filtered_grants = list(filter(active, filter(allocations, data))) + list(filterfalse(active, filter(allocations, data)))
        # active and inactive grants on current cluster, not the same as all grants

    elif (args['--inactive']):
        filtered_grants = list(filterfalse(active, filter(allocations, data)))

    elif args['--active']:
        filtered_grants = list(filter(active, filter(allocations, data)))

    elif args['--last']:
        filtered_grants = list(filter(last, filter(allocations, data)))

    else:
        filtered_grants = list(filter(allocations, data))
    

    for j in filtered_grants:
        if args['--short']:
            print_grant_short_info(j)
        else:
            print_grant_info(j)
        if j != filtered_grants[-1]:
            line_print()


if __name__ == '__main__':
    main()
