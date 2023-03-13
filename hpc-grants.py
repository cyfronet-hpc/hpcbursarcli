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
from docopt import docopt
import json
from helper_functions import get_data

env_lib_dir = 'HPC_BURSAR_LIBDIR'
if env_lib_dir in os.environ.keys():
    sys.path.append(os.environ[env_lib_dir])

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
    print(f" state: {data['state']}, start: {data['start']}, end:  end: {data['end']}")
    allocation_usages_dict = {}
    for allocation_usage in data['allocations_usages']:
        allocation_usages_dict[allocation_usage['name']] = allocation_usage

    allocations = order_allocations(data['allocations'])
    if allocations:
        for al in allocations:
            print(f" Allocation: {al['name']}, resource: {al['resource']}")
            print(f"  state: {al['state']}, start: {al['end']}, start: {al['end']},")
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
