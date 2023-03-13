#!/usr/bin/env python3.9

# Copyright 2022 ACC Cyfronet AGH-UST

# Licensed under the Apache License, Version 2.0,
# copy of the license is available in the LICENSE file;

"""
hpc-grants - Show available grant with details.

Usage:
    hpc-grants
    hpc-grants -h | --help
    hpc-grants -v | --verbose
    hpc-grants -V | --version

Options:
    -h --help   Show help.
    -v --verbose   Show additional info.
    -V --version   Show version.
"""

import os
import stat
import sys
import subprocess
import grp

env_lib_dir = 'HPC_BURSAR_LIBDIR'
if env_lib_dir in os.environ.keys():
    sys.path.append(os.environ[env_lib_dir])

from docopt import docopt
import pymunge
import requests
import json
import datetime

PROJECT_BASE = '/net/pr2/projects/plgrid/'
PROJECT_FS = '/net/pr2/'
LFS_PATH = '/usr/bin/lfs'

MODE = 2770

VERSION = '0.1'
BURSAR_URL = os.getenv('HPC_BURSAR_URL', 'http://127.0.0.1:8000/api/v1/')
BURSAR_CERT_PATH = os.getenv('HPC_BURSAR_CERT_PATH', '')
USER = os.getenv('USER', 'root')
SERVICE = 'admin/grants_group_info'
URL = BURSAR_URL + SERVICE

verbose = False


def debug(text):
    if verbose:
        print(text)


def generate_token(user, service):
    user_service = user + ':' + service
    bytes_user_service = str.encode(user_service)
    with pymunge.MungeContext() as ctx:
        user_service = ctx.encode(bytes_user_service)
    return user_service


def get_data():
    user = USER
    header = {
        'x-auth-hpcbursar': generate_token(user, SERVICE)
    }
    response = requests.get(URL + '/', headers=header, verify=BURSAR_CERT_PATH)
    if response.status_code == 403:
        raise Exception('You are unauthorized to perform this request!')
    elif response.status_code != 200:
        raise Exception('Invalid response from server!')

    try:
        data = response.json()
        return data
    except Exception as e:
        raise Exception('Unable to parse server\'s response!')


def execute(cmd):
    cmd_full = cmd
    debug('Executing command: %s' % str(cmd_full))
    cp = subprocess.run(cmd_full, capture_output=True)
    # output is converted to utf8 string!
    return cp.returncode, cp.stdout.decode(), cp.stderr.decode()


def sum_storage(grants):
    sum = 0
    for grant in grants:
        if 'allocations' in grant.keys():
            for allocation in grant['allocations']:
                if allocation['resource'] == 'storage':
                    sum += allocation['parameters']['capacity']
    return sum


def check_quota(gid):
    cmd = [LFS_PATH, 'quota', '-p', str(gid), PROJECT_FS]
    return_code, stdout, stderr = execute(cmd)
    for line in stdout.split('\n'):
        if PROJECT_FS in line:
            parts = line.split()
            quota = int(int(parts[3]) / 1024 / 1024)
            return quota


def set_quota(gid, quota):
    quota_kb = quota * 1024 * 1024
    cmd = [LFS_PATH, 'setquota', '-p', str(gid), '-B', f'{quota_kb}', PROJECT_FS]
    execute(cmd)


def set_project(path, gid):
    cmd = [LFS_PATH, 'project', '-p', str(gid), '-s', '-r', path]
    execute(cmd)


def synchronize_storage(group_grants):
    system_groups_raw = grp.getgrall()
    system_groups_gid = {}
    for sgr in system_groups_raw:
        system_groups_gid[sgr.gr_name] = sgr.gr_gid

    for group, grants in group_grants.items():
        if group not in system_groups_gid.keys():
            debug(f'Group: {group} not present in the system!')
            continue
        capacity = sum_storage(grants)
        if capacity < 1:
            capacity = 1
        gid = system_groups_gid[group]
        debug(f'group: {group}, capacity: {capacity}, gid: {gid}')

        group_dir_path = PROJECT_BASE + group
        if os.path.isdir(group_dir_path):
            quota = check_quota(gid)
            if quota != capacity:
                set_quota(gid, capacity)
        else:
            os.mkdir(group_dir_path)
            os.chmod(group_dir_path, stat.S_ISGID
                     | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                     | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP)
            os.chown(group_dir_path, -1, gid)
            set_project(group_dir_path, gid)
            set_quota(gid, capacity)


def is_grant_active(grant):
    # end = datetime.grant['end'] + datetime.timedelta(days=1)
    # start = datetime.grant['start']
    # if end > datetime.datetime.now().date() and start < datetime.datetime.now().date() and 'grant_active' in grant['state']:
    if 'accepted' in grant['status'] or 'active' in grant['status']:
        return True
    else:
        return False


def main():
    args = docopt(__doc__)
    if args['--version']:
        print(f'manage-project-storage version: {VERSION}')
        sys.exit(0)

    if args['--verbose']:
        global verbose
        verbose = True

    data = get_data()
    group_grants = {}
    for group in data['groups']:
        group_grants[group['name']] = []
    for grant in data['grants']:
        if not is_grant_active(grant):
            continue
        group_name = grant['group']
        if group_name in group_grants.keys():
            group_grants[group_name] += [grant]
        else:
            debug(f"No group {group_name} for grant: {grant['name']}")

    synchronize_storage(group_grants)


if __name__ == '__main__':
    main()
