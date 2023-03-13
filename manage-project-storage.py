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

import stat
import sys
import subprocess
import grp
from docopt import docopt
from helper_functions import *

env_lib_dir = 'HPC_BURSAR_LIBDIR'
if env_lib_dir in os.environ.keys():
    sys.path.append(os.environ[env_lib_dir])

PROJECT_BASE = '/net/ascratch/groups/'
PROJECT_FS = '/net/ascratch/'
LFS_PATH = '/usr/bin/lfs'

MODE = 2770

VERSION = '0.1'

verbose = False


def debug(text):
    if verbose:
        print(text)


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
                if allocation['resource'] == 'Storage':
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
            # group is not present in the system
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
    if 'accepted' in grant['status']:
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
