#!/usr/bin/python2
# Copyright 2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301 USA
"""
Mirror site synchronization status plugin for nagios/icinga
------------------------------------------------------------
to be used with mirrorchecker tool: http://mirrorchecker.readthedocs.org

Configuration example for nagios/icinga1
-----------------------------------------
1. Drop this file in /usr/lib/nagios64/plugins/check_mirror
2. add to your commands.cfg:
 define command{
         command_name check_mirror
         command_line $USER1$/check_mirror --url $ARG1$ --c 72 --w 48
  }

 3. create a new service for each mirror site:
  define service {
        use local-service
        hostgroup_name mirrorchecker
        service_description mirror1.example.com/pub mirror site last sync
        notes_url http://mirrorchecker-deployment.source.com/mirrors
        check_command check_mirror!http:\/\/mirrorchecker-deployment.source.com/mirrors/mirror1.example.com_pub
  }
"""
from __future__ import print_function
from __future__ import division
import argparse
import sys
import requests

NAGIOS_STATUS = {
    'OK': 0,
    'WARNING': 1,
    'CRITICAL': 2,
    'UNKNOWN': 3,
}


def print_exit(status, seconds, hours):
    print(
        '{0} - {1} seconds since last sync, which are {2:.4f} hours.|last_sync={3}'.format(
            status, seconds, hours, seconds
        )
    )
    sys.exit(NAGIOS_STATUS[status])


def main():
    parser = argparse.ArgumentParser(description='check mirror status')
    parser.add_argument('--url', help='mirror monitoring url', required=True)
    parser.add_argument(
        '--w', help='warning threshold in hours', type=float, default=24
    )
    parser.add_argument(
        '--c', help='critical threshold in hours', type=float, default=36
    )
    args = parser.parse_args()
    sync = -1
    try:
        res = requests.get(args.url, timeout=5.0)
        sync = res.json().values()[0]
    except (requests.exceptions.RequestException, ValueError):
        print('UNKNOWN - unable to connect')
        sys.exit(NAGIOS_STATUS['UNKNOWN'])
    hours = sync / 60 / 60 if sync > 0 else sync
    if 0 < hours < args.w:
        print_exit('OK', sync, hours)
    elif args.w <= hours < args.c:
        print_exit('WARNING', sync, hours)
    elif hours >= args.c:
        print_exit('CRITICAL', sync, hours)
    else:
        print('UNKNOWN - no synchronization detected yet.')
        sys.exit(NAGIOS_STATUS['UNKNOWN'])


if __name__ == '__main__':
    main()
