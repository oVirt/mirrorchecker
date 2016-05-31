import paramiko
import time
import logging
import requests
import re
from datetime import datetime
logger = logging.getLogger()
def put_sftp(scp, local_file, remote_file, confirm=True):
    scp.put(local_file, remote_file, None, confirm, preserve_mtime=False)

def sftp_direct_put(sftp, filename, string, **kwargs):
    with sftp.open(filename=filename, mode='w') as remote_file:
        remote_file.write(string)

def walk_sftp():
    pass

def get_urlstamps(base_url, dirs, pattern=r'(^\d{10}$)'):
    result = {}
    for stamp in dirs:
        url = '{0}/{1}'.format(base_url, stamp)
        res = requests.get(url)
        if res.status_code == 200 and re.match(pattern, res.content):
            result[stamp] = int(res.content)
        else:
            result[stamp] = -1
    return result

def diff_timestamps(ts1, ts2):
    try:
        dt_1 = datetime.fromtimestamp(ts1)
        dt_2 = datetime.fromtimestamp(ts2)
        return (dt_1 - dt_2).total_seconds()
    except TypeError as e:
        logging.exception('illegal timestamp')
        pass





def walk_find(remote_path, ssh_args):
    out = []
    cmd = "find {0} -type d -printf '%P\n'".format(remote_path)
 #   paramiko.util.log_to_file('paramiko.log')
#    logging.getLogger("paramiko").setLevel(logging.DEBUG)
    ssh = paramiko.SSHClient()

    logging.debug('%s', ssh_args)
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(**ssh_args)
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=10)
    out = [line for line in stdout.readlines()]
    err = [line for line in stderr.readlines()]
    ssh.close()
    if len(err) > 0:
        raise paramiko.SSHException('error executing %s, stderr: %s' % (cmd, err))
    return out

def get_timestamp(format=None):
    return int(time.time())
