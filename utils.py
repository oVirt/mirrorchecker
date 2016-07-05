from paramiko.ssh_exception import SSHException
import paramiko
import time
import logging
import requests
import re
import socket
from datetime import datetime
logger = logging.getLogger('__main__')
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
    except TypeError, e:
        logging.exception('illegal timestamp')
        pass

def walk_find(remote_path, ssh_args):
    out = []
    cmd = "find {0} -type d -printf '%P\n'".format(remote_path)
    ssh, proxy = get_ssh(ssh_args)
    logging.debug('%s', ssh_args)
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=10)
    out = [line for line in stdout.readlines()]
    err = [line for line in stderr.readlines()]
    ssh.close()
    proxy.close()
    if len(err) > 0:
        raise paramiko.SSHException('error executing %s, stderr: %s' % (cmd, err))
    return out

def get_timestamp(format=None):
    return int(time.time())


def get_ssh(ssh_args):
    local_sshargs = dict(ssh_args)
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        proxy_cmd = local_sshargs.pop('proxy_cmd', False)
        ssh_proxy = None
        if proxy_cmd:
            proxy_cmd = proxy_cmd.replace('%h', local_sshargs['hostname'])
            proxy_cmd = proxy_cmd.replace('%p', local_sshargs.get('port', '22'))
            ssh_proxy = paramiko.ProxyCommand(proxy_cmd)
        ssh.connect(sock = ssh_proxy, **local_sshargs)
        return ssh, ssh_proxy if ssh_proxy else type("", (), dict(close=lambda x: None))()
    except (socket.gaierror, socket.error, socket.timeout) as err:
        logger.error('error starting proxy_md %s' % proxy_cmd)
        raise
    except SSHException as err:
        logger.error('error setting up ssh connection')
        raise
