import os
import subprocess
import shutil
import time
import contextlib
import tempfile
from subprocess import call

class sshd_wrapper(object):

    def __init__(self, working_dir=None, server_key=None, port='3000'):
        self.port = port
        if not working_dir:

            running_dir = os.path.dirname(os.path.realpath(__file__))
            running_dir = tempfile.mkdtemp(dir=running_dir)
            self.working_dir = '%s/.sshd' % running_dir
        else:
            self.working_dir = '%s/.sshd' % working_dir
        tempfile.tempdir
        os.mkdir(self.working_dir)
        os.chmod(self.working_dir, 0700)
        self.port = port
        self.p_sshd = None
        self.host_key = None
        self.cfg_file = None
        self.dirs = None
        self.def_public_key = None
        self.def_private_key = None
        self.create_config(self.port, self.working_dir)
        self.generate_key()

    def create_config(self, port, working_dir):
        """
        create_config

        :param port:
        :param working_dir:
        """
        dirs = {'h_key': '%s/host_keys' % working_dir,
                'var': '%s/var' % working_dir,
                'ssh': '%s/.ssh' % working_dir}
        for dir in dirs.itervalues():
            os.mkdir(dir)
            os.chmod(dir, 0700)

        cmd = ("ssh-keygen -t rsa -f {0}/ssh_host_rsa_key -N ''"
               ).format(dirs['h_key'])
        call(cmd, shell=True)
        with open('%s/ssh_host_rsa_key' % dirs['h_key']) as key_file:
            self.host_key = key_file.readlines()
        sshd_template = ("Port {custom_port}\n"
                         "HostKey {key_dir}/ssh_host_rsa_key\n"
                         "UsePrivilegeSeparation no\n"
                         "PermitUserEnvironment yes\n"
                         "PidFile {var_dir}/russhd2.pid\n"
                         "AuthorizedKeysFile  {ssh_dir}/authorized_keys\n")
        sshd_cfg = sshd_template.format(custom_port=port,
                                        key_dir=dirs['h_key'],
                                        var_dir=dirs['var'],
                                        ssh_dir=dirs['ssh'])

        with open('%s/sshd_config' % working_dir, 'w') as f:
            f.write(sshd_cfg)

        self.cfg_file = '%s/sshd_config' % working_dir
        self.dirs = dirs

    def add_auth_key(self, key):
        auth_file = '%s/authorized_keys' % self.dirs['ssh']
        with open(auth_file, 'a') as keys:
            keys.write(key)
            keys.write('\n')
        os.chmod(auth_file, 0600)

    def generate_key(self):
        key_file = '%s/id_rsa' % self.dirs['ssh']
        cmd = "ssh-keygen -b 2048 -t rsa -f %s -q -N ''" % key_file
        call(cmd, shell=True)
        with open(key_file, 'r') as private_key, open('%s.pub' % key_file, 'r') as pub_key:
            self.def_public_key = pub_key.read()
            self.def_private_key = private_key.read()
        self.add_auth_key(self.def_public_key)

    def start_sshd(self):
        cmd = "exec /usr/sbin/sshd -D -f {0}".format(self.cfg_file)
        self.p_sshd = subprocess.Popen(cmd, shell=True)

    def stop(self):
        self.p_sshd.terminate()

    def destroy(self, delete=True):
        self.stop()
        if delete:
            shutil.rmtree(self.working_dir)


@contextlib.contextmanager
def sshd_server(*args, **kwargs):
    obj = sshd_wrapper(*args, **kwargs)
    obj.start_sshd()
    try:
        yield obj
    finally:
        obj.destroy()
    return


def main():
    with sshd_server() as sshd:
        print sshd.def_public_key
        time.sleep(100)


if __name__ == '__main__':
    main()
