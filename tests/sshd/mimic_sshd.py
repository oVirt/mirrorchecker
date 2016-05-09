import argparse
import os
import tempfile
import shutil
from subprocess import call

def create_config(port, working_dir):
    return """
Port {custom_port}
HostKey {dir}/host_keys/ssh_host_rsa_key
UsePrivilegeSeparation no       # Default for new installations.
PermitUserEnvironment yes
PidFile {dir}/var/russhd2.pid
""".format(custom_port=port, dir=working_dir)


def create_host_key(dest):
    status = call("ssh-keygen -t dsa -f {0}/ssh_host_rsa_key -N ''".format(dest),
                  shell=True)

def setup_ssh(root_dir, port):
    """TODO: Docstring for setup_ssh.
    :returns: TODO

    """
    working_dir = tempfile.mkdtemp(dir=root_dir)
    os.mkdir('{0}/host_keys'.format(working_dir))
    create_host_key('{0}/host_keys'.format(working_dir))
    os.mkdir('{0}/.ssh'.format(working_dir))
    os.mkdir('{0}/config'.format(working_dir))
    sshd_config = create_config(port, working_dir)
    with open('{0}/config/sshd_config'.format(working_dir), 'w') as f:
        f.write(sshd_config)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root_dir',
                        default=os.path.dirname(os.path.realpath(__file__)))
    parser.add_argument('--port', default='2222')
    args = parser.parse_args()
    setup_ssh(args.root_dir, args.port)


if __name__ == "__main__":
    main()
