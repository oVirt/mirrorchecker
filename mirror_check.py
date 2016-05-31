import time
import utils
import logging
import paramiko
import random
import yaml
from twisted.internet import task
from twisted.internet import reactor
from collections import namedtuple


class stamper(object):
    """
    put timestamps in directories via SCP
    """
    def __init__(self, name, remote_path, ssh_args, dirs='', max_stamp=10,
                 stamp_interval='60', mirrors = {}):
        self.name = name
        self.ssh_args = ssh_args
        self.max_stamp = max_stamp
        self.remote_path = remote_path
        self.dirs = dirs
        self.stamp_interval = stamp_interval
        self.last_stamps = {}
        self.mirrors = mirrors
        self.mirrors_tasks = {}

    def run(self):
        logging.debug('stampping following dirs: %s', self.dirs)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(**self.ssh_args)
        sftp = ssh.open_sftp()
        begin = time.time()
        for stamp in self.dirs:
            path = '/'.join([self.remote_path, stamp.strip()])
            try:
                with sftp.open(path, 'w') as f:
                    timestamp = utils.get_timestamp()
                    f.write(unicode(timestamp))
                    self.last_stamps[stamp] = int(timestamp)
            except Exception as e:
                logging.exception('exception while stamping %s' % path)
                pass
        end = time.time()
        logging.debug('stampped {0} dirs, took {1}s'.format(len(self.dirs),
                                                           str(end-begin)))

class mirror_site(object):
    """
    a mirror site status
    """
    def __init__(self, name, url, stamps='', check_interval=10):
        self.url = url
        self.stamps = stamps
        self.check_interval = check_interval
        self.name = name

    def run(self):
        logging.debug('checking stamps for %s', self.url)
        dirs = utils.get_urlstamps(self.url, self.stamps)
        logging.debug('got stamps for %s\n%s', self.url, self.stamps)
        for k, v in dirs.iteritems():
            diff = utils.diff_timestamps(utils.get_timestamp(),
                                         int(v))
            logging.debug('%s: %s: out-of-sync: %ss', self.url,
                                                           k,
                                                           diff)


def generate_dirs(remote_path, amount, ssh_args, ts_fname):
    dirs = utils.walk_find(remote_path, ssh_args)
    dirs.sort(key=lambda x: x.count('/'), reverse=True)
    sample_dirs = random.sample(dirs, min(amount, len(dirs) - 1))
    return ['%s/%s' %(x.strip(), ts_fname) for x in sample_dirs]


def load_config(config_fname):
    with open(config_fname, 'r') as f:
        configs = yaml.load(f)
    return configs


def build_backends(configs):
    backends = configs.get('backends')
    backend_tasks = {}
    obj_task = namedtuple('obj_task', 'obj task')
    for backend in backends:
        backend_obj = stamper(**backend)
        if not backend_obj.dirs:
            backend_stamps = generate_dirs(backend_obj.remote_path,
                                        backend_obj.max_stamp,
                                        backend_obj.ssh_args,
                                        configs.get('ts_fname'))
            backend_obj.dirs = backend_stamps
        for mirror in backend_obj.mirrors:
            mirror_obj = mirror_site(**mirror)
            mirror_obj.stamps = backend_obj.dirs
            mirror_task = task.LoopingCall(mirror_obj.run)
            backend_obj.mirrors_tasks[mirror_obj.name] = obj_task(mirror_obj,
                                                                  mirror_task)
        backend_task = task.LoopingCall(backend_obj.run)
        backend_tasks[backend_obj.name] = obj_task(backend_obj, backend_task)
    return backend_tasks


def main():
    log_formatter = '%(threadName)s::%(levelname)s::%(asctime)s::%(module)s::%(lineno)d::%(name)s::(%(funcName)s) %(message)s'
    logging.basicConfig(filename='mirror_checker.log', level=logging.DEBUG,
                        format=log_formatter)
    # to argparse
    # ssh_args = {'hostname': 'resources.ovirt.org'}
    # remote_path = '/srv/resources/repos/ngoldin/mirror-tester'
    # max_stamp = 10
    # stamp_interval = 10.0
    # ts_fname = '.timestamp'
    config_fname = 'mirrors.yaml'
    # end of argparse

    configs = load_config(config_fname)
    print configs
    backends = build_backends(configs)
    for k,v in backends.iteritems():
        print 'starting backend: %s' % v.obj.name
        v.task.start(v.obj.stamp_interval)
        for k2,v2 in v.obj.mirrors_tasks.iteritems():
            print 'starting backend: %s' % v2.obj.name
            v2.task.start(v2.obj.check_interval)
    reactor.run()


#    reactor.run()
    #initialize
    # t_stamper = stamper(**configs.get('backend'))
    # stamps = generate_dirs(t_stamper.remote_path, max_stamp,t_stamper.ssh_args,
                           # configs.get('ts_fname'))
    # t_stamper.dirs = stamps

    # stamp_task = task.LoopingCall(t_stamper.run)
    # stamp_task.start(t_stamper.stamping_interval)

    # local_mirror = mirror_site('http://resources.ovirt.org/repos/ngoldin/mirror-tester/',
                               # stamps)
    # mirror_monitor = task.LoopingCall(local_mirror.check_stamps)
    # mirror_monitor.start(stamp_interval + 1)

if __name__ == '__main__':
    main()
