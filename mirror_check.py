import time
import utils
import logging
import random
import yaml
from flask import Flask
from twisted.web.resource import Resource
from twisted.internet import task
from twisted.internet import reactor
from twisted.web.wsgi import WSGIResource
from twisted.web.server import Site
from collections import namedtuple


defaults = {
        'log_level': 'debug',
        'log_file': 'mirror_checker.log',
        'http_port': 8080,
        'http_prefix': 'api',
        }
backends = None
app = Flask(__name__)
logger = logging.getLogger(__name__)

class stamper(object):
    """
    put timestamps in directories via SCP
    """
    def __init__(self, name, remote_path, ssh_args, dirs='', max_stamp=10,
                 stamp_interval='60', mirrors={}, ts_fname=''):
        self.name = name
        self.ssh_args = ssh_args
        self.max_stamp = max_stamp
        self.remote_path = remote_path
        self.dirs = dirs
        self.stamp_interval = stamp_interval
        self.last_stamps = {}
        self.mirrors = mirrors
        self.mirrors_tasks = {}
        self.ts_fname = ts_fname

    def run(self):
        ssh, proxy = utils.get_ssh(self.ssh_args)
        sftp = ssh.open_sftp()
        begin = time.time()
        for stamp in self.dirs:
            path = '/'.join([self.remote_path, stamp.strip()])
            try:
                with sftp.open(path, 'w') as f:
                    timestamp = utils.get_timestamp()
                    f.write(unicode(timestamp))
                    self.last_stamps[stamp] = int(timestamp)
            except Exception, e:
                logger.exception('exception while stamping %s' % path)
                pass
        end = time.time()
        logger.debug('stampped {0} dirs, took {1}s'.format(len(self.dirs),
                                                           str(end-begin)))
        ssh.close()
        proxy.close()


class mirror_site(object):
    """
    a mirror site status
    """
    def __init__(self, name, url, stamps='', check_interval=10):
        self.url = url
        self.stamps = stamps
        self.check_interval = check_interval
        self.name = name
        self.diff = -1

    def run(self):
        logger.debug('checking stamps for %s', self.url)
        dirs = utils.get_urlstamps(self.url, self.stamps)
        logger.debug('got stamps for %s\n%s', self.url, self.stamps)
        for k, v in dirs.iteritems():
            self.diff = utils.diff_timestamps(utils.get_timestamp(), int(v))
            logger.debug('%s: %s: out-of-sync: %ss', self.url, k, self.diff)


def setup_logger(log_file, log_level):
    global logger
    level = logging.INFO
    if log_level == 'debug':
        level = logging.DEBUG
    elif log_level == 'error':
        level = logging.ERROR
    elif log_level == 'warning':
        level = logging.WARNING
    log_formatter = ('%(threadName)s::%(levelname)s::%(asctime)s::%(module)s'
                     '::%(lineno)d::%(name)s::(%(funcName)s) %(message)s')
    fmt = logging.Formatter(log_formatter)
    fh = logging.FileHandler(log_file)
    fh.setLevel(level)
    fh.setFormatter(fmt)
    logger.setLevel(level)
    logger.addHandler(fh)

def generate_dirs(remote_path, amount, ssh_args, ts_fname):
    dirs = utils.walk_find(remote_path, ssh_args)
    dirs.sort(key=lambda x: x.count('/'), reverse=True)
    sample_dirs = random.sample(dirs, min(amount, len(dirs) - 1))
    return ['%s/%s' % (x.strip(), ts_fname) for x in sample_dirs]


def load_config(config_fname):
    global defaults
    configs_yaml = {}
    with open(config_fname, 'r') as f:
        configs_yaml = yaml.load(f)

    configs = defaults.copy()
    configs.update(configs_yaml)
    return configs


def build_backend(backend_confs):
    backend_tasks = {}
    obj_task = namedtuple('obj_task', 'obj task')
    backend_obj = stamper(**backend_confs)


    if not backend_obj.dirs:
        backend_stamps = generate_dirs(backend_obj.remote_path,
                backend_obj.max_stamp,
                backend_obj.ssh_args,
                backend_confs.get('ts_fname'))
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


@app.route('/mirrors/<mirror_name>', methods=['GET'])
def get_mirrors(mirror_name):
    global backends
    backend = backends.get('resources.ovirt.org')
    if mirror_name == 'all':
        mirrors = []
        for k, v in backend.obj.mirrors_tasks.iteritems():
            mirrors.append('%s: %s\n' % (k, v.obj.diff))
        return ''.join(mirrors)

    mirror = backend.obj.mirrors_tasks.get(mirror_name)
    if mirror:
        return str(mirror.obj.diff)
    else:
        return 'mirror does not exist'


def main():
    global backends
    config_fname = 'mirrors.yaml'
    configs = load_config(config_fname)
    setup_logger(configs['log_file'], configs['log_level'])
    # to-do: add support for multiply backends
    backends = build_backend(configs.get('backends')[0])
    print backends
    for k, v in backends.iteritems():
        v.task.start(v.obj.stamp_interval)
        for k2, v2 in v.obj.mirrors_tasks.iteritems():
            v2.task.start(v2.obj.check_interval, now=False)
    flask_site = WSGIResource(reactor, reactor.getThreadPool(), app)
    root = Resource()
    root.putChild(configs['http_prefix'], flask_site)
    reactor.listenTCP(configs['http_port'], Site(root))
    reactor.run()

if __name__ == '__main__':
    main()
