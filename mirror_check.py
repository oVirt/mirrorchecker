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
from paramiko.ssh_exception import SSHException

defaults = {
    'log_level': 'debug',
    'log_file': 'mirror_checker.log',
    'http_port': 8080,
    'http_prefix': 'api'}
backend = None
app = Flask(__name__)
logger = logging.getLogger(__name__)


class stamper(object):
    """
    put timestamps in directories via SCP
    """
    def __init__(self, name, remote_path, ssh_args, mirrors, dirs=None,
                 max_stamp=10, stamp_interval='60', ts_fname=''):
        self.name = name
        self.ssh_args = ssh_args
        self.max_stamp = max_stamp
        self.remote_path = remote_path
        self.dirs = dirs
        self.stamp_interval = stamp_interval
        self.last_stamps = {}
        self.mirrors = mirrors
        self.mirror_tasks = {}
        self.ts_fname = ts_fname
        self.task = None

    def run(self):
        ssh, proxy = utils.get_ssh(self.ssh_args)
        sftp = ssh.open_sftp()
        begin = time.time()
        for stamp in self.dirs:
            path = '/'.join([self.remote_path, stamp.strip()])
            try:
                logger.debug('writing %s', path)
                with sftp.open(path, 'w') as f_ts:
                    timestamp = utils.get_timestamp()
                    f_ts.write(unicode(timestamp))
                    self.last_stamps[stamp] = int(timestamp)
            except SSHException:
                logger.exception('exception while stamping %s', path)
        end = time.time()
        logger.debug('renewed timestamps on %s dirs, took %ss',
                     len(self.dirs),
                     str(end-begin))

        ssh.close()
        proxy.close()

    def start(self):
        self.task.start(self.stamp_interval, now=False)


class mirror_site(object):
    """
    a mirror site status
    """
    def __init__(self, name, url, stamps='', check_interval=10):
        self.url = url
        self.stamps = stamps
        self.check_interval = check_interval
        self.name = name
        self.task = task.LoopingCall(self.run)
        self.diff = {x: -1 for x in self.stamps}

    def run(self):
        logger.debug('checking stamps for %s', self.url)
        dirs = utils.get_urlstamps(self.url, self.stamps)
        logger.debug('got stamps for %s\n%s', self.url, self.stamps)
        for key, value in dirs.iteritems():
            self.diff[key] = utils.diff_timestamps(utils.get_timestamp(),
                                                   value)

    def start(self):
        self.task.start(self.check_interval, now=False)

    def max_diff(self):
        return max(v for v in self.diff.itervalues())


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
    file_h = logging.FileHandler(log_file)
    file_h.setLevel(level)
    file_h.setFormatter(fmt)
    logger.setLevel(level)
    logger.addHandler(file_h)


def generate_dirs(dirs, remote_path, amount, ssh_args, ts_fname):
    if not dirs:
        dirs = utils.walk_find(remote_path, ssh_args)
        dirs.sort(key=lambda x: x.count('/'), reverse=True)
        dirs = random.sample(dirs, min(amount, len(dirs) - 1))
    return ['%s/%s' % (x.strip(), ts_fname) for x in dirs]


def load_config(config_fname):
    global defaults
    configs_yaml = {}
    try:
        with open(config_fname, 'r') as config_file:
            configs_yaml = yaml.load(config_file)
    except IOError:
        logging.error('failed to open %s, using defaults', config_file)
    configs = defaults.copy()
    configs.update(configs_yaml)
    return configs


def build_backend(backend_confs):
    global backend
    backend = stamper(**backend_confs)
    backend.dirs = generate_dirs(backend.dirs,
                                 backend.remote_path,
                                 backend.max_stamp,
                                 backend.ssh_args,
                                 backend.ts_fname)
    for mirror in backend.mirrors:
        mirror = mirror_site(stamps=backend.dirs,
                             **mirror)
        backend.mirror_tasks[mirror.name] = mirror
    backend.task = task.LoopingCall(backend.run)
    return backend


@app.route('/mirrors/<mirror_name>', methods=['GET'])
def get_mirrors(mirror_name):
    global backend
    if mirror_name == 'all':
        mirrors = []
        for name, mirror in backend.mirror_tasks.iteritems():
            mirrors.append('%s: %s\n' % (name, mirror.max_diff()))
        return ''.join(mirrors)

    mirror = backend.mirror_tasks.get(mirror_name)
    if mirror:
        return str(mirror.max_diff())
    else:
        return 'not found'


def main():
    global backend
    config_fname = 'mirrors.yaml'
    configs = load_config(config_fname)
    setup_logger(configs['log_file'], configs['log_level'])
    # to-do: add support for multiply backends
    backend = build_backend(configs.get('backends')[0])
    backend.start()
    for mirror in backend.mirror_tasks.values():
        mirror.start()
    flask_site = WSGIResource(reactor, reactor.getThreadPool(), app)
    root = Resource()
    root.putChild(configs['http_prefix'], flask_site)
    reactor.listenTCP(configs['http_port'], Site(root))
    reactor.run()

if __name__ == '__main__':
    main()
