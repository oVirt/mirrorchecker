import time
import utils
import logging
import random
import yaml
from flask import Flask, abort, jsonify
from twisted.web.resource import Resource
from twisted.internet import task
from twisted.internet import reactor
from twisted.web.wsgi import WSGIResource
from twisted.web.server import Site
from paramiko.ssh_exception import SSHException

DEFAULTS = {
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
        self.max_diff = -1
        filter_keys = ['task']
        self.exposes = {key: 1 for key in self.__dict__.iterkeys()
                        if key not in filter_keys}

    def run(self):
        logger.debug('checking stamps for %s', self.url)
        dirs = utils.get_urlstamps(self.url, self.stamps)
        logger.debug('got stamps for %s\n%s', self.url, self.stamps)
        for key, value in dirs.iteritems():
            self.diff[key] = utils.diff_timestamps(utils.get_timestamp(),
                                                   value)
        self.max_diff = self.__max_diff()

    def start(self):
        self.task.start(self.check_interval, now=False)

    def __max_diff(self):
        return max(v for v in self.diff.itervalues())

    def expose(self):
        return {'name': self.name,
                'max_diff': self.max_diff(),
                'all_diff': self.diff}


def setup_logger(log_file, log_level):
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
    configs_yaml = {}
    try:
        with open(config_fname, 'r') as config_file:
            configs_yaml = yaml.load(config_file)
    except IOError:
        logging.error('failed to open %s, using defaults', config_file)
    configs = DEFAULTS.copy()
    configs.update(configs_yaml)
    return configs


def build_backend(backend_confs):
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


@app.route('/mirrors', methods=['GET'])
def get_all_mirrors():
    result = [mirror for mirror in backend.mirror_tasks.iterkeys()]
    return jsonify({'mirrors': result})


@app.route('/mirrors/<mirror_name>', methods=['GET'])
def get_mirrors(mirror_name):
    mirror = backend.mirror_tasks.get(mirror_name, False)
    if mirror:
        result = {attr: mirror.__dict__.get(attr)
                  for attr in mirror.exposes.iterkeys()}
        return jsonify({mirror_name: result})
    else:
        return abort(404)


@app.route('/mirrors/<mirror_name>/<mirror_attr>', methods=['GET'])
def get_mirror_attr(mirror_name, mirror_attr):
    mirror = backend.mirror_tasks.get(mirror_name, False)
    if mirror and mirror.exposes.get(mirror_attr, False):
        return jsonify({mirror_attr: mirror.__dict__.get(mirror_attr, None)})
    else:
        abort(404)


def main():
    global backend
    config_fname = 'mirrors.yaml'
    configs = load_config(config_fname)
    setup_logger(configs['log_file'], configs['log_level'])
    # to-do: add support for multi backends
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
