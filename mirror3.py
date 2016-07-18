import functools
import socket
import time
import json
import argparse
import logging
import asyncio
import aiohttp
from aiohttp import web
import yaml
import paramiko
from paramiko.ssh_exception import SSHException

LOGGER = 'mirror_checker'

class MirrorAPI(object):
    def __init__(self, loop, backend, host='127.0.0.1', port=8080):
        self.loop = loop
        self.backend = backend
        self.host = host
        self.port = port
        self.app = web.Application(loop=loop)
        self.handler = self.Hanlders(self.backend)
        self.app.router.add_route('GET', '/mirrors/{mirror_name}',
                                  self.handler.mirror)
        self.app.router.add_route('GET', '/mirrors', self.handler.all_mirrors)

    async def init_server(self):
        srv = await self.loop.create_server(self.app.make_handler(), self.host,
                                            self.port)
        return srv

    class Hanlders(object):
        def __init__(self, backend):
            self.backend = backend

        async def all_mirrors(self, request):
            result = {}
            for mirror in self.backend.mirrors.values():
                result[mirror.url] = int(time.time()) - mirror.max_ts
            return web.Response(
                body=json.dumps({'mirrors': result}).encode('utf-8'),
                content_type='application/json')

        async def mirror(self, request):
            return web.Response(text="single_mirror")


class Backend(object):

    def __init__(self, loop, configs):
        self.loop = loop
        self.configs = configs
        if not configs.get('dirs', False):
            self.configs['dirs'] = self._generate_dirs()
        self.configs['dirs'] = ['/'.join([dir,
                                          self.configs['ts_fname']])
                                for dir in self.configs['dirs']]
        self.mirrors = self._build_mirrors()
        self.last_ts = -1
        # confirm directories exists or pick random dirs

    def run(self):
        self.loop.run_in_executor(None,
                                  func=functools.partial(self._send_scp,
                                                         self.configs))

    def _build_mirrors(self):
        mirrors = {}
        for mirror in self.configs['mirrors']:
            mirror = Mirror(loop=self.loop, files=self.configs['dirs'], **mirror)
            mirrors[mirror.url] = mirror
        return mirrors

    def _send_scp(self, configs):
        while True:
            logger = logging.getLogger(LOGGER)
            with self._get_ssh() as ssh:
                sftp = ssh.open_sftp()
                begin = time.time()
                for file in configs['dirs']:
                    path = '/'.join([configs['remote_path'], file.strip()])
                    try:
                        logger.debug('%s: sending %s', configs['remote_path'],
                                     path)
                        with sftp.open(path, 'w') as remote_file:
                            timestamp = int(time.time())
                            remote_file.write(str(timestamp))
                            self.last_ts = timestamp
                    except SSHException:
                        logger.exception('%s: error sending file %s',
                                         configs['remote_path'], path)
                    end = time.time()
                logger.debug('%s: finished sending in %s',
                             configs['remote_path'], str(end-begin))
            time.sleep(configs['stamp_interval'])



    def _generate_dirs(self):
        raise NotImplementedError

    def _discover_dirs(self):
        raise NotImplementedError


    def _get_ssh(self):
        logger = logging.getLogger(LOGGER)
        local_sshargs = dict(self.configs['ssh_args'])
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            proxy_cmd = local_sshargs.pop('proxy_cmd', False)
            ssh_proxy = None
            if proxy_cmd:
                proxy_cmd = proxy_cmd.replace('%h', local_sshargs['hostname'])
                proxy_cmd = proxy_cmd.replace('%p', local_sshargs.get('port', '22'))
                ssh_proxy = paramiko.ProxyCommand(proxy_cmd)
            ssh.connect(sock=ssh_proxy, **local_sshargs)
            return ssh
        except (socket.gaierror, socket.error, socket.timeout) as err:
            logger.exception('error starting proxy_cmd %s', proxy_cmd)
            raise
        except SSHException as err:
            logger.exception('error setting up ssh connection')
            raise


class Mirror(object):
    def __init__(self, loop, files, url, interval=10):
        # create task
        self.loop = loop
        self.files = files
        self.url = url
        self.interval = interval
        self.status = {}
        self.task = asyncio.ensure_future(self._aggr_files(), loop=self.loop)
        self.max_ts = -1


    async def _get_file(self, session, url):
        with aiohttp.Timeout(10):
            async with session.get(url) as response:
                timestamp = await response.text()
                return (url, timestamp)

    async def _aggr_files(self):
        logger = logging.getLogger(LOGGER)
        while True:
            with aiohttp.ClientSession(loop=self.loop) as session:
                results = await asyncio.gather(
                    *[self._get_file(session, '/'.join([self.url, file]))
                      for file in  self.files],
                    return_exceptions=True)
                for url, timestamp in results:
                    self.status[url] = timestamp
                    self.max_ts = max(int(self.max_ts), int(timestamp))

                logger.info('got results: %s', self.status)
            await asyncio.sleep(self.interval)


def setup_logger(log_file, log_level):
    #TO-DO setup none-blocking logging with queue
    logger = logging.getLogger(LOGGER)
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
    return logger


def load_config(config_fname):
    defaults = {
        'log_level': 'debug',
        'log_file': 'mirror_checker.log',
        'http_port': 8080,
        'http_prefix': 'api'}
    configs_yaml = {}
    try:
        with open(config_fname, 'r') as config_file:
            configs_yaml = yaml.load(config_file)
    except IOError:
        print('failed to open %s', config_fname)
        raise
    configs = defaults.copy()
    configs.update(configs_yaml)
    return configs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_file', type=str, default='mirrors.yaml')
    args = parser.parse_args()
    configs = load_config(args.config_file)
    logger = setup_logger(configs['log_file'], configs['log_level'])
    flat_config = '\n'.join('\t{}: {}'.format(key, val) for key, val in configs.items())
    logger.info('loaded configuration:\n %s', flat_config)
    try:
        loop = asyncio.get_event_loop()
        backend = Backend(loop=loop, configs=configs['backends'][0].copy())
        mirror_api = MirrorAPI(loop=loop, backend=backend)
        loop.run_until_complete(mirror_api.init_server())
        backend.run()
        logger.info('starting event loop')
        loop.run_forever()
    except Exception:
        logger.exception('fatal exception, exiting: ')

if __name__ == '__main__':
    main()
