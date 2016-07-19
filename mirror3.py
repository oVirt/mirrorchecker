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
import threading
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
        self.srv = None

    async def init_server(self):
        self.srv = await self.loop.create_server(self.app.make_handler(),
                                                 self.host,
                                                 self.port)
        return self.srv

    async def shutdown(self):
            try:
                await self.srv.close()
                await self.loop.run_until_complete(self.srv.wait_closed())
                await self.loop.run_until_complete(self.app.shutdown())
                await self.loop.run_until_complete(self.app.cleanup())
            except asyncio.CancelledError:
                pass

    class Hanlders(object):
        def __init__(self, backend):
            self.backend = backend

        async def all_mirrors(self, request):
            result = {}
            for mirror in self.backend.mirrors.values():
                if mirror.max_ts < 0:
                    result[mirror.url] = mirror.max_ts
                else:
                    result[mirror.url] = int(time.time()) - mirror.max_ts
            return web.Response(
                body=json.dumps({'mirrors': result},
                                sort_keys=True,
                                indent=4).encode('utf-8'))


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
        self._scp_task = None
        self._cancel_event = threading.Event()
        # confirm directories exists or pick random dirs

    def run(self):
        self._scp_task = self.loop.run_in_executor(None,
                                                   func=functools.partial(
                                                       self._send_scp,
                                                       self.configs,
                                                   self._cancel_event))
    def shutdown(self):
        if self._scp_task:
            self._scp_task.cancel()
            self._cancel_event.set()
        for mirror in self.mirrors.values():
            mirror.shutdown()

    def _build_mirrors(self):
        mirrors = {}
        for mirror in self.configs['mirrors']:
            mirror = Mirror(loop=self.loop, files=self.configs['dirs'], **mirror)
            mirrors[mirror.url] = mirror
        return mirrors

    def _send_scp(self, configs, cancel_event):
        logger = logging.getLogger(LOGGER)
        while True and not self._cancel_event.is_set():
            try:
                with self._get_ssh() as ssh:
                    sftp = ssh.open_sftp()
                    begin = time.time()
                    for file in configs['dirs']:
                        path = '/'.join([configs['remote_path'], file.strip()])
                        with sftp.open(path, 'w') as remote_file:
                            timestamp = int(time.time())
                            remote_file.write(str(timestamp))
                            self.last_ts = timestamp
                    end = time.time()

                    logger.info('sent %s files to %s:%s, took: %.4fs',
                                 len(self.configs['dirs']),
                                self.configs['ssh_args']['hostname'],
                                self.configs['remote_path'],
                                (end-begin))


            except Exception as err:
                logger.exception('exception type:%s  %s, args: %s', type(err), type(err).__name__, err.args)
            time.sleep(configs['stamp_interval'])



    def _generate_dirs(self):
        raise NotImplementedError

    def _discover_dirs(self):
        raise NotImplementedError


    def _get_ssh(self):
        local_sshargs = dict(self.configs['ssh_args'])
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
        # except (IOError, socket.timeout, SSHException,
                # ProxyCommandFailure, BrokenPipeError) as err:


class Mirror(object):
    def __init__(self, loop, files, url, interval=90):
        # create task
        self.loop = loop
        self.files = files
        self.url = url
        self.interval = interval
        self.status = {}
        self.task = asyncio.ensure_future(self._aggr_files(), loop=self.loop)
        self.max_ts = -1

    def shutdown(self):
        self.task.cancel()

    async def _get_file(self, session, url):
        with aiohttp.Timeout(10):
            async with session.get(url) as response:
                timestamp = await response.text()
                if response.status != 200:
                    timestamp = None
                return (url, timestamp)

    async def _aggr_files(self):
        logger = logging.getLogger(LOGGER)
        while True:
            try:
                fetched = len(self.files)
                begin = time.time()
                with aiohttp.ClientSession(loop=self.loop) as session:
                    results = await asyncio.gather(
                        *[self._get_file(session, '/'.join([self.url, file]))
                        for file in  self.files],
                        return_exceptions=True)
                    for url, timestamp in results:
                        if not timestamp:
                            logger.warning('failed fetching %s', url)
                            fetched = fetched - 1
                        else:
                            self.status[url] = timestamp
                            self.max_ts = max(int(self.max_ts), int(timestamp))
                end = time.time()
                logger.info('fetched %s/%s files from %s, took: %.4fs', fetched,
                            len(self.files),
                            self.url,
                            end - begin)
            except:
                logger.exception('error fetching files from %s', self.url)
            finally:
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
    log_formatter = ('%(threadName)s::%(levelname)s::%(asctime)s::%(lineno)d::(%(funcName)s) %(message)s')
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
        loop.set_debug(enabled=True)
        backend = Backend(loop=loop, configs=configs['backends'][0].copy())
        mirror_api = MirrorAPI(loop=loop, backend=backend)
        loop.run_until_complete(mirror_api.init_server())
        backend.run()
        logger.info('starting event loop')
        loop.run_forever()
    except KeyboardInterrupt:
        logger.exception('caught keyboard interrupt')
        raise
    except Exception:
        logger.exception('caught fatal exception')
        raise
    finally:
        logger.info('starting shutdown')
        backend.shutdown()
        mirror_api.shutdown()
        loop.close()
        logger.info('shutdown complete')


if __name__ == '__main__':
    main()
