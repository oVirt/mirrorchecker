import functools
import time
import json
import argparse
import logging
import threading
import asyncio
import signal
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor

import yaml
import aiohttp
from aiohttp import web
import paramiko
from paramiko.ssh_exception import SSHException

LOGGER = 'mirror_checker'


class MirrorAPI(object):

    def __init__(self, loop, backend, host='localhost', port=8080):
        self.loop = loop
        self.backend = backend
        self.host = host
        self.port = port
        self.app = web.Application(loop=loop)
        self.handler = self.Hanlders(self.backend)
        self.app.router.add_route(
            'GET', '{0}/{1}'.format(
                self.backend.configs['http_prefix'], '{mirror_name}'
            ), self.handler.mirror
        )
        self.app.router.add_route(
            'GET', self.backend.configs['http_prefix'],
            self.handler.all_mirrors
        )
        self.app.router.add_route(
            'GET', '{0}/{1}'.format(
                self.backend.configs['http_prefix'],
                self.backend.configs['yum_mirror_request']
            ), self.handler.yum_mirrorlist
        )

        self.srv = None

    async def init_server(self):
        self.srv = await self.loop.create_server(
            self.app.make_handler(), self.host, self.port
        )
        return self.srv

    def shutdown(self):
        self.srv.close()
        asyncio.ensure_future(self.srv.wait_closed())
        asyncio.ensure_future(self.app.shutdown())
        asyncio.ensure_future(self.app.cleanup())

    class Hanlders(object):

        def __init__(self, backend):
            self.backend = backend

        async def all_mirrors(self, request):
            result = {}
            for mirror in self.backend.mirrors.values():
                if mirror.max_ts < 0:
                    result[mirror.url] = mirror.max_ts
                else:
                    seconds = int(time.time()) - mirror.max_ts
                    res = {
                        'in_seconds': seconds,
                        'in_minutes': round(seconds / 60, 2),
                        'in_hours': round(seconds / 60 / 60, 2)
                    }
                    result[mirror.url] = res
            return web.Response(
                body=json.dumps(
                    {'mirrors': result}, sort_keys=True, indent=4
                ).encode('utf-8')
            )

        async def mirror(self, request):
            return web.Response(text="single_mirror")

        async def yum_mirrorlist(self, request):
            results = (
                '{0}/{1}\n'.format(
                    mirror.url, self.backend.configs['yum_suffix']
                ) for mirror in self.backend.mirrors.values()
                if mirror.max_ts > 0 and (
                    int(time.time()) - mirror.max_ts <
                    self.backend.configs['yum_threshold']
                )
            )
            results = (
                result.replace('@VERSION@', request.match_info['version'])
                .replace('@DIST@', request.match_info['dist'])
                for result in results
            )
            return web.Response(body=''.join(results).encode('utf-8'))


class Backend(object):

    def __init__(self, loop, configs):
        self.loop = loop
        self.configs = configs
        if not configs.get('dirs', False):
            self.configs['dirs'] = self._generate_dirs()
        self.configs['dirs'] = [
            '/'.join([dir, self.configs['ts_fname']])
            for dir in self.configs['dirs']
        ]
        self.mirrors = self._build_mirrors()
        self.last_ts = -1
        self._scp_task = None
        self._cancel_event = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=5)

    def run(self):
        self._scp_task = self.loop.run_in_executor(
            self._executor,
            func=functools.partial(
                self._send_scp, self.configs, self._cancel_event
            )
        )

    async def shutdown(self):
        if self._scp_task:
            self._cancel_event.set()
            try:
                await asyncio.wait_for(self._scp_task, timeout=5.0)
            except SSHException:
                pass
            except asyncio.TimeoutError:
                pass
            self._scp_task.cancel()
            self._executor.shutdown(wait=True)
        for mirror in self.mirrors.values():
            mirror.shutdown()

    def _build_mirrors(self):
        mirrors = {}
        for mirror in self.configs['mirrors']:
            mirror = Mirror(
                loop=self.loop, files=self.configs['dirs'], **mirror
            )
            mirrors[mirror.url] = mirror
        return mirrors

    def _send_scp(self, configs, cancel_event):
        logger = logging.getLogger(LOGGER)

        while not cancel_event.is_set():
            try:

                with self._get_sftp(self.configs['ssh_args']) as sftp:
                    begin = time.time()
                    for file in configs['dirs']:
                        path = '/'.join([configs['remote_path'], file.strip()])
                        with sftp.open(path, 'w') as remote_file:
                            timestamp = int(time.time())
                            remote_file.write(str(timestamp))
                            self.last_ts = timestamp
                    end = time.time()
                    logger.info(
                        'sent %s files to %s:%s, took: %.4fs',
                        len(self.configs['dirs']),
                        self.configs['ssh_args']['hostname'],
                        self.configs['remote_path'], (end - begin)
                    )
                cancel_event.wait(configs['stamp_interval'])
            except SSHException:
                logger.exception('error sending files over SCP')
                if cancel_event.is_set():
                    raise

    def _generate_dirs(self):
        raise NotImplementedError

    def _discover_dirs(self):
        raise NotImplementedError

    @contextmanager
    def _get_sftp(self, ssh_args):
        with self._get_ssh(ssh_args) as ssh:
            sftp = ssh.open_sftp()
            yield sftp
            sftp.close()

    @contextmanager
    def _get_ssh(self, ssh_args):
        local_sshargs = dict(ssh_args)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        proxy_cmd = local_sshargs.pop('proxy_cmd', False)
        ssh_proxy = None
        if proxy_cmd:
            proxy_cmd = proxy_cmd.replace('%h', local_sshargs['hostname'])
            proxy_cmd = proxy_cmd.replace(
                '%p', local_sshargs.get('port', '22')
            )
            ssh_proxy = paramiko.ProxyCommand(proxy_cmd)
        ssh.connect(sock=ssh_proxy, **local_sshargs)
        yield ssh
        ssh.close()
        if ssh_proxy:
            ssh_proxy.close()


class Mirror(object):

    def __init__(self, loop, files, url, interval=90):
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
                fetched = 0
                begin = time.time()
                with aiohttp.ClientSession(loop=self.loop) as session:
                    results = await asyncio.gather(
                        *[
                            self._get_file(
                                session, '/'.join([self.url, file])
                            ) for file in self.files
                        ],
                        return_exceptions=True
                    )
                    for result in results:
                        if isinstance(result, asyncio.CancelledError):
                            raise asyncio.CancelledError(result.args)
                        if isinstance(result, Exception):
                            logger.warning('failed fetching: %s', repr(result))
                        else:
                            url, timestamp = result
                            if not timestamp:
                                logger.warning(
                                    'failed fetching %s, not found', url
                                )
                            else:
                                self.status[url] = timestamp
                                self.max_ts = max(
                                    int(self.max_ts), int(timestamp)
                                )
                                fetched = fetched + 1
                end = time.time()
                logger.info(
                    'fetched %s/%s files from %s, took: %.4fs', fetched,
                    len(self.files), self.url, end - begin
                )

                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                raise
            except:
                logger.exception('error fetching files from %s', self.url)


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
    log_formatter = (
        '%(threadName)s::%(levelname)s::%(asctime)s'
        '::%(lineno)d::(%(funcName)s) %(message)s'
    )
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
        'http_host': 'localhost',
        'http_prefix': 'api',
    }
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


async def shutdown(loop, backend, mirror_api):
    logger = logging.getLogger(LOGGER)
    mirror_api.shutdown()
    await backend.shutdown()
    tasks = [
        task for task in asyncio.Task.all_tasks()
        if task is not asyncio.tasks.Task.current_task()
    ]
    result = await asyncio.gather(*tasks, return_exceptions=True)
    logger.debug('results of all cancelled tasks: %s', result)
    logger.info('stopping event loop')
    loop.stop()


def exit_handler(sig, loop, backend, mirror_api):
    logger = logging.getLogger(LOGGER)
    logger.info('received %s, scheduling shutdown', sig)
    asyncio.ensure_future(shutdown(loop, backend, mirror_api))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config_file', type=str, default='mirrors.yaml'
    )
    args = parser.parse_args()
    configs = load_config(args.config_file)
    logger = setup_logger(configs['log_file'], configs['log_level'])
    flat_config = '\n'.join(
        '\t{}: {}'.format(key, val) for key, val in configs.items()
    )
    logger.info('loaded configuration:\n %s', flat_config)
    loop = asyncio.get_event_loop()
    backend = Backend(loop=loop, configs=configs['backends'][0].copy())
    mirror_api = MirrorAPI(
        loop=loop,
        backend=backend,
        port=configs['http_port'],
        host=configs['http_host']
    )
    for signame in ['SIGTERM']:
        loop.add_signal_handler(
            getattr(signal, signame), functools.partial(
                exit_handler, signame, loop, backend, mirror_api
            )
        )

    loop.run_until_complete(mirror_api.init_server())
    backend.run()
    try:
        logger.info('starting event loop')
        loop.run_forever()
    except:
        logger.exception('fatal exception')
        raise
    finally:
        loop.close()
        logger.info('exiting')


if __name__ == '__main__':
    main()
