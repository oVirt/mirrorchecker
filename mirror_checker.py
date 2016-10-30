#!/usr/bin/env python3
import argparse
import asyncio
import functools
import itertools
import json
import logging
import ast
from logging.handlers import WatchedFileHandler
import pprint
import re
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import aiohttp
from aiohttp import web
import paramiko
from paramiko.ssh_exception import SSHException
import yaml

LOGGER = 'mirror_checker'


class MirrorAPI(object):
    """MirrorAPI - A web server serving mirror sites status"""

    def __init__(self, loop, backend, host='localhost', port=8080):
        """Initialize the web server with a Backend object.

        Args:
            loop (asyncio.BaseEventLoop): event loop
            backend (Backend): backend object
            host (string): host name to bind
            port (int): web server port to bind
        """
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
                self.backend.configs['yum_request']
            ), self.handler.yum_mirrorlist
        )

        self.srv = None

    async def init_server(self):
        """Start the web server"""
        self.srv = await self.loop.create_server(
            self.app.make_handler(
                logger=logging.getLogger(LOGGER),
                access_log=logging.getLogger(LOGGER)
            ),
            self.host,
            self.port
        )
        return self.srv

    def shutdown(self):
        """Schedule shutdown of the web server"""
        self.srv.close()
        asyncio.ensure_future(self.srv.wait_closed())
        asyncio.ensure_future(self.app.shutdown())
        asyncio.ensure_future(self.app.cleanup())

    class Hanlders(object):
        """Web handlers for mirror status"""

        def __init__(self, backend):
            """__init__
            Args:
                backend (Backend): backend object

            """
            self.backend = backend

        async def all_mirrors(self, request):
            """Returns status of all mirror sites last synchronization time
            in JSON format.

            Args:
                request (aiohttp.web.Request):

            Returns:
                aiohttp.web.Response: response in JSON format
            """
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
            """Returns a single mirror synchronization status in seconds.

            Expected format is the full mirror path with all '/' replaced
            with '_'. For example:
            request:    'hostname.com_pub_ovirt_3.6'
            reply: status for 'http://hostname.com/pub/ovirt/3.6/'

            Args:
                request (aiohttp.web.Request): mirror hostname and path with all
                '/' replaced with '_'

            Returns:
                aiohttp.web.Response: response in JSON
            """
            mirror_req = request.match_info['mirror_name']
            mirror_req = mirror_req.replace('_', '/')
            mirror_req = 'http://{0}'.format(mirror_req)
            mirror = self.backend.mirrors.get(mirror_req, False)
            if mirror:
                if mirror.max_ts < 0:
                    res = mirror.max_ts
                else:
                    res = int(time.time()) - mirror.max_ts
                return web.Response(
                    body=json.dumps(
                        {mirror_req: res}, indent=4
                    ).encode('utf-8')
                )
            return aiohttp.web.HTTPNotFound()

        async def yum_mirrorlist(self, request):
            """Returns a list of mirrors file, filtered by Backend object
            filter command.
            Variables are substituted as defined in 'yum_response' and
            'yum_request' parameters.

            Example:
            given this configuration:
                http_prefix: '/mirrors'
                yum_request: 'yum/mirrorlist-ovirt-{version}-{dist}
                yum_response: 'ovirt-{version}/rpm/{dist}'

            request: http://localhost/mirrors/mirrorslist-ovirt-3.6-el7
            response: list of mirror sites returned by the Backend filter
            method with {version} and {dist} substituted:
                http://mirror1.com/pub/ovirt-3.6/rpm/el7
                http://mirror2.com/pub/linux/ovirt-3.6/rpm/el7
                ...


            Args:
                request (aiohttp.web.Response): A mirror list GET request

            Returns:
                aiohttp.web.Response: response in JSON
            """
            results = (
                '{0}/{1}\n'.format(
                    mirror.url, self.backend.configs['yum_response']
                ) for mirror in self.backend.filter()
            )
            replace = {
                '{{{0}}}'.format(k): v
                for k, v in request.match_info.items()
            }
            pattern = re.compile("|".join(replace.keys()))
            results = (
                pattern.sub(lambda m: replace[m.group(0)], result)
                for result in results
            )
            return web.Response(body=''.join(results).encode('utf-8'))


class Backend(object):
    """Represents a source site from which mirror sites pull data

    This is basically a directory on a remote site, that is reachable via SSH.
    Under the directories defined in the configs['dirs'] list, a timestamp
    will be uploaded on each interval.
    """

    def __init__(self, loop, configs):
        """Initalize the backend from the configuration dict

        Args:
               loop (asyncio.BaseEventLoop): event loop
               configs (dict): configurations

        """
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
        self._executor = ThreadPoolExecutor(max_workers=1)

    def run(self):
        """Start sending timestamps over SCP in a new thread"""
        self._scp_task = self.loop.run_in_executor(
            self._executor,
            func=functools.partial(
                self._send_scp, self.configs, self._cancel_event
            )
        )

    async def shutdown(self):
        """Attempt to close SCP thread, and cancel all mirroring site tasks"""
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
        """_build_mirrors"""
        mirrors = {}
        for mirror in self.configs['mirrors']:
            mirror = Mirror(
                loop=self.loop, files=self.configs['dirs'], **mirror
            )
            mirrors[mirror.url] = mirror
        return mirrors

    def _send_scp(self, configs, cancel_event):
        """Send timestamps over SCP

        Args:
               configs (dict): backend config
               cancel_event (threading.Event): runs until the event is set

        """
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
        """_generate_dirs"""
        raise NotImplementedError

    def _discover_dirs(self):
        """_discover_dirs"""
        raise NotImplementedError

    @contextmanager
    def _get_sftp(self, ssh_args):
        """A safe wrapper around paramiko.sftp_client.SFTPClient

        Args:
               ssh_args (dict): ssh arguments for SSHClient.connect() method

        Returns:
            paramiko.sftp_client.SFTPClient: a connected SFTP client
        """
        with self._get_ssh(ssh_args) as ssh:
            sftp = ssh.open_sftp()
            yield sftp
            sftp.close()

    @contextmanager
    def _get_ssh(self, ssh_args):
        """A safe wrapper around paramiko.SSHClient

        Closes ProxyCommand explicitly, if 'proxy_cmd' is used in the
        ssh arguments.

        Args:
               ssh_args (dict): ssh arguments for SSHClient.connect() method

        Returns:
             paramiko.client.SSHClient: a connected SSHClient
        """
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
        try:
            yield ssh
        finally:
            ssh.close()
            if ssh_proxy:
                ssh_proxy.close()

    def filter(self, custom_filters=None, custom_whitelist=None):
        """Apply filters to the mirrors and return a filtered list of mirrors

        Filters are in the form:
            lambda mirror: ....
        Where mirror is a Mirror object. The default filters are:

        Include all mirrors marked as 'whitelist' regardless of their state,
        Exclude unreachable mirrors,
        Exclude mirrors whose maximal timestamp difference from now is above
        'yum_threshold' parameter,
        return the union of the exclude and include.

        Mirror sites that matched ONE of the 'whitelist' filters, will always
        be returned.


           Args:
               custom_filters (list): a list of lambda function to append
               to the filters(logical OR)
               custom_whitelist (str): a list of lambda function to append
               to the whitelist filters(logical ANY)

            Returns:
                list: list of Mirror objects
        """
        return self._filter(self.mirrors, custom_filters, custom_whitelist)

    def _filter(self, mirrors, custom_filters=None, custom_whitelist=None):
        default_whitelist = [lambda mirror: mirror.whitelist]
        default_filters = [
            lambda mirror: mirror.reachable, lambda mirror: int(time.time()) -
            mirror.max_ts < self.configs['yum_threshold']
        ]
        custom_filters = [] if custom_filters is None else custom_filters
        custom_whitelist = [] if custom_whitelist is None else custom_whitelist

        filters = list(itertools.chain(default_filters, custom_filters))
        whitelist_filters = list(
            itertools.chain(default_whitelist, custom_whitelist)
        )

        def _filter_all(filters, elements):
            return filter(lambda t: all(f(t) for f in filters), elements)

        def _filter_any(filters, elements):
            return filter(lambda t: any(f(t) for f in filters), elements)

        whitelist = [
            mirror
            for mirror in _filter_any(whitelist_filters, mirrors.values())
        ]
        include = [mirror for mirror in _filter_all(filters, mirrors.values())]

        return list(set(whitelist) | set(include))


class Mirror(object):
    """A Mirror site synchronization status

    Represents a mirror site status. Pulls timestamps via http periodically,
    keeping max(current_time - timestamp)

    """

    def __init__(
        self, loop, files, url, interval=90, slow_start=0, whitelist=False
    ):
        """Initialize the mirror site object.

        Args:
               loop (asyncio.BaseEventLoop): event loop
               files (list): list of timestamp files
               url (str): mirror site URL
               interval (float): sample interval
               slow_start (int): number of first changes to keep the site still
               undiscovered.

        """
        self.loop = loop
        self.files = files
        self.url = url
        self.interval = interval
        self.status = {}
        self.task = asyncio.ensure_future(self._aggr_files(), loop=self.loop)
        self.max_ts = -1
        self.slow_start = slow_start
        self.whitelist = whitelist
        self.max_cache = [-1] * self.slow_start
        self.reachable = True

    def shutdown(self):
        """Cancel the sampling task"""
        self.task.cancel()

    async def _get_file(self, session, url):
        """Downloads a URL and converts it to text

        Args:
               session (aiohttp.ClientSession): web session
               url (str): url to collect

        Returns:
            tuple: url and text response
        """
        with aiohttp.Timeout(10):
            async with session.get(url) as response:
                timestamp = await response.text()
                if response.status != 200:
                    timestamp = None
                return (url, timestamp)

    async def _aggr_files(self):
        """Trigger coroutines to update timestamps and update maximal one"""
        logger = logging.getLogger(LOGGER)
        while True:
            try:
                fetched = 0
                errors = 0
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
                        elif isinstance(
                            result, (
                                aiohttp.errors.ClientConnectionError,
                                asyncio.TimeoutError
                            )
                        ):
                            logger.warning(
                                'unable to connect to %s: %s', self.url,
                                repr(result)
                            )
                            errors = errors + 1

                        else:
                            url, timestamp = result
                            if not timestamp:
                                logger.warning(
                                    'failed fetching %s, not found', url
                                )
                            else:
                                self.status[url] = int(timestamp)
                                fetched = fetched + 1
                end = time.time()
                local_max_ts = max(
                    [v for v in self.status.values()], default=-1
                )
                if self.slow_start > 0:
                    if self.max_cache[self.slow_start - 1] != local_max_ts:
                        self.slow_start = self.slow_start - 1
                        self.max_cache[self.slow_start - 1] = local_max_ts
                else:
                    self.max_ts = local_max_ts

                logger.info(
                    'fetched %s/%s files from %s, took: %.4fs', fetched,
                    len(self.files), self.url, end - begin
                )
                if errors == len(self.files) and self.reachable is not False:
                    logger.warning('marking %s as unreachable', self.url)
                    self.reachable = False
                elif errors < len(self.files) and self.reachable is not True:
                    logger.info('marking %s as reachable', self.url)
                    self.reachable = True

                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                raise
            except:
                logger.exception(
                    'fatal exception fetching files from %s', self.url
                )


def setup_logger(configs):
    """Setup logging

    Args:
           configs (dict): logging configuration


    Returns:
        logging.logger: the configured logger
    """

    # TO-DO: use logging.config.dictConfig instead
    logger = logging.getLogger(LOGGER)
    level = getattr(logging, configs['level'])
    log_formatter = (
        '%(threadName)s::%(levelname)s::%(asctime)s'
        '::%(lineno)d::(%(funcName)s) %(message)s'
    )
    fmt = logging.Formatter(log_formatter)
    logger.setLevel(level)
    if configs.get('file', False):
        file_h = WatchedFileHandler(configs.get('file'))
        file_h.setLevel(level)
        file_h.setFormatter(fmt)
        logger.addHandler(file_h)
    else:
        std_h = logging.StreamHandler(sys.stdout)
        std_h.setLevel(level)
        std_h.setFormatter(fmt)
        logger.addHandler(std_h)
    return logger


def _load_mirror_txt(mirror_fname):
    """Load mirrors list from CSV style file
    Args:
        config_fname (str): mirror file name

        expects the following format:
            url='<url>',key1=value1,key2=value2...

        URL is mandatory.
        All variables after the '=' are interpolated as Python literals.


    Returns:
        dict: mirrors dict
    """

    defaults = {
        'interval': 90,
        'whitelist': False,
    }
    with open(mirror_fname, 'r') as file:
        mirrors = {}
        for line in file.readlines():
            mirror = dict(
                pair.split('=', 1) for pair in line.strip().split(',')
            )
            mirror = {k: ast.literal_eval(v) for k, v in mirror.items()}
            if not mirror.get('url', False):
                raise ValueError('missing url key in {0}'.format(mirror_fname))
            elif mirrors.get(mirror.get('url')):
                raise ValueError(
                    'duplicate url: {0}'.format(mirror.get('url'))
                )
            merged = defaults.copy()
            merged.update(mirror)
            mirrors[merged.get('url')] = merged
        return list(mirrors.values())


def load_config(config_fname):
    """Load configuration from yaml

    Args:
           config_fname (str): yaml configuration file path

    Returns:
        dict: default configurations merged with yaml
    """
    defaults = {
        'logging': {'file': None,
                    'level': 'INFO'},
        'http_port': 8080,
        'http_host': 'localhost',
        'http_prefix': 'api',
    }
    configs_yaml = {}
    try:
        with open(config_fname, 'r') as config_file:
            configs_yaml = yaml.load(config_file)
            if configs_yaml['backends'][0].get('mirrors_file'):
                if configs_yaml['backends'][0].get('mirrors'):
                    print(
                        'duplicate definition of mirrors, using mirrors_file'
                    )
                    configs_yaml['backends'][0].pop('mirrors')
                mirrors = _load_mirror_txt(
                    configs_yaml['backends'][0].get('mirrors_file')
                )
                configs_yaml['backends'][0]['mirrors'] = mirrors

    except IOError:
        print('failed to open %s', config_fname)
        raise
    configs = defaults.copy()
    configs.update(configs_yaml)
    return configs


async def shutdown(loop, backend, mirror_api):
    """Attempt to cancel all tasks and close loop


    Args:
           loop (asyncio.BaseEventLoop): event loop
           backend (Backend): Backend object
           mirror_api (MirrorAPI): MirrorAPI object

    Returns:
        None: None
    """
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
    """Schedule the shutdown coroutine from signal


   Args:
           sig (int): signal number
           loop (asyncio.BaseEventLoop): event loop
           backend (Backend): Backend object
           mirror_api (MirrorAPI): MirrorAPI object

    """
    logger = logging.getLogger(LOGGER)
    logger.info('received %s, scheduling shutdown', sig)
    asyncio.ensure_future(shutdown(loop, backend, mirror_api))


def main():
    """main"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config_file', type=str, default='mirrors.yaml'
    )
    args = parser.parse_args()
    configs = load_config(args.config_file)
    logger = setup_logger(configs['logging'])
    logger.info('loaded configuration:\n%s', pprint.pformat(configs, depth=10))
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
