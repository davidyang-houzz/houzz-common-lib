from __future__ import absolute_import

import logging
import socket
import os
import signal
import sys
import threading

import requests

from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TNonblockingServer, THttpServer, TServer
from twitter.common import app, options

import houzz.common.py3_util
from houzz.common.module.config_module import ConfigModule
from houzz.common.module.perf_module import PerfModule
from houzz.common.service_register import ServiceRegister
import six

logger = logging.getLogger(__name__)


class ServiceModule(app.Module):
    NON_BLOCK_SERVER = 1
    TORNADO_SERVER = 2
    HTTP_SERVER = 3
    THREAD_POOL_SERVER = 4
    THREADED_SERVER = 5
    """
    Binds this application with The Configuration module
    """
    OPTIONS = {
        'zk_resolver_enable': options.Option(
            '--zk_resolver_enable',
            default=False, action='store_true', dest='zk_resolver_enable',
            help='True to Enable zk resolver.  Requires --zk_endpoint otherwise requires --service_config_dir'),
        'zk_endpoint': options.Option(
            '--zk_endpoint',
            default=None, dest='zk_endpoint',
            metavar='HOST[:PORT]',
            help='The ZooKeeper endpoint to talk to.  HOST or HOST:PORT pair.'),

        'identity': options.Option(
            '--identity',
            default=None, dest='identity',
            metavar='like /houzz/solr/ads/read/providers/prod',
            help='The service identity.'),

        'svc_host': options.Option(
            '--svc_host',
            dest='svc_host',
            help="the service host",
            default=None),

        'svc_port': options.Option(
            '--svc_port',
            dest='svc_port',
            type=int,
            help="the service port",
            default=None),

        'svc_type': options.Option(
            '--svc_type',
            dest='svc_type',
            type=int,
            help='Service Type NON_BLOCK_SERVER:{} TORNADO_SERVER:{} HTTP_SERVER:{} THREAD_POOL_SERVER:{} THREADED_SERVER:{}'.format(
                NON_BLOCK_SERVER, TORNADO_SERVER, HTTP_SERVER, THREAD_POOL_SERVER, THREADED_SERVER),
            default=None),

        'svc_process': options.Option(
            '--svc_process',
            dest='svc_process',
            type=int,
            help="for NON_BLOCK_SERVER is thread number, for the TORNADO_SERVER: number of the forked process",
            default=0),

        'svc_ec2': options.Option(
            '--svc_ec2',
            dest='svc_ec2',
            action="store_true",
            help="service on ec2"),

        'svc_k8s': options.Option(
            '--svc_k8s',
            dest='svc_k8s',
            action="store_true",
            help="service on K8S, need K8S_SVC_HOST config"),

        'svc_k8s_host': options.Option(
            '--svc_k8s_host',
            dest='svc_k8s_host',
            help="the k8s service hostname, which is SVC_NAME.NAMESPACE or elb domain",
            default=None),

        'svc_dis_perf': options.Option(
            '--dis_perf', dest='svc_dis_perf',
            metavar='DISABLE_AUTO_PERFORMANCE_COLLECT',
            action="store_true",
            help='disable perf collect')
    }

    @classmethod
    def full_class_path(cls):
        return cls.__module__ + '.' + cls.__name__

    def __init__(self):
        app.register_module(ConfigModule())
        app.register_module(PerfModule())
        dependencies = [
            ConfigModule.full_class_path(),
            PerfModule.full_class_path()
        ]
        app.Module.__init__(
            self,
            label=self.full_class_path(),
            dependencies=dependencies,
            description="HZ Service Module")
        self._host = None
        self._port = None
        self._svc_process = 0
        self._svc_type = ServiceModule.NON_BLOCK_SERVER
        self._register = None
        self._task_id = None
        self._ppid = None
        self._ec2 = False
        self._k8s = False
        self._non_blocking_server_ref = None

    def setup_function(self):
        options = app.get_options()
        config = ConfigModule().app_config
        port = options.svc_port
        host = options.svc_host
        self._svc_process = options.svc_process
        self._svc_type = options.svc_type
        self._ec2 = options.svc_ec2
        svc_k8s_host = options.svc_k8s_host
        self._k8s = options.svc_k8s and svc_k8s_host is not None        
        if config:
            port = port or config.get('PORT')
            host = host or config.get('HOST')
            self._svc_process = self._svc_process or config.get('FORK_NUM', 0)
            self._svc_type = self._svc_type or config.get('TYPE', ServiceModule.NON_BLOCK_SERVER)
            self._ec2 = self._ec2 or config.get('EC2', False)
            self._svc_k8s_host = svc_k8s_host or config.get('K8S_SVC_HOST', None)
            self._k8s = (self._k8s or config.get('K8S_SVC', False)) and self._svc_k8s_host is not None
        self._host = host
        self._port = port

    @staticmethod
    def _wrap_processor(processor):

        def wrapper_func(fn, method):
            return PerfModule.get_perf_count('{}|{}'.format(app.name(), method))(fn)

        for method, func in six.viewitems(processor._processMap):
            logger.info("wrap %s", method)
            processor._processMap[method] = wrapper_func(func, method)

    @staticmethod
    def _tornado_wrap_processor(processor):
        def callback_handler(handler_method):
            def handler_method_wrapper(self, *args, **kwargs):
                callback = None
                if 'callback' in kwargs:
                    callback = kwargs['callback']
                    del kwargs['callback']
                result = handler_method(self, *args, **kwargs)
                if callback:
                    callback(result)
                else:
                    return result

            return handler_method_wrapper

        for method, func in six.viewitems(processor._processMap):
            logger.info("tornado_wrap %s", method)
            processor._processMap[method] = callback_handler(func)

    def register(self, prop):
        options = app.get_options()
        config = ConfigModule().app_config

        zk_resolver_enable = options.zk_resolver_enable or config.get("ZK_RESOLVER_ENABLE")
        if not zk_resolver_enable:
            return
        zk_endpoint = options.zk_endpoint or config.get("ZK_ENDPOINT")
        identity = options.identity or config.get("IDENTITY")
        if not identity:
            logger.warning('identity of service is missing, you can\'t use discovery service to find it')
            return
        self._register = ServiceRegister(zk_endpoint, identity, (self.get_internal_ip(ec2=self._ec2, k8s_host=self._svc_k8s_host if self._k8s else None), self._port))
        self._register.register(prop)

    def serve(self, processor=None, svc_type=None, auto_perf=None, prop=None, threads=None, host=None, port=None,
              disable_discovery=False):
        """
        Args:
            processor (TProcessor)
            svc_type (str)
            auto_perf (bool)
            prop (dict)
            threads (int)
            disable_discovery (bool)
        """
        options = app.get_options()

        if host is None:
            host = self._host
        if port is None:
            port = self._port

        if not disable_discovery:
            self.register(prop)
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()

        if (svc_type or self._svc_type) in (ServiceModule.THREAD_POOL_SERVER, ServiceModule.THREADED_SERVER):
            tfactory = TTransport.TFramedTransportFactory()
        else:
            tfactory = TBinaryProtocol.TBinaryProtocolFactory()

        if port:
            transport = TSocket.TServerSocket(host=host, port=port)
        else:
            transport = TSocket.TServerSocket(unix_socket=host)

        if (svc_type or self._svc_type) == ServiceModule.TORNADO_SERVER:
            self._tornado_wrap_processor(processor)

        if processor and auto_perf is not False:
            if not options.svc_dis_perf:
                self._wrap_processor(processor)

        if (svc_type or self._svc_type) == ServiceModule.HTTP_SERVER:
            server = THttpServer.THttpServer(processor, (host, port), pfactory)
            logger.info("Starting http server...")
            server.serve()
        elif (svc_type or self._svc_type) == ServiceModule.TORNADO_SERVER:
            from thrift import TTornado
            from tornado import ioloop, netutil
            import tornado.process

            server = TTornado.TTornadoServer(processor, tfactory, pfactory)
            server.io_loop = None  # Will use the default IOLoop.
            logger.info("Starting server...")
            if port:
                socket = tornado.netutil.bind_sockets(port, address=host)
                tornado.process.fork_processes(self._svc_process)
                server.add_sockets(socket)
            else:
                socket = netutil.bind_unix_socket(host, mode=0o666)
                tornado.process.fork_processes(self._svc_process)
                server.add_socket(socket)

            for sig in [signal.SIGINT, signal.SIGTERM, ]:
                signal.signal(sig, self.terminate)
            # From this point, this is the child process
            self._ppid = ppid = os.getppid()
            self._task_id = tornado.process.task_id()
            for sig in [signal.SIGINT, signal.SIGTERM, ]:
                signal.signal(sig, self.terminate)

            logger.info('>>>>>>>>> Tornado process child starting')
            logger.info('parent: %s, child: %s' % (os.getppid(), os.getpid(),))

            logger.info('Starting event loop')

            def check_pid():
                """ Check For the existence of a unix pid. """
                try:
                    os.kill(ppid, 0)
                except OSError:
                    tornado.ioloop.IOLoop.instance().stop()
                    logger.info('child: %s stopped' % (os.getpid()))
                    return False
                else:
                    return True

            tornado.ioloop.PeriodicCallback(check_pid, 100).start()
            try:
                tornado.ioloop.IOLoop.instance().start()
            except KeyboardInterrupt:
                self.terminate()
        else:
            if (svc_type or self._svc_type) == ServiceModule.THREAD_POOL_SERVER:
                server = TServer.TThreadPoolServer(
                    processor, transport, tfactory, pfactory)
                if threads or self._svc_process:
                    server.setNumThreads(threads or self._svc_process)
            elif (svc_type or self._svc_type) == ServiceModule.THREADED_SERVER:
                server = TServer.TThreadedServer(
                    processor, transport, tfactory, pfactory)
            else:
                self._non_blocking_server_ref = server = TNonblockingServer.TNonblockingServer(
                    processor, transport, tfactory, pfactory, threads or self._svc_process or 10)

                server.prepare()

            if port is None:
                logger.debug('change socket permission to allow everyone')
                import stat
                os.chmod(host, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

            logger.info("Starting server...")
            server.serve()

    def get_process_count(self):
        return self._process_count

    def get_process_index(self):
        return self._task_id

    @staticmethod
    def get_internal_ip(ec2=False, k8s_host=None):
        if k8s_host is not None:
            logger.info('get_internal_ip: k8s {} to {}'.format(k8s_host, socket.gethostbyname(k8s_host)))
            return socket.gethostbyname(k8s_host)
        elif ec2:
            token = requests.put('http://169.254.169.254/latest/api/token', headers={'X-aws-ec2-metadata-token-ttl-seconds':'60'})
            req = requests.get('http://169.254.169.254/latest/meta-data/local-ipv4', timeout=0.005, headers={'X-aws-ec2-metadata-token': token.text})
            return houzz.common.py3_util.unicode2str(req.text)
        else:
            return socket.gethostbyname(socket.gethostname())

    def non_blocking_server_graceful_shutdown(self, timeout_in_secs=0):
        def shutdown_server():
            if self._non_blocking_server_ref:
                self._non_blocking_server_ref.stop()
                self._non_blocking_server_ref.close()
            sys.exit(0)

        def process_existing_reqs(*args, **kwargs):
            if timeout_in_secs <= 0:
                shutdown_server()

            # The purpose of using timer provided by the threading module is to facilitate its running
            # in its own separate thread. The Main thread is used by TNonblockingServer for processing of
            # requests. Using time.sleep API blocks that since the sleep also happens in the main thread and thus
            # the requests remain unfulfilled and are dropped, causing a loss of response. We want to be able
            # to process the exising requests before the server completely shuts down making it robust against
            # sudden interruptions and terminations

            logger.info("\n>>>>>>>>> terminating the interrupted pod in {0} secs".format(timeout_in_secs))
            t = threading.Timer(timeout_in_secs, shutdown_server)
            t.start()

        return process_existing_reqs

    def teardown_function(self):
        logger.debug(
            "ServiceModule starting teardown_function, self._register: %s, self._task_id: %s" %
            (self._register, self._task_id))
        if self._task_id is None and self._register:
            logger.info("ServiceModule unregister itself from zk.")
            self._register.unregister()
        logger.info("ServiceModule teardown_function Done.")

    def terminate(self, *args, **kwargs):
        logger.info(
            'terminate starts, pid: %s, ppid: %s' %
            (os.getpid(), self._ppid, ))
        if self._ppid:
            os.kill(self._ppid, signal.SIGINT)
            os._exit(0)
        else:
            sys.exit(0)
