"""
Forked from https://github.com/nylas/nylas-perftools.

Statistical profiling for long-running Python processes. This was built to work
with gevent, but would probably work if you ran the emitter in a separate OS
thread too.

Example usage
-------------
Add
>>> gevent.spawn(run_profiler, '0.0.0.0', 16384)

in your program to start the profiler, and run the emitter in a new greenlet.
Then curl localhost:16384 to get a list of stack frames and call counts.
"""

from __future__ import print_function
from __future__ import absolute_import
import logging
import sys
import threading
import collections
import time
from werkzeug.serving import BaseWSGIServer, WSGIRequestHandler
from werkzeug.wrappers import Request, Response

logger = logging.getLogger('stacksampler')


class Sampler(threading.Thread):
    """
    A simple stack sampler for low-overhead CPU profiling: samples the call
    stack every `interval` seconds and keeps track of counts by frame.
    """
    def __init__(self, interval=0.005):
        super(Sampler, self).__init__()
        self.interval = interval
        self._started = None
        self._stack_counts = collections.defaultdict(int)
        self.quit_event = threading.Event()
        self.setDaemon(True)

    def run(self):
        self._started = time.time()

        while not self.quit_event.is_set():
            self.quit_event.wait(self.interval)
            self._sample()

    def _sample(self):
        for _thread_id, frame in sys._current_frames().items():
            stack = []
            while frame is not None:
                stack.append(self._format_frame(frame))
                frame = frame.f_back
            stack = ';'.join(reversed(stack))
            self._stack_counts[stack] += 1

    def _format_frame(self, frame):
        return '{}({})'.format(frame.f_code.co_name,
                               frame.f_globals.get('__name__'))

    def output_stats(self):
        if self._started is None:
            return ''
        elapsed = time.time() - self._started
        lines = ['elapsed {}'.format(elapsed),
                 'granularity {}'.format(self.interval)]
        ordered_stacks = sorted(list(self._stack_counts.items()),
                                key=lambda kv: kv[1], reverse=True)
        lines.extend(['{} {}'.format(frame, count)
                      for frame, count in ordered_stacks])
        return '\n'.join(lines) + '\n'

    def reset(self):
        self._started = time.time()
        self._stack_counts = collections.defaultdict(int)

    def shutdown(self):
        self.quit_event.set()


class Emitter(object):
    """A really basic HTTP server that listens on (host, port) and serves the
    process's profile data when requested. Resets internal sampling stats if
    reset=true is passed."""
    def __init__(self, sampler, host, port):
        self.sampler = sampler
        self.host = host
        self.port = port

    def handle_request(self, environ, start_response):
        stats = self.sampler.output_stats()
        request = Request(environ)
        if request.args.get('reset') in ('1', 'true'):
            self.sampler.reset()
        response = Response(stats)
        return response(environ, start_response)

    def run(self):
        self.server = BaseWSGIServer(self.host, self.port, self.handle_request,
                                     _QuietHandler)
        self.server.log = lambda *args, **kwargs: None
        logger.info('Serving profiles on port {}'.format(self.port))
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()


class _QuietHandler(WSGIRequestHandler):
    def log_request(self, *args, **kwargs):
        """Suppress request logging so as not to pollute application logs."""
        pass


def run_profiler(host='0.0.0.0', port=16384):
    sampler = Sampler()
    sampler.start()
    e = Emitter(sampler, host, port)
    e.run()
