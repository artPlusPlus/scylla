from multiprocessing import Process
import socket
import time

import zmq

from . import DEFAULT_MSG_SUB_PORT, DEFAULT_GLB_SUB_PORT, DEFAULT_REQ_PORT
from .base_envelope import get_envelope_type
from .util import publish, terminate, ping


class Node(Process):
    @property
    def id(self):
        return '{0}.{1}'.format(socket.gethostname(), self.name)

    @property
    def host_ip(self):
        try:
            host_name = socket.getfqdn()
            return socket.gethostbyname(host_name)
        except socket.gaierror:
            host_name = socket.gethostname()
            return socket.gethostbyname(host_name)

    @property
    def _online(self):
        return all([self._direct_connection, self._global_connection])

    @property
    def online(self):
        if ping(node_id=self.id, timeout=1 * 1000):
            return True
        return False

    def __init__(self, name,
                 direct_message_port=DEFAULT_MSG_SUB_PORT,
                 global_stream_port=DEFAULT_GLB_SUB_PORT):
        super(Node, self).__init__(name=name)

        self.test = False

        self._run = True
        self._context = None
        self._poller = None
        self._socket_handlers = None

        self._direct_connection = False
        self._direct_port = direct_message_port
        self._direct_stream = None

        self._global_connection = False
        self._global_port = global_stream_port
        self._global_stream = None

        # TODO: weakref.WeakValueDictionary()
        self._direct_message_handlers = {}
        self._global_message_handlers = {}

    def start(self):
        super(Node, self).start()

        while not self.online:
            time.sleep(0.1)

    def run(self):
        self._run = True
        self._setup()
        self._main()
        self._teardown()

    def _setup(self):
        self._context = zmq.Context.instance()
        self._poller = zmq.Poller()
        self._socket_handlers = {}

        # Register basic direct message handlers
        self.register_direct_message_handler('prime_direct', self._received_direct_primer)
        self.register_direct_message_handler('stop', self._received_stop)
        self.register_direct_message_handler('ping', self._received_ping)

        # The direct message stream only receives messages addressed to this node
        self._direct_stream = self._context.socket(zmq.SUB)
        # print '[{0}]'.format(self.id), 'connecting direct sub', self._direct_port
        self._direct_stream.connect('tcp://localhost:{0}'.format(self._direct_port))
        self._direct_stream.setsockopt(zmq.SUBSCRIBE, str(self.id))
        self._register_socket(self._direct_stream, zmq.POLLIN, self._process_direct_message)

        # Register basic global message handlers
        self.register_global_message_handler('prime_global', self._received_global_primer)
        self.register_global_message_handler('ping', self._received_ping)

        # The global message stream receives messages based on handled types
        self._global_stream = self._context.socket(zmq.SUB)
        # print '[{0}]'.format(self.id), 'connecting global sub', self._global_port
        self._global_stream.connect('tcp://localhost:{0}'.format(self._global_port))
        for message_type in self._global_message_handlers:
            self._global_stream.setsockopt(zmq.SUBSCRIBE, str(message_type))
        self._register_socket(self._global_stream, zmq.POLLIN, self._process_global_message)

    def _main(self):
        while self._run:
            events = dict(self._poller.poll(timeout=1000))
            for _socket, event_mask in self._socket_handlers:
                if events.get(_socket) == event_mask:
                    handler = self._socket_handlers[(_socket, event_mask)]
                    envelope_data = _socket.recv_multipart()
                    handler(envelope_data)
            if not self._direct_connection:
                # print '[{0}] priming direct connection'.format(self.id)
                publish(self.id, self.id, 'prime_direct', 'prime_direct')
            if not self._global_connection:
                # print '[{0}] priming global connection'.format(self.id)
                publish(None, self.id, 'prime_global', 'prime_global')

    def _teardown(self):
        for _socket, _ in self._socket_handlers:
            _socket.close(linger=1000)
        publish(None, self.id, 'offline', 'offline')
        terminate()

    def register_direct_message_handler(self, message_type, handler):
        if message_type in self._direct_message_handlers:
            msg = '{0} already handled by {1}'.format(
                message_type, self._direct_message_handlers[message_type])
            raise ValueError(msg)

        self._direct_message_handlers[message_type] = handler

    def register_global_message_handler(self, message_type, handler):
        if message_type in self._global_message_handlers:
            msg = '{0} already handled by {1}'.format(
                message_type, self._global_message_handlers[message_type])
            raise ValueError(msg)

        self._global_message_handlers[message_type] = handler

        if self._global_stream:
            self._global_stream.setsockopt(zmq.SUBSCRIBE, str(message_type))

    def _register_socket(self, _socket, event_mask, handler):
        self._socket_handlers[(_socket, event_mask)] = handler
        self._poller.register(_socket, event_mask)

    def _process_direct_message(self, envelope_data):
        try:
            envelope = get_envelope_type(envelope_data[-1])
            envelope = envelope.unseal(envelope_data)
        except AttributeError:
            envelope = envelope_data
        try:
            handler = self._direct_message_handlers[envelope.message_type]
        except KeyError:
            print '[{0}] DROPPED MESSAGE - NO HANDLER: {1}'.format(self.id, envelope)
            return
        handler(envelope)

    def _process_global_message(self, envelope_data):
        try:
            envelope = get_envelope_type(envelope_data[-1])
            envelope = envelope.unseal(envelope_data)
        except AttributeError:
            envelope = envelope_data
        # print '[{0}]'.format(self.id), 'received global message', envelope
        try:
            handler = self._global_message_handlers[envelope.message_type]
        except KeyError:
            print '[{0}] DROPPED MESSAGE - NO HANDLER: {1}'.format(self.id, envelope)
            return
        handler(envelope)

    def _received_stop(self, envelope):
        publish(None, self.id, 'stopping', 'stopping')
        print '[{0}] STOPPING'.format(self.id)
        self._run = False

    def _received_direct_primer(self, envelope):
        if envelope.sender == self.id:
            self._direct_connection = True
            if self._global_connection:
                publish(None, self.id, 'online', 'online')
                print '[{0}] ONLINE'.format(self.id)

    def _received_global_primer(self, envelope):
        if envelope.sender == self.id:
            self._global_connection = True
            if self._direct_connection:
                publish(None, self.id, 'online', 'online')
                print '[{0}] ONLINE'.format(self.id)

    def _received_ping(self, envelope):
        if not self._online:
            return
        publish(envelope.sender, self.id, 'pong', 'tcp://{0}:{1}'.format(self.host_ip, self._direct_port))