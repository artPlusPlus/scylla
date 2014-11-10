from multiprocessing import Process
import socket

import zmq
import msgpack

from . import DEFAULT_SUB_PORT


class Node(Process):
    @property
    def id(self):
        return u'{0}.{1}'.format(socket.gethostname(), self.name)

    def __init__(self, name, message_port=None):
        super(Node, self).__init__(name=name)

        self._run = True
        self._context = None
        self._poller = None
        self._socket_handlers = None

        self._message_thread = None

        self._message_port = message_port or DEFAULT_SUB_PORT
        self._message_stream = None

        # TODO: weakref.WeakValueDictionary()
        self._message_handlers = {}

    def add_message_handler(self, message_type, handler):
        if message_type in self._message_handlers:
            msg = '{0} already handled by {1}'.format(
                message_type, self._message_handlers[message_type])
            raise ValueError(msg)
        self._message_handlers[message_type] = handler

    def _register_socket(self, _socket, event_mask, handler):
        self._socket_handlers[(_socket, event_mask)] = handler
        self._poller.register(_socket, event_mask)

    def run(self):
        self._run = True
        self._setup()
        self._main()
        self._teardown()

    def _setup(self):
        self._context = zmq.Context.instance()
        self._poller = zmq.Poller()
        self._socket_handlers = {}

        self._message_stream = self._context.socket(zmq.SUB)
        self._message_stream.connect('tcp://localhost:{0}'.format(self._message_port))
        self._message_stream.setsockopt(zmq.SUBSCRIBE, str(self.id))
        for message_type in self._message_handlers:
            self._message_stream.setsockopt(zmq.SUBSCRIBE, str(message_type))
        self._register_socket(self._message_stream, zmq.POLLIN, self._process_message)

    def _main(self):
        while self._run:
            for _socket, event_mask in self._poller.poll(timeout=1000):
                try:
                    handler = self._socket_handlers[(_socket, event_mask)]
                except KeyError:
                    print '[{0}] - NO HANDLER FOR SOCKET: {0}'.format(self.id, socket)
                    continue
                message = _socket.recv_multipart()
                handler(message)

    def _process_message(self, message):
        try:
            msg_type, msg_source, msg_body = message
            msg_source = msgpack.unpackb(msg_source)
            msg_body = msgpack.unpackb(msg_body)
        except ValueError:
            msg_body = message
            msg_type = None
            msg_source = None

        try:
            handler = self._message_handlers[msg_type]
        except KeyError:
            if msg_type != self.id:
                print '[{0}] DROPPED MESSAGE - NO HANDLER: {1}'.format(self.id, msg_body)
                return
            if msg_body == 'STOP':
                self._run = False
        else:
            handler(msg_type, msg_source, msg_body, self.id)

    def _teardown(self):
        for _socket, _ in self._socket_handlers:
            _socket.close(linger=1000)
        self._context.term()