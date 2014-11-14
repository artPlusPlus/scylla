import time
from threading import Thread
from contextlib import contextmanager

import zmq
import msgpack

from . import DEFAULT_MSG_PUB_PORT, DEFAULT_GLB_PUB_PORT, DEFAULT_REQ_PORT
from .base_envelope import get_envelope_type
from .simple_envelope import SimpleEnvelope


_PUB_SOCKETS = {}
_REQ_SOCKETS = {}


def publish(msg_target, msg_source, msg_type, msg_body,
            host='localhost', port=None):
    global _PUB_SOCKETS

    if not port:
        port = DEFAULT_MSG_PUB_PORT if msg_target else DEFAULT_GLB_PUB_PORT

    address = 'tcp://{0}:{1}'.format(host, port)
    try:
        socket = _PUB_SOCKETS[address]
    except KeyError:
        print 'creating pub socket:', address
        context = zmq.Context.instance()
        socket = context.socket(zmq.PUB)
        socket.setsockopt(zmq.LINGER, 1000)
        socket.connect(address)
        _PUB_SOCKETS[address] = socket

    # BUG - global subs are filtering by msg_type, while publishing puts msg_target first
    envelope = SimpleEnvelope.compress(msg_target, msg_source, msg_type, msg_body)
    # BUG

    print 'pub', port
    socket.send_multipart(envelope)


def subscribe(msg_filters, timeout, host='localhost', port=DEFAULT_GLB_PUB_PORT):
    result = []

    if isinstance(msg_filters, basestring):
        msg_filters = (msg_filters,)

    address = 'tcp://{0}:{1}'.format(host, port)
    context = zmq.Context.instance()
    socket = context.socket(zmq.SUB)
    socket.connect(address)
    for msg_filter in msg_filters:
        socket.setsockopt(zmq.SUBSCRIBE, str(msg_filter))

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    start = time.time()
    while time.time() - start < timeout:
        events = dict(poller.poll(timeout=timeout))
        if socket in events:
            msg_data = socket.recv_multipart()
            try:
                envelope = get_envelope_type(msg_data[-1])
                result.append(envelope.expand(msg_data))
            except AttributeError:
                result.append(msg_data)

    socket.close()
    return result


def ping(node_id=None, timeout=None, port=None):
    def pong_handler(_node_id, _timeout, _port, _result):
        if node_id:
            _result.extend(subscribe(_node_id, _timeout, port=_port))
        else:
            _result.extend(subscribe('pong', _timeout, port=_port))

    if not port:
        port = DEFAULT_MSG_PUB_PORT if node_id else DEFAULT_GLB_PUB_PORT

    result = []
    pong_thread = Thread(target=pong_handler, args=(node_id, timeout, port, result))
    pong_thread.start()
    publish(node_id, 'anon', 'ping', 'ping')
    pong_thread.join(timeout=timeout)
    try:
        return result[0]
    except IndexError:
        return result


def request(msg_type, msg_source, msg_body,
            host='localhost', port=DEFAULT_REQ_PORT):
    global _REQ_SOCKETS

    address = 'tcp://{0}:{1}'.format(host, port)
    try:
        socket = _REQ_SOCKETS[address]
    except KeyError:
        context = zmq.Context.instance()
        socket = context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 1000)
        socket.connect(address)
        _REQ_SOCKETS[address] = socket

    envelope = SimpleEnvelope.compress(address, msg_source, msg_type, msg_body)
    socket.send_multipart(envelope)
    msg_data = socket.recv_multipart()
    try:
        envelope = get_envelope_type(msg_data[-1])
        _reply = envelope.expand(msg_data)
    except AttributeError:
        _reply = msg_data
    return _reply


class reply(object):
    @property
    def request(self):
        return self._request

    def __init__(self, port=DEFAULT_REQ_PORT):
        self._port = port
        self._reply = None
        self._socket = None
        self._request = None

    def __enter__(self):
        context = zmq.Context.instance()
        socket = context.socket(zmq.REP)
        socket.bind('tcp://*:{0}'.format(self._port))

        request_data = socket.recv_multipart()
        try:
            envelope = get_envelope_type(request_data[-1])
            self._request = envelope.expand(request_data)
        except AttributeError:
            self._request = request_data
        return self

    def send(self, destination, sender, msg_type, msg_body):
        message = SimpleEnvelope.compress(destination, sender, msg_type, msg_body)
        self._socket.send(message)

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._socket.send('NO_REPLY')
        except:
            pass
        self._socket.close()


def terminate():
    while len(_PUB_SOCKETS):
        _, socket = _PUB_SOCKETS.popitem()
        socket.close()
    while len(_REQ_SOCKETS):
        _, socket = _REQ_SOCKETS.popitem()
        socket.close()
    zmq.Context.instance().term()