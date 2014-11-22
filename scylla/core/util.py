import time
import socket
from threading import Thread

import zmq

from . import DEFAULT_MSG_PUB_PORT, DEFAULT_GLB_PUB_PORT, DEFAULT_REQ_PORT
from . import DEFAULT_MSG_SUB_PORT, DEFAULT_GLB_SUB_PORT
from .base_envelope import get_envelope_type
from .simple_envelope import SimpleEnvelope


_PUB_SOCKETS = {}
_REQ_SOCKETS = {}


def publish(msg_target, msg_source, msg_type, msg_body, host=None, port=None):
    global _PUB_SOCKETS

    host = host or get_host_ip()
    if not port:
        port = DEFAULT_MSG_PUB_PORT if msg_target else DEFAULT_GLB_PUB_PORT

    address = 'tcp://{0}:{1}'.format(host, port)
    try:
        _socket = _PUB_SOCKETS[address]
    except KeyError:
        context = zmq.Context.instance()
        _socket = context.socket(zmq.PUB)
        _socket.setsockopt(zmq.LINGER, 1000)
        _socket.connect(address)
        _PUB_SOCKETS[address] = _socket

    envelope = SimpleEnvelope(msg_target, msg_source, msg_type, msg_body)
    envelope = envelope.seal(msg_target if msg_target else msg_type)

    # print 'pub', port, msg_type
    _socket.send_multipart(envelope)


def subscribe(destination_keys, timeout, host=None, port=DEFAULT_GLB_PUB_PORT):
    result = []

    host = host or get_host_ip()
    if isinstance(destination_keys, basestring):
        destination_keys = (destination_keys,)

    address = 'tcp://{0}:{1}'.format(host, port)
    context = zmq.Context.instance()
    _socket = context.socket(zmq.SUB)
    _socket.connect(address)
    for dest_key in destination_keys:
        _socket.setsockopt(zmq.SUBSCRIBE, str(dest_key))

    poller = zmq.Poller()
    poller.register(_socket, zmq.POLLIN)
    events = dict(poller.poll(timeout=timeout))
    if _socket in events:
        msg_data = _socket.recv_multipart()
        try:
            envelope = get_envelope_type(msg_data[-1])
            result.append(envelope.unseal(msg_data))
        except AttributeError:
            result.append(msg_data)

    _socket.close()
    return result


def ping(node_id=None, timeout=None, host=None, port=None):
    def pong_handler(_timeout, _host, _port, _result):
        envelopes = subscribe('util', _timeout, host=_host, port=_port)
        envelopes = [e for e in envelopes if e.message_type == 'pong']
        if node_id:
            envelopes = [e for e in envelopes if e.sender == node_id]
        _result.extend(envelopes)

    if not port:
        port = DEFAULT_MSG_SUB_PORT if node_id else DEFAULT_GLB_SUB_PORT

    result = []
    pong_thread = Thread(target=pong_handler, args=(timeout, host, port, result))
    pong_thread.start()
    publish(node_id, 'util', 'ping', 'ping')
    pong_thread.join()
    try:
        return result[0]
    except IndexError:
        return None


def request(msg_type, msg_source, msg_body, host=None, port=DEFAULT_REQ_PORT):
    global _REQ_SOCKETS

    host = host or get_host_ip()

    address = 'tcp://{0}:{1}'.format(host, port)
    try:
        _socket = _REQ_SOCKETS[address]
    except KeyError:
        context = zmq.Context.instance()
        _socket = context.socket(zmq.REQ)
        _socket.setsockopt(zmq.LINGER, 1000)
        _socket.connect(address)
        _REQ_SOCKETS[address] = _socket

    envelope = SimpleEnvelope(address, msg_source, msg_type, msg_body)
    envelope = envelope.seal('')
    _socket.send_multipart(envelope)
    msg_data = _socket.recv_multipart()
    try:
        envelope = get_envelope_type(msg_data[-1])
        _reply = envelope.unseal(msg_data)
    except AttributeError:
        _reply = msg_data
    return _reply


class reply(object):
    @property
    def request(self):
        return self._request

    def __init__(self, host=None, port=DEFAULT_REQ_PORT):
        self._host = host or get_host_ip()
        self._port = port
        self._reply = None
        self._socket = None
        self._request = None

    def __enter__(self):
        context = zmq.Context.instance()
        self._socket = context.socket(zmq.REP)
        self._socket.bind('tcp://*:{0}'.format(self._port))

        request_data = self._socket.recv_multipart()
        try:
            envelope = get_envelope_type(request_data[-1])
            self._request = envelope.unseal(request_data)
        except AttributeError:
            self._request = request_data
        return self

    def reply(self, sender, msg_type, msg_body):
        message = SimpleEnvelope(self._request.sender, sender, msg_type, msg_body)
        message = message.seal(self._request.sender)
        self._socket.send_multipart(message)

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._socket.send('NO_REPLY')
        except:
            pass
        self._socket.close()


def terminate():
    while len(_PUB_SOCKETS):
        _, _socket = _PUB_SOCKETS.popitem()
        _socket.close()
    while len(_REQ_SOCKETS):
        _, _socket = _REQ_SOCKETS.popitem()
        _socket.close()
    zmq.Context.instance().term()


def get_host_ip():
    try:
        return socket.gethostbyname(socket.getfqdn())
    except socket.gaierror:
        return socket.gethostbyname(socket.gethostname())


def get_host_name(full=False):
    if full:
        return socket.getfqdn()
    return socket.gethostname()
