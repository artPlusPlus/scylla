import zmq
import msgpack

from . import DEFAULT_PUB_PORT, DEFAULT_REQ_PORT


_PUB_SOCKETS = {}
_REQ_SOCKETS = {}


def publish(msg_type, msg_source, msg_body,
            host='localhost', port=DEFAULT_PUB_PORT):
    global _PUB_SOCKETS

    address = 'tcp://{0}:{1}'.format(host, port)
    try:
        socket = _PUB_SOCKETS[address]
    except KeyError:
        context = zmq.Context.instance()
        socket = context.socket(zmq.PUB)
        socket.setsockopt(zmq.LINGER, 1000)
        socket.connect(address)
        _PUB_SOCKETS[address] = socket

    message = (msg_type, msgpack.packb(msg_source), msgpack.packb(msg_body))
    socket.send_multipart(message)


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

    message = (msg_type, msgpack.packb(msg_source), msgpack.packb(msg_body))
    socket.send_multipart(message)
    reply = socket.recv_multipart()
    return reply


def terminate():
    while len(_PUB_SOCKETS):
        _, socket = _PUB_SOCKETS.popitem()
        socket.close()
    while len(_REQ_SOCKETS):
        _, socket = _REQ_SOCKETS.popitem()
        socket.close()
    zmq.Context.instance().term()