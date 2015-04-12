import time

import zmq
import msgpack

from ._udp import UDPSocket
import _util


class Ping(object):
    @property
    def pong_addr(self):
        return 'tcp://{0}:{1}'.format(self._pong_host, self._pong_port)

    @property
    def pong_host(self):
        return self._pong_host

    @property
    def pong_port(self):
        return self._pong_port

    def __init__(self, pong_host, pong_port):
        self._pong_host = pong_host
        self._pong_port = pong_port

    def pack(self):
        result = {'pong_host': self._pong_host,
                  'pong_port': self._pong_port}
        return msgpack.packb(result)

    @classmethod
    def unpack(cls, packed_ping):
        result = msgpack.unpackb(packed_ping)
        result = cls(result['pong_host'], result['pong_port'])
        return result


class Pong(object):
    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    @property
    def url(self):
        return self._url

    def __init__(self, node_id, node_name, node_type, node_url):
        self._id = node_id
        self._name = node_name
        self._type = node_type
        self._url = node_url

    def pack(self):
        result = {'id': str(self._id),
                  'name': self._name,
                  'type': self._type,
                  'url': self._url}
        return msgpack.packb(result)

    @classmethod
    def unpack(cls, packed_pong):
        result = msgpack.unpackb(packed_pong)
        result = cls(result['id'], result['name'], result['type'],
                     result['url'])
        return result


def ping(broadcast_port=50000, timeout=1):
    result = []
    ctx = zmq.Context()

    pong_host = _util.get_tcp_host()
    pong_sink = ctx.socket(zmq.PULL)
    pong_port = pong_sink.bind_to_random_port('tcp://*')

    _ping = Ping(pong_host, pong_port)
    _ping = _ping.pack()

    paddle = UDPSocket()
    paddle.send(_ping, broadcast_port)

    start = time.time()
    poller = zmq.Poller()
    poller.register(pong_sink, zmq.POLLIN)
    while (time.time() - start) < timeout:
        try:
            events = dict(poller.poll(10))
        except KeyboardInterrupt:
            print "interrupt"
            break

        if pong_sink in events:
            _pong = pong_sink.recv()
            if not _pong:
                continue
            _pong = Pong.unpack(_pong)
            result.append(_pong)

    paddle.close()
    pong_sink.close()
    ctx.term()

    return result
