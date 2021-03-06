"""
Copyright (c) 2015 Contributors as noted in the AUTHORS file

This file is part of scylla.

scylla is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

scylla is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with scylla.  If not, see <http://www.gnu.org/licenses/>.
"""

import time
from uuid import UUID

import zmq
import msgpack

from . import _ZMQ_CONTEXT
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
        if isinstance(node_id, basestring):
            self._id = UUID(node_id)
        else:
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


def ping(broadcast_port=50000, timeout=1000):
    result = []

    pong_host = _util.get_tcp_host()
    pong_sink = _ZMQ_CONTEXT.socket(zmq.PULL)
    pong_port = pong_sink.bind_to_random_port('tcp://*')

    _ping = Ping(pong_host, pong_port)
    _ping = _ping.pack()

    paddle = UDPSocket()
    paddle.send(_ping, broadcast_port)

    start = time.time()
    poller = zmq.Poller()
    poller.register(pong_sink, zmq.POLLIN)
    elapsed = 0
    while elapsed < timeout:
        elapsed = (time.time() - start)*1000
        try:
            events = dict(poller.poll(timeout/100))
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

    return result
