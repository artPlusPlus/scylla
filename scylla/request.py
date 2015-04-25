import time
import uuid

import zmq
import msgpack

from . import _ZMQ_CONTEXT
from ._uri import URI
from .response import Response, Statuses


def request(client, method, url, data=None, timeout=1000):
    start = time.time()

    req = Request(client, method, url, data=data)

    s = _ZMQ_CONTEXT.socket(zmq.REQ)
    s.identity = req.id.bytes
    s.connect('{0}://{1}'.format(req.url.scheme, req.url.host))
    s.send(req.pack())

    response = Response(client, Statuses.TIMEOUT)

    poller = zmq.Poller()
    poller.register(s, zmq.POLLIN)
    elapsed = 0
    while elapsed < (timeout/1000):
        elapsed = time.time()-start
        try:
            events = dict(poller.poll(timeout/100))
        except KeyboardInterrupt:
            print "interrupt"
            break

        if s in events:
            resp = s.recv()
            if not resp:
                continue
            response = Response.unpack(resp)
            break

    s.close()

    return response


def get(url, client=None, data=None):
    return request(client, Methods.GET, url, data=data)


def put(url, client=None, data=None):
    return request(client, Methods.PUT, url, data=data)


def post(url, client=None, data=None):
    return request(client, Methods.POST, url, data=data)


def delete(url, client=None, data=None):
    return request(client, Methods.DELETE, url, data=data)


class Methods(object):
    GET = 'GET'
    PUT = 'PUT'
    POST = 'POST'
    DELETE = 'DELETE'


class Request(object):
    @property
    def id(self):
        return self._id

    @property
    def client(self):
        return self._client

    @property
    def url(self):
        return self._url

    @property
    def method(self):
        return self._method

    @property
    def data(self):
        return self._data

    def __init__(self, client, method, url, data=None):
        self._id = uuid.uuid4()
        self._client = client
        self._method = method
        self._url = URI(url)
        self._data = data

    def pack(self):
        result = {'client': str(self._client),
                  'method': self._method,
                  'url': str(self._url),
                  'data': self._data}
        return msgpack.packb(result)

    @classmethod
    def unpack(cls, packed_request):
        result = msgpack.unpackb(packed_request)
        result = cls(result['client'], result['method'],
                     result['url'], data=result['data'])
        return result