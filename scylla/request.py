import uuid

import zmq
import msgpack

from ._uri import URI
from .response import Response


def request(client, method, url, data=None, timeout=1000):
    req = Request(client, method, url, data=data)
    s = zmq.Context.instance().socket(zmq.DEALER)
    s.identity = req.id
    s.connect(req.url.host)
    s.send(req.pack())
    resp = s.receive(timeout=timeout)
    if resp:
        resp = Response.unpack(resp)
    s.close()
    return resp


def get(url, data=None):
    return request(Methods.GET, url, data=data)


def put(url, data=None):
    return request(Methods.PUT, url, data=data)


def post(url, data=None):
    return request(Methods.POST, url, data=data)


def delete(url, data=None):
    return request(Methods.DELETE, url, data=data)


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
        result = {'client': self._client,
                  'method': self._method,
                  'url': str(self._url),
                  'data': self._data}
        return msgpack.packb(result)

    @classmethod
    def unpack(cls, packed_request):
        result = msgpack.unpackb(packed_request)
        result = cls(result['client'], result['method'],
                     result['url'], result['data'])
        return result