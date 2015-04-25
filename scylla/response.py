import msgpack


class Statuses(object):
    OK = 200
    BAD_REQUEST = 400
    NOT_FOUND = 404
    METHOD_NOT_ALLOWED = 405
    NOT_IMPLEMENTED = 501
    TIMEOUT = 504


class Response(object):
    @property
    def client(self):
        return self._client

    @property
    def status(self):
        return self._status

    @property
    def data(self):
        return self._data

    def __init__(self, client, status, data=None):
        self._client = client
        self._status = status
        self._data = data

    def pack(self):
        result = {'client': self._client,
                  'status': self._status,
                  'data': self._data}
        return msgpack.packb(result)

    @classmethod
    def unpack(cls, packed_request):
        result = msgpack.unpackb(packed_request)
        result = cls(result['client'], result['status'], result['data'])
        return result