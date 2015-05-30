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
