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

from ._uri import URI
from .request import Methods
from .response import Response, Statuses


class Connection(object):
    @property
    def url(self):
        return self._url

    def __init__(self, url):
        if isinstance(url, basestring):
            url = URI(url)
        self._url = url

    def to_json(self):
        result = {'url': str(self.url)}
        return result

    def __call__(self, request):
        response = None

        if request.method == Methods.POST:
            response = self._post(request)
        elif request.method == Methods.GET:
            response = self._get(request)
        elif request.method == Methods.PUT:
            response = self._put(request)
        elif request.method == Methods.DELETE:
            response = self._delete(request)

        return response

    def _post(self, request):
        response = Response(request.client, Statuses.METHOD_NOT_ALLOWED)
        return response

    def _get(self, request):
        response = Response(request.client, Statuses.OK, data=self.to_json())
        return response

    def _put(self, request):
        response = Response(request.client, Statuses.METHOD_NOT_ALLOWED)
        return response

    def _delete(self, request):
        response = Response(request.client, Statuses.METHOD_NOT_ALLOWED)
        return response


