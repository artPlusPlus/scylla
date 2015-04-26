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

import weakref
import uuid

from .request import Methods, delete
from .response import Response, Statuses
from .connection_collection import ConnectionCollection


class Slot(object):
    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def type_hint(self):
        if isinstance(self._type_hint, weakref.ReferenceType):
            return self._type_hint()
        return self._type_hint

    def __init__(self, name, type_hint=None):
        super(Slot, self).__init__()

        self._id = uuid.uuid4()
        self._name = name
        self._connections = ConnectionCollection()

        try:
            self._type_hint = weakref.ref(type_hint)
        except TypeError:
            self._type_hint = type_hint

    def __call__(self, request):
        response = None
        if request.method == Methods.GET:
            if request.url.connection:
                response = self._get_connection(request)
            elif request.url.has_connections:
                response = self._get_connections(request)
            else:
                response = self._get(request)
        elif request.method == Methods.PUT:
            if request.url.connection:
                try:
                    connection = self._connections[request.url.connection]
                except KeyError:
                    response = Response(request.client, Statuses.NOT_FOUND)
                else:
                    response = connection(request)
            elif request.url.has_connections:
                response = self._put_connection(request)
            else:
                response = self._put(request)
        elif request.method == Methods.DELETE:
            if request.url.connection:
                response = self._delete_connection(request)

        if not response:
            if request.url.connection:
                data = [Methods.GET, Methods.PUT, Methods.DELETE]
            elif request.url.has_connections:
                data = [Methods.GET]
            else:
                data = [Methods.GET]
            response = Response(request.client,
                                Statuses.METHOD_NOT_ALLOWED,
                                data=data)
        return response

    def _get(self, request):
        response = Response(request.client,
                            Statuses.OK,
                            data=self.to_json())
        return response

    def _get_connections(self, request):
        response = Response(
            request.client, Statuses.OK,
            data=[c.to_json() for c in self._connections])
        return response

    def _get_connection(self, request):
        try:
            connection = self._connections[request.url.connection]
        except KeyError:
            response = Response(request.client, Statuses.NOT_FOUND)
        else:
            response = Response(request.client, Statuses.OK,
                                data=connection.to_json)
        return response

    def _put(self, request):
        response = Response(request.client,
                            Statuses.METHOD_NOT_ALLOWED,
                            data=['GET'])
        return response

    def _put_connection(self, request):
        response = Response(request.client,
                            Statuses.NOT_IMPLEMENTED,
                            data=['GET'])
        return response

    def _delete_connection(self, request):
        if request.client.connection != request.url.connection:
            # When the agent requesting disconnection is not the source
            # itself, we must request that the source drop us as a target
            source_url = '{0}/connections/{1}'.format(
                self._connections[request.url.connection], self._id)
            drop_response = delete(source_url)
            if drop_response.status not in (Statuses.OK, Statuses.NOT_FOUND):
                response = Response(
                    request.client, Statuses.BAD_REQUEST,
                    data='Unable to disconnect. Source failed to disconnect.')
                return response
        try:
            self._connections.remove(request.connection)
        except ValueError:
            response = Response(request.client, Statuses.NOT_FOUND)
        else:
            response = Response(request.client, Statuses.OK)
        return response

    def _connect(self, url):
        pass

    def to_json(self):
        result = {'id': str(self._id),
                  'name': self._name,
                  'slot_type': self.__class__.__name__,
                  'connections': [c.to_json() for c in self._connections]}

        try:
            result['data_type_hint'] = self.type_hint.__name__
        except AttributeError:
            result['data_type_hint'] = self.type_hint

        return result