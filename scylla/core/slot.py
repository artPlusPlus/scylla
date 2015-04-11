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
        pass

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
        result = {'id': self._id,
                  'name': self._name,
                  'slot_type': self.__class__.__name__,
                  'data_type_hint': self._type_hint,
                  'connections': [c.to_json() for c in self._connections]}
        return result