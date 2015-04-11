import weakref

from .slot import Slot

from .request import get, put, delete
from .response import Response, Statuses


class Input(Slot):
    def __init__(self, name, put_handler, dirty_handler, pull_handler, type_hint=None, multi=False):
        super(Input, self).__init__(name, type_hint=type_hint)

        # self._put_handler = weakref.ref(put_handler)
        # self._dirty_handler = weakref.ref(dirty_handler)
        # self._pull_handler = weakref.ref(pull_handler)
        self._put_handler = put_handler
        self._dirty_handler = dirty_handler
        self._pull_handler = pull_handler
        self._multi = multi
        self._is_dirty = False

    def pull_value(self, request):
        result = {}
        if self._is_dirty:
            if self._connections:
                # Values are not cached. If any source changes, all are pulled
                for connection in self._connections:
                    result[connection] = get(self._connections[connection])
            else:
                # No connections, handler provides the last value 'put' into
                # the Input
                result[None] = self._pull_handler()
            self._is_dirty = False
            return True, result
        return False, result

    def _dirty(self, request):
        if self._is_dirty:
            return
        self._is_dirty = True
        handler = self._dirty_handler(request)
        if not handler:
            raise InputError('Dirty Handler is dead.')
        handler()

    def _get(self, request):
        response = super(Input, self)._get(request)
        request.data['multi'] = self._multi
        return response

    def _put(self, request):
        if self._connections:
            response = Response(
                request.client,
                Statuses.BAD_REQUEST,
                data='Unable to set value. Input has incoming connection(s).')
        else:
            try:
                if self._put_handler(request):
                    self._dirty(request)
                response = Response(request.client, Statuses.OK)
            except Exception, e:
                response = Response(request.client, Statuses.BAD_REQUEST, data=str(e))
        return response

    def _put_connection(self, request):
        if request.url.connection in self._connections:
            if 'dirty' in request.data:
                self._dirty(request)
                return

        force = request.data.get('force', False)
        try:
            connection_url = request.data['url']
        except KeyError:
            response = Response(request.client, Statuses.BAD_REQUEST,
                                data='Unable to connect. No connection URL provided.')
            return response

        if not self._multi:
            if force:
                source_url = '{0}/connections/{1}'.format(
                    self._connections[0], self._id)
                drop_response = delete(source_url)
                if drop_response.status in (Statuses.OK, Statuses.NOT_FOUND):
                    self._connections.pop()
                else:
                    response = Response(
                        request.client, Statuses.BAD_REQUEST,
                        data='Unable to connect. Failed to disconnect existing connection.')
                    return response
            else:
                response = Response(
                    request.client, Statuses.BAD_REQUEST,
                    data='Unable to connect. Input has incoming connection.')
                return response

        if request.client.connection != request.url.connection:
            connect_response = put(
                '{0}/connections/{1}'.format(connection_url, self._id),
                data={'force': force, 'connection': self._url})
            if connect_response.status is not Statuses.OK:
                response = Response(request.client, Statuses.BAD_REQUEST,
                                    data='Unable to connect. Source failed to connect.')
                return response

        self._connections.append(connection_url)
        self._dirty(request)
        return Response(request.client, Statuses.OK)


class InputError(Exception):
    pass