import weakref

from .slot import Slot
from .input_connection import InputConnection

from .response import Response, Statuses


class Input(Slot):
    @property
    def is_dirty(self):
        if self._connections:
            result = [c.dirty for c in self._connections]
        else:
            result = [self._is_dirty]
        return any(result)

    @property
    def is_multi(self):
        return self._multi

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

    def pull(self, request):
        result = {}
        if self._connections:
            for connection in self._connections:
                result[connection.url] = connection.pull(self._id)
        else:
            result[None] = self._pull_handler(request)
            self._is_dirty = False
        return result

    def _dirty(self, request):
        if self._is_dirty:
            return
        self._is_dirty = True
        # handler = self._dirty_handler()  # TODO: Weakref
        # if not handler:
        #     raise InputError('Dirty Handler is dead.')
        # handler(request)
        self._dirty_handler(request)

    def _connection_dirtied(self, connection, request):
        self._dirty(request)

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
                response = Response(request.client, Statuses.BAD_REQUEST, data=unicode(e))
        return response

    def _put_connection(self, request):
        try:
            source_url = request.data['url']
        except KeyError:
            msg = 'Unable to connect. No source URL provided.'
            return Response(request.client, Statuses.BAD_REQUEST, data=msg)

        if not self._multi and self._connections:
            msg = 'Unable to connect. Input has incoming connection.'
            return Response(request.client, Statuses.BAD_REQUEST, data=msg)

        connection = InputConnection(source_url, self._connection_dirtied)
        self._connections.append(connection)
        self._dirty(request)

        connection_url = '{0}/{1}'.format(request.url, connection.url.slot)
        return Response(request.client, Statuses.OK, data={'url': connection_url})

    def to_json(self):
        result = super(Input, self).to_json()
        result['dirty'] = self.is_dirty
        result['multi'] = self.is_multi
        return result


class InputError(Exception):
    pass