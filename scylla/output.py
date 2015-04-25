import weakref

from .slot import Slot
from .output_connection import OutputConnection, OutputConnectionError
from .response import Response, Statuses


class Output(Slot):
    @property
    def value(self):
        return self._compute_handler_ref(None)

    def __init__(self, name, compute_handler, type_hint=None):
        super(Output, self).__init__(name, type_hint=type_hint)

        # self._compute_handler_ref = weakref.ref(compute_handler)
        self._compute_handler_ref = compute_handler

    def dirty(self):
        for connection in self._connections:
            connection.dirty(self._id)

    def _get(self, request):
        # handler = self._compute_handler_ref()
        # if not handler:
        #     raise OutputError('Compute handler is dead.')
        response = super(Output, self)._get(request)
        response.data['value'] = str(self._compute_handler_ref(request))
        return response

    def _put_connection(self, request):
        try:
            target_url = request.data['url']
        except KeyError:
            msg = 'Unable to connect. No source URL provided.'
            return Response(request.client, Statuses.BAD_REQUEST, data=msg)

        connection = OutputConnection(target_url)
        self._connections.append(connection)
        try:
            connection.dirty(self._id)
        except OutputConnectionError, e:
            return Response(request.client, Statuses.BAD_REQUEST, data=e.message)
        return Response(request.client, Statuses.OK)

    def to_json(self):
        result = super(Output, self).to_json()
        result['value'] = str(self.value)
        return result


class OutputError(Exception):
    pass