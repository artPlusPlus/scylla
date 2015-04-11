import weakref

from .slot import Slot
from .request import put


class Output(Slot):
    def __init__(self, name, compute_handler, type_hint=None):
        super(Output, self).__init__(name, type_hint=type_hint)

        # self._compute_handler_ref = weakref.ref(compute_handler)
        self._compute_handler_ref = compute_handler

    # def get(self, client=None):
    #     handler = self._compute_handler_ref()
    #     if not handler:
    #         raise OutputError('Compute handler is dead.')
    #     return handler(client)

    def dirty(self):
        for connection in self._connections:
            put(connection, data={'dirty': True, 'url': self._url})

    # def connect(self, target):
    #     if not target._connect_source(self, False):
    #         raise OutputError('Connection not made')
    #     self._connect_target(target)
    #
    # def _connect_target(self, target):
    #     self._target_refs.add(target)
    #     return True
    #
    # def disconnect(self, target):
    #     target._disconnect_source(self)
    #     self._disconnect_target(target)
    #
    # def _disconnect_target(self, target):
    #     try:
    #         self._target_refs.remove(target)
    #     except ValueError:
    #         pass

    def to_json(self, request=None):
        result = super(Output, self).to_json()
        # result['value'] = self._compute_handler_ref()(request)
        result['value'] = str(self._compute_handler_ref(request))
        return result


class OutputError(Exception):
    pass