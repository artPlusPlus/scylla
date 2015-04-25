from .response import Response, Statuses
import request
from .connection import Connection


class InputConnection(Connection):
    @property
    def dirty(self):
        return self._dirty

    def __init__(self, url, dirty_handler):
        super(InputConnection, self).__init__(url)

        self._dirty_handler = dirty_handler  #TODO: WeakMethod
        self._dirty = True
        self._value = None

    def pull(self, input_id):
        if self._dirty:
            resp = request.get(self._url, client=input_id)
            if resp.status == Statuses.OK:
                self._value = resp.data['value']
                self._dirty = False
            else:
                raise RuntimeError('failed to get value')
        return self._value

    def to_json(self):
        result = super(InputConnection, self).to_json()
        result['dirty'] = self._dirty
        result['value'] = self._value
        return result

    def _put(self, _request):
        response = None

        if _request.client == str(self._url.slot):
            try:
                dirty = _request.data['dirty']
            except (KeyError, TypeError):
                pass
            else:
                if dirty != self._dirty:
                    self._dirty = dirty
                    if self._dirty:
                        self._dirty_handler(self, _request)
                response = Response(_request.client, Statuses.OK)

        return response