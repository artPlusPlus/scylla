import request
from .response import Statuses
from .connection import Connection


class OutputConnection(Connection):
    def __init__(self, url):
        super(OutputConnection, self).__init__(url)

    def dirty(self, output_id):
        response = request.put(
            '{0}/connections/{1}'.format(self._url, output_id),
            client=output_id, data={'dirty': True})
        if response.status != Statuses.OK:
            raise OutputConnectionError('Failed to dirty {0}'.format(self._url))


class OutputConnectionError(Exception):
    pass