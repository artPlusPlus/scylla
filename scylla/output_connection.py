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