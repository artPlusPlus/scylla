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

import re
import uuid

_URI_SCHEME_PATTERN = r'(?P<SCHEME>.*)://'
_URI_HOST_PATTERN = r'(?P<HOST>[\d\w\.\:\-]*)/?'
_URI_PATH_PATTERNS = [r'(?P<GRAPHS>(?<=/)graphs/?)',
                      r'((?P<GRAPH>(?<=/graphs/)[\w-]*)/?)',
                      r'(?P<NODES>(?<=/)nodes/?)',
                      r'((?P<NODE>(?<=/nodes/)[\w-]*)/?)',
                      r'(?P<SLOTS>(?<=/)slots/?)',
                      r'((?P<SLOT>(?<=/slots/)[\w-]*)/?)',
                      r'(?P<CONNECTIONS>(?<=/)connections/?)',
                      r'((?P<CONNECTION>(?<=/connections/)[\w-]*)/?)']
_URI_PATTERN = '{0}{1}(?P<PATH>{2}?)?'.format(_URI_SCHEME_PATTERN,
                                              _URI_HOST_PATTERN,
                                              '?'.join(_URI_PATH_PATTERNS))
_URI_PARTS_RE = re.compile(_URI_PATTERN)


class URI(object):
    @property
    def scheme(self):
        return self._path_parts['SCHEME']

    @property
    def host(self):
        return self._path_parts['HOST']

    @property
    def has_graphs(self):
        return bool(self._path_parts['GRAPHS'])

    @property
    def graph(self):
        result = None
        if self.has_graphs:
            result = self._path_parts['GRAPH']
            if result:
                result = uuid.UUID(result)
        return result

    @property
    def has_nodes(self):
        return bool(self._path_parts['NODES'])

    @property
    def node(self):
        result = self._path_parts['NODE']
        if result:
            result = uuid.UUID(result)
        return result

    @property
    def has_slots(self):
        return bool(self._path_parts['SLOTS'])

    @property
    def slot(self):
        result = self._path_parts['SLOT']
        if result:
            result = uuid.UUID(result)
        return result

    @property
    def has_connections(self):
        if self.slot:
            return bool(self._path_parts['CONNECTIONS'])
        return False

    @property
    def connection(self):
        result = self._path_parts['CONNECTION']
        if result:
            result = uuid.UUID(result)
        return result

    @property
    def path(self):
        return self._path_parts['PATH']

    def __init__(self, uri):
        self._uri = str(uri)
        self._path_parts = _URI_PARTS_RE.match(self._uri).groupdict()

    def __str__(self):
        return self._uri

    def __repr__(self):
        return super(URI, self).__repr__()