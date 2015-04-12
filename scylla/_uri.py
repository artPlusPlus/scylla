import re
import uuid

_URI_SCHEME_PATTERN = r'(?P<SCHEME>.*)://'
_URI_HOST_PATTERN = r'(?P<HOST>.*?)/'
_URI_PATH_PATTERNS = [r'(?P<GRAPHS>graphs/)',
                      r'(?P<GRAPH>(?<=graphs/).*?)',
                      r'(?P<NODES>nodes/)',
                      r'(?P<NODE>(?<=nodes/).*)?',
                      r'(?P<SLOTS>slots/)',
                      r'(?P<SLOT>(?<=slots/).*)',
                      r'(?P<CONNECTIONS>/connections/)',
                      r'(?P<CONNECTION>(?<=/connections/).*)']
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
        result = None
        if self.has_nodes:
            result = self._path_parts['NODE']
            if result:
                result = uuid.UUID(result)
        return result

    @property
    def has_slots(self):
        return bool(self._path_parts['SLOTS'])

    @property
    def slot(self):
        result = None
        if self.has_slots:
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
        result = None
        if self.has_connections:
            result = self._path_parts['CONNECTION']
            if result:
                result = uuid.UUID(result)
        return result

    @property
    def path(self):
        return self._path_parts['PATH']

    def __init__(self, uri):
        self._uri = uri
        self._path_parts = _URI_PARTS_RE.match(uri).groupdict()

    def __str__(self):
        return self._uri

    def __repr__(self):
        super(URI, self).__repr__()