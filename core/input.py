import weakref


class Input(object):
    @property
    def node(self):
        return self._node_ref()

    @property
    def type_hint(self):
        if isinstance(self._type_hint, weakref.ReferenceType):
            return self._type_hint()
        return self._type_hint

    @property
    def source(self):
        if isinstance(self._source_ref, weakref.ReferenceType):
            return self._source_ref()
        return self._source_ref

    def __init__(self, node, type_hint=None):
        super(Input, self).__init__()

        self._node_ref = weakref.ref(node)
        try:
            self._type_hint = weakref.ref(type_hint)
        except TypeError:
            self._type_hint = type_hint

    def connect(self, source, force_disconnect=False):
        original_source = self.source

        if not self._connect_source(source, force_disconnect):
            raise InputError('Unable to connect source. Input connected to difference source.')

        if not source._connect_target(self):
            if original_source:
                self.connect(original_source)
                raise InputError('Unable to connect source. Original source restored.')
            raise InputError('Unable to connect source.')

    def _connect_source(self, source, force_disconnect):
        if self.source:
            if source is self.source:
                return True
            elif not force_disconnect:
                return False
            self.source._disconnect_target(self)

        self._source_ref = weakref.ref(source, callback=self._drop_source)

        return True

    def _drop_source(self):
        self._source_ref = None

    def put(self, data):
        if self.source:
            raise ValueError('Unable to put data. Input has incoming connection')
        self._put(data)

    def _put(self, data):
        pass

    def get(self):
        pass

class InputError(Exception):
    pass