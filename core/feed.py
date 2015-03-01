import weakref


class FeedError(Exception):
    pass


class Feed(object):
    @property
    def _node(self):
        if isinstance(self._node_ref, weakref.ReferenceType):
            return self._node_ref()
        return self._node_ref

    @property
    def type_hint(self):
        if isinstance(self._type_hint, weakref.ReferenceType):
            return self._type_hint()
        return self._type_hint

    def __init__(self, node, input=False, output=False, type_hint=None):
        self._node_ref = weakref.ref(node)

        self._input = None
        if input:
            self._input = _FeedInput()

        self._output = None
        if output:
            self._output = _FeedOutput()

        try:
            self._type_hint = weakref.ref(type_hint)
        except TypeError:
            self._type_hint = type_hint

        try:
            self._type_hint = weakref.ref(type_hint)
        except TypeError:
            self._type_hint = type_hint

    def put(self, item):
        if self._input:
            self._input.value = value
        else:
            raise ValueError('Unable to set value. Field is read-only.')

    def get(self):
        self._node._update(self)
        return self._output.get()

    def __iter__(self):
        while True:
            yield self.get()


class _FeedInput(object):
    def __init__(self):
        super(_FeedInput, self).__init__()

        self._item_handler = _FeedInput._item_coroutine(self)
        self._items = []

    @property
    def source(self):
        if isinstance(self._source_ref, weakref.ReferenceType):
            return self._source_ref()
        return self._source_ref

    def connect(self, source, force_disconnect=False):
        assert isinstance(source, _FeedOutput)

        original_source = self.source

        if not self._connect_source(source, force_disconnect):
            raise FeedError('Unable to connect source. Input connected to difference source.')

        if not source._connect_target(self):
            if original_source:
                self.connect(original_source)
                raise FeedError('Unable to connect source. Original source restored.')
            raise FeedError('Unable to connect source.')

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

    def put(self, items):
        if self.source:
            raise Feed('Unable to feed items. Input has source connection.')


class _FeedOutput(object):
    def __init__(self):
        super(_FeedOutput, self).__init__()

        self._target_refs = weakref.WeakSet()
        self._items = []

    def connect(self, target):
        if not target._connect_source(self, False):
            raise RuntimeError('Connection not made')
        self._connect_target(target)

    def _connect_target(self, target):
        self._target_refs.add(target)
        return True

    def get_item(self, target):
        if target in self._target_refs:
            return self._items.pop(0)

    def __iter__(self):
        while True:
            yield self._items.pop(0)