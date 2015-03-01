import weakref


class Output(object):
    @property
    def node(self):
        return self._node_ref()

    @property
    def type_hint(self):
        if isinstance(self._type_hint, weakref.ReferenceType):
            return self._type_hint()
        return self._type_hint

    @property
    def targets(self):
        return [t() for t in self._target_refs]

    @property
    def value(self):
        return self.node._compute(self)

    def __init__(self, node, type_hint=None):
        super(Output, self).__init__()

        self._node_ref = weakref.ref(node)
        try:
            self._type_hint = weakref.ref(type_hint)
        except TypeError:
            self._type_hint = type_hint
        self._target_refs = weakref.WeakSet()

    def connect(self, target):
        if not target._connect_source(self, False):
            raise RuntimeError('Connection not made')
        self._connect_target(target)

    def _connect_target(self, target):
        self._target_refs.add(target)
        return True

    def disconnect(self, target):
        target._disconnect_source(self)
        self._disconnect_target(target)

    def _disconnect_target(self, target):
        try:
            self._target_refs.remove(target)
        except ValueError:
            pass