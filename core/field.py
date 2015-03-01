import weakref


class FieldError(Exception):
    pass


class Field(object):
    @property
    def value(self):
        self._node._update(self)
        return self._output.value

    @value.setter
    def value(self, value):
        if self._input:
            self._input.value = value
        else:
            raise ValueError('Unable to set value. Field is read-only.')

    @property
    def _default_value(self):
        if isinstance(self._default_value_ref, weakref.ReferenceType):
            return self._default_value_ref()
        return self._default_value_ref

    @_default_value.setter
    def _default_value(self, value):
        try:
            self._default_value_ref = weakref.ref(value)
        except TypeError:
            self._default_value_ref = value

    @property
    def _node(self):
        if isinstance(self._node_ref, weakref.ReferenceType):
            return self._node_ref()
        return self._node_ref

    @property
    def dirty(self):
        if self._input:
            return self._input.dirty
        return False

    @property
    def _input_value(self):
        try:
            return self._input.value
        except AttributeError:
            return None

    @property
    def _output_value(self):
        try:
            return self._output.value
        except AttributeError:
            return None

    @_output_value.setter
    def _output_value(self, value):
        try:
            self._output.value = value
        except AttributeError:
            pass

    @property
    def type_hint(self):
        if isinstance(self._type_hint, weakref.ReferenceType):
            return self._type_hint()
        return self._type_hint

    def __init__(self, node, input=False, output=True, value=None, type_hint=None):
        super(Field, self).__init__()

        self._node_ref = weakref.ref(node)

        self._input = None
        if input:
            self._input = _Input(value=value)

        self._output = None
        if output:
            self._output = _Output(value=value)

        try:
            self._type_hint = weakref.ref(type_hint)
        except TypeError:
            self._type_hint = type_hint

    def connect_source(self, field, force=False):
        if not self._input:
            raise RuntimeError('Field is not an input.')
        try:
            source = field._output
        except AttributeError:
            raise RuntimeError('Unable to connect to source. Field is not an output.')
        if not source:
            raise RuntimeError('Unable to connect to source. Field has no output.')

        self._input.connect(source, force_disconnect=force)

    def connect_target(self, field):
        if not self._output:
            raise RuntimeError('Field is not an output.')
        try:
            target = field._input
        except AttributeError:
            raise RuntimeError('Unable to connect to target. Field is not an input.')
        if not input:
            raise RuntimeError('Unable to connect to target. Field has no input.')

        return self._output.connect(target)

    def _clean(self, node):
        if node is not self._node:
            return
        self._output_value = self._input_value
        if self._input:
            self._input.clean()


class _Input(object):
    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        if self.source:
            raise FieldError('Unable to set value. Input has source connection.')
        self._value_handler.send(value)

    @property
    def source(self):
        if isinstance(self._source_ref, weakref.ReferenceType):
            return self._source_ref()
        return self._source_ref

    @property
    def dirty(self):
        return self._dirty

    def __init__(self, value=None):
        super(_Input, self).__init__()

        self._value_handler = self._value_coroutine(self)
        self._value = value
        self._dirty = False
        self._source_ref = None

    def connect(self, source, force_disconnect=False):
        assert isinstance(source, _Output)

        original_source = self.source

        if not self._connect_source(source, force_disconnect):
            raise FieldError('Unable to connect source. Input connected to difference source.')

        if not source._connect_target(self):
            if original_source:
                self.connect(original_source)
                raise FieldError('Unable to connect source. Original source restored.')
            raise FieldError('Unable to connect source.')

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

    @staticmethod
    def _value_coroutine(input):
        def handler(input):
            while True:
                value = (yield)
                i = input()
                if not i:
                    continue
                i._dirty = value != i._value
                if i._dirty:
                    i._value = value
        cr = handler(weakref.ref(input))
        cr.next()
        return cr

    def clean(self):
        """
        Should be called by owner.
        """
        self._dirty = False

    def __call__(self, source, value):
        if self.source and source != self.source:
            raise FieldError('Unable to set value. Input has source connection.')
        self._value_handler.send(value)


class _Output(object):
    @property
    def value(self):
        if isinstance(self._value, weakref.ReferenceType):
            return self._value()
        return self._value

    @value.setter
    def value(self, value):
        if value == self.value:
            return
        self._value = value

        for target_ref in self._target_refs:
            target = target_ref()
            target(self, value)

    def __init__(self, value=None):
        super(_Output, self).__init__()

        self._value = None
        self._target_refs = weakref.WeakSet()

        self.value = value

    def connect(self, target):
        if not target._connect_source(self, False):
            raise RuntimeError('Connection not made')
        self._connect_target(target)

    def _connect_target(self, target):
        self._target_refs.add(target)
        target(self, self.value)
        return True

    def disconnect(self, target):
        target._disconnect_source(self)
        self._disconnect_target(target)

    def _disconnect_target(self, target):
        self._target_refs.remove(target)

    def _drop_target(self, ref):
        try:
            self._target_refs.remove(ref)
        except ValueError:
            return