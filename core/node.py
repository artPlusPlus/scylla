import uuid

from .output import Output
from .input import Input


class Node(object):
    @property
    def id(self):
        return self._id_field

    @property
    def name(self):
        return self._name_field

    def __init__(self, name):
        super(Node, self).__init__()

        self._id_out = Output(self)
        self._id_value = uuid.uuid4()

        self._name_in = Input(self)
        self._name_out = Output(self)
        self._name_value = name

    def _compute(self, output):
        if output is self._id_out:
            return self._id_value
        if output is self._name_out:
            if self._name_in.dirty:
                self._name_value = self._name_in.value
            return self._name_value

    @classmethod
    def resolve_node_type(cls, node_type):
        return None

    @classmethod
    def restore_node(cls, node_id, **node_data):
        pass
