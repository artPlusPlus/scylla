import uuid

from .node import Node


class Graph(object):
    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    def __init__(self, name=None):
        super(Graph, self).__init__()

        self._id = uuid.uuid4()
        self._name = name
        self._nodes = {}

    def get_node(self, node_id):
        try:
            return self._nodes[node_id].to_json()
        except KeyError:
            pass
        return None

    def post_node(self, name, node_type, **node_data):
        print 'creating {0} node named {1}'.format(type, name)

        node_id = uuid.uuid4()
        self._nodes[node_id] = {'id': node_id,
                                'name': name,
                                'type': node_type,
                                'data': node_data}
        return node_id

    def put_node(self, node_id, **node_data):
        node = self._nodes[node_id]
        node_type = node_data.pop('type', None)
        if node_type:
            node_type = Node.resolve_node_type(node_type)
            if not node_type:
                raise RuntimeError('UNKNOWN NODE TYPE: {0}'.format(node_type))
            if node:
                if not isinstance(node, node_type):
                    node = node_type.restore_node(node_id, **node_data)
                    self._nodes[node_id] = node
        else:
            node.restore(**node_data)

        return True

    def delete_node(self, node_id):
        print 'deleting node: {0}'
        try:
            del self._nodes[node_id]
        except KeyError:
            return False
        return True

    def to_json(self):
        result = {'id': self._id,
                  'name': self._name,
                  'node_date': [n.to_json for n in self._nodes]}
        return result