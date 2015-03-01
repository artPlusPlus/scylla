import weakref

import zmq


class Slot(object):
    @property
    def id(self):
        if self._node:
            return '{0}:{1}'.format(self._node.id, self._name)
        return None

    @property
    def _node(self):
        try:
            return self._node_ref()
        except TypeError:
            return None

    @property
    def host_ip(self):
        if self._node:
            return self._node.host_ip
        return None

    @property
    def port(self):
        return self._port

    def __init__(self, name, node, port=None):
        super(Slot, self).__init__()

        self._name = name
        self._node_ref = weakref.ref(node)
        self._port = port
        self._socket = None


class Input(Slot):
    def __init__(self, name, node, port=None):
        super(Input, self).__init__(name, node, port=port)

        self._socket = self._node.context.socket(zmq.ROUTER)
        if not port:
            self._socket.bind_to_random_port('tcp://{0}'.format(self.host_ip))
        else:
            self._socket.bind('tcp://{0}:{1}'.format(self._node.host, port))


class Output(Slot):
    def __init__(self, name, node, port):
        super(Output, self).__init__(name, node, port=port)

        self._socket = self._node.context.socket(zmq.DEALER)
        self._socket.setsockopt(zmq.IDENTITY, self.id)

    def connect(self, host, port):
        self._socket.connect('tcp://{0}:{1}'.format(host, port))

    def __call__(self, message):
        self._socket.send(message)