import zmq


class NodeHandle(object):
    @property
    def id(self):
        return self._id

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    def __init__(self, node_id, node_host, node_port):
        super(NodeHandle, self).__init__()

        self._id = node_id
        self._host = node_host
        self._port = node_port
        self._connection = None

    def connect(self, context=None):
        context = context or zmq.Context.instance()
        self._connection = context.socket(zmq.DEALER)
        self._connection.connect('tcp://{0}:{1}'.format(self._host, self._port))

    def send(self, message):
        self._connection.send(message)

    def close(self, linger=100):
        if not self._connection.closed:
            self._connection.close(linger=linger)

    def __del__(self):
        if not self._connection.closed:
            self._connection.close(linger=100)