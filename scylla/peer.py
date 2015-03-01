import time

from .node_handle import NodeHandle


class Peer(NodeHandle):
    DEFAULT_DURATION = 10.0

    @property
    def expired(self):
        return self._expiration < time.time()

    @property
    def duration(self):
        return self._duration

    @duration.setter
    def duration(self, value):
        self._duration = value

    def __init__(self, node_id, node_host, node_port, duration=None):
        super(Peer, self).__init__(node_id, node_host, node_port)

        self._duration = duration or Peer.DEFAULT_DURATION
        self._expiration = 0

        self.touch()

    def touch(self):
        self._expiration = time.time() + self._duration