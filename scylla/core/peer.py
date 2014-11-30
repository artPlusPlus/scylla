import time
import zmq


class Peer(object):
    @property
    def id(self):
        return self._id

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def expired(self):
        return self._expires_at < time.time()

    @property
    def duration(self):
        return self._duration

    @duration.setter
    def duration(self, value):
        self._duration = value

    def __init__(self, id, host, port, duration=10.0):
        super(Peer, self).__init__()

        self._id = id
        self._host = host
        self._port = port
        self._duration = duration
        self._expires_at = 0
        self._input = zmq.Context.instance().socket(zmq.DEALER)
        self._input.connect('tcp://{0}:{1}'.format(self._host, self._port))

        self.touch()

    def touch(self):
        self._expires_at = time.time() + self._duration

    def send(self, message):
        self._input.send(message)

    def stop(self, linger=1000):
        self._input.close(linger=linger)

    def __del__(self):
        self.stop()