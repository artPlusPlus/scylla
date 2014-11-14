import msgpack

from .base_envelope import BaseEnvelope


class SimpleEnvelope(BaseEnvelope):
    @property
    def destination(self):
        return self._destination

    @property
    def sender(self):
        return self._sender

    @property
    def message_type(self):
        return self._message_type

    @property
    def message_body(self):
        return self._message_body

    def __init__(self, destination, sender, msg_type, msg_body):
        super(SimpleEnvelope, self).__init__()

        self._destination = destination
        self._sender = sender
        self._message_type = msg_type
        self._message_body = msg_body

    @classmethod
    def compress(cls, destination, sender, type, body):
        super(SimpleEnvelope, cls).compress()

        result = [destination or '',
                  msgpack.packb(sender),
                  msgpack.packb(type),
                  msgpack.packb(body),
                  msgpack.packb(cls.__name__)]
        return result

    @classmethod
    def expand(cls, message_data):
        super(SimpleEnvelope, cls).expand(message_data)

        destination = message_data[0] or None
        sender = msgpack.unpackb(message_data[1])
        msg_type = msgpack.unpackb(message_data[2])
        msg_body = msgpack.unpackb(message_data[3])

        return cls(destination, sender, msg_type, msg_body)