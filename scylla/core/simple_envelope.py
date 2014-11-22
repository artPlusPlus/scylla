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

    def __init__(self, destination=None, sender=None, msg_type=None, msg_body=None):
        super(SimpleEnvelope, self).__init__()

        self._destination = str(destination) if destination else None
        self._sender = str(sender) if sender else None
        self._message_type = str(msg_type) if msg_type else None
        self._message_body = str(msg_body) if msg_body else None

    def _pack(self):
        msg_data = {'destination': self._destination,
                    'sender': self._sender,
                    'msg_type': self._message_type,
                    'msg_body': self._message_body}
        return msgpack.dumps(msg_data)
