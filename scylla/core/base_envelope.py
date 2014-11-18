import weakref
import ujson
import msgpack


_ENVELOPE_REGISTRY = weakref.WeakValueDictionary()


class _MetaEnvelope(type):
    def __new__(cls, clsname, bases, dct):
        global _ENVELOPE_REGISTRY
        print 'REGISTERING ENVELOPE:', clsname
        new_cls = super(_MetaEnvelope, cls).__new__(cls, clsname, bases, dct)
        _ENVELOPE_REGISTRY[clsname] = new_cls
        return new_cls


class BaseEnvelope(object):
    __metaclass__ = _MetaEnvelope

    def __init__(self, *args, **kwargs):
        super(BaseEnvelope, self).__init__()

    def seal(self, destination_key):

        result = [destination_key,
                  self._pack(),
                  msgpack.packb(self.__class__.__name__)]

        return result

    def _pack(self):
        return ''

    @classmethod
    def unseal(cls, envelope_data):
        msg_data = msgpack.unpackb(envelope_data[1])
        return cls(**ujson.loads(msg_data))


def get_envelope_type(envelope_name):
    try:
        envelope_name = msgpack.unpackb(envelope_name)
    except (msgpack.UnpackException, msgpack.ExtraData):
        return None
    try:
        return _ENVELOPE_REGISTRY[envelope_name]
    except KeyError:
        return None