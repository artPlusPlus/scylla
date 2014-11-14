import weakref

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

    def __init__(self):
        super(BaseEnvelope, self).__init__()

    @classmethod
    def compress(cls, *args, **kwargs):
        pass

    @classmethod
    def expand(cls, message_data):
        pass


def get_envelope_type(envelope_name):
    try:
        envelope_name = msgpack.unpackb(envelope_name)
    except (msgpack.UnpackException, msgpack.ExtraData):
        return None
    try:
        return _ENVELOPE_REGISTRY[envelope_name]
    except KeyError:
        return None