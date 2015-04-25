from ._uri import URI
from .connection import Connection


class ConnectionCollection(object):
    def __init__(self):
        self._connection_order = []
        self._map__slot_id__url = {}

    def append(self, item):
        if not isinstance(item, Connection):
            msg = '{0} is not of type {0}'.format(item, Connection)
            raise TypeError(msg)
        self._connection_order.append(item.url.slot)
        self._map__slot_id__url[item.url.slot] = item

    def remove(self, item):
        if isinstance(item, basestring):
            self._connection_order.remove(item)
            del self._map__slot_id__url[item]
        elif isinstance(item, URI):
            self._connection_order.remove(item.slot)
            del self._map__slot_id__url[item.slot]
        elif isinstance(item, Connection):
            self._connection_order.remove(item.url.slot)
            del self._map__slot_id__url[item.url.slot]
        else:
            try:
                self._connection_order.remove(item)
                del self._map__slot_id__url[item.url.slot]
            except (KeyError, IndexError):
                msg = '{0} not found in {1}'.format(item, self)
                raise KeyError(msg)

    def __contains__(self, item):
        if isinstance(item, basestring):
            return item in self._connection_order
        elif isinstance(item, URI):
            return item.slot in self._connection_order
        elif isinstance(item, Connection):
            return item.url.slot in self._connection_order
        else:
            return item in self._connection_order

    def __iter__(self):
        for connection_id in self._connection_order:
            yield self._map__slot_id__url[connection_id]

    def __getitem__(self, item):
        if isinstance(item, int):
            try:
                key = self._connection_order[item]
                return self._map__slot_id__url[key]
            except IndexError:
                raise IndexError()
        elif isinstance(item, slice):
            try:
                keys = self._connection_order[item.start:]
                return [self._map__slot_id__url[key] for key in keys]
            except IndexError:
                raise IndexError()

        if isinstance(item, URI):
            key = item.slot
        else:
            key = item

        try:
            return self._map__slot_id__url[key]
        except KeyError:
            raise KeyError()

    def __len__(self):
        return len(self._connection_order)

    def __bool__(self):
        return bool(self._connection_order)

    # Python 2.x
    def __nonzero__(self):
        return self.__bool__()