from ._uri import URI


class ConnectionCollection(object):
    def __init__(self):
        self._connection_order = []
        self._map_id_url = {}

    def append(self, url):
        self._connection_order.append(url.connection)
        self._map_id_url[url.connection] = url

    def remove(self, url):
        self._connection_order.remove(url.connection)
        del self._map_id_url[url.connection]

    def __contains__(self, item):
        if isinstance(item, URI):
            return item.connection in self._connection_order
        return item in self._connection_order

    def __getitem__(self, item):
        if isinstance(item, (int, slice)):
            try:
                return self._connection_order.__getitem__(item)
            except IndexError:
                raise IndexError()
        try:
            key = item.connection
        except AttributeError:
            key = item
        try:
            return self._map_id_url[key]
        except KeyError:
            raise KeyError()