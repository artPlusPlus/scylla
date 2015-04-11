import socket


class UDPSocket(object):
    @property
    def fileno(self):
        return self._socket.fileno()

    def __init__(self, port, host=None, broadcast_host='255.255.255.255'):
        if host is None:
            for addr in socket.gethostbyname(socket.gethostname())[-1]:
                if not addr.startswith('127'):
                    host = addr
                    break

        self._host = host
        self._port = port
        self._broadcast_host = broadcast_host

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(('', port))

    def send(self, message):
        self._socket.sendto(message, 0, (self._broadcast_host, self._port))

    def receive(self, message_size=1024):
        message, origin = self._socket.recvfrom(message_size)
        origin_host, _ = origin
        if origin_host != self._host:
            return message
        return None