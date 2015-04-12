import socket

import _util


class UDPSocket(object):
    @property
    def socket(self):
        return self._socket

    @property
    def fileno(self):
        return self._socket.fileno()

    def __init__(self):
        self._host = None
        self._port = None

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def bind(self, port, host=None):
        self._host = host or _util.get_tcp_host()
        self._port = port
        self._socket.bind((self._host, self._port))

    def send(self, message, port, host='255.255.255.255'):
        self._socket.sendto(message, 0, (host, port))

    def recv(self, message_size=1024):
        message, _ = self._socket.recvfrom(message_size)
        return message

    def close(self):
        self._socket.close()

    def __del__(self):
        self._socket.close()