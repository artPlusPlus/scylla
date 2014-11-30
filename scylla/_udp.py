import socket

from . import DEFAULT_DISCOVERY_PORT


class UDPSocket(object):
    @property
    def fileno(self):
        return self._socket.fileno()

    def __init__(self, host=None, broadcast_host='255.255.255.255',
                 port=DEFAULT_DISCOVERY_PORT, recv_size=4096):
        self._host = host or socket.gethostbyname(socket.gethostname())
        self._broadcast_host = broadcast_host
        self._port = port
        self._recv_size = recv_size

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(('', port))

        self._buffer = ''

    def send(self, message):
        self._socket.sendto(message, 0, (self._broadcast_host, self._port))

    def receive(self, msg_len=None):
        msg = ''
        cursor = 0
        while not msg_len:
            if not self._buffer:
                self._buffer += self._socket.recv(self._recv_size)
            if not self._buffer[cursor].strip():
                msg_len = int(self._buffer[0:cursor])
                self._buffer = self._buffer[cursor+1:]
                break
            cursor += 1
        while len(msg) < msg_len:
            if not self._buffer:
                self._buffer += self._socket.recv(self._recv_size)
            chunk_size = min(len(self._buffer), msg_len-len(msg))
            chunk = self._buffer[:chunk_size]
            self._buffer = self._buffer[len(chunk):]
            msg += chunk
        return msg