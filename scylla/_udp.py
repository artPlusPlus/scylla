"""
Copyright (c) 2015 Contributors as noted in the AUTHORS file

This file is part of scylla.

scylla is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

scylla is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with scylla.  If not, see <http://www.gnu.org/licenses/>.
"""

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