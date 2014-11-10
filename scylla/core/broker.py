import zmq

from . import DEFAULT_PUB_PORT, DEFAULT_SUB_PORT
from .node import Node


class Broker(Node):
    def __init__(self, name, message_port=None,
                 proxy_pub_port=DEFAULT_SUB_PORT,
                 proxy_sub_port=DEFAULT_PUB_PORT):
        super(Broker, self).__init__(name, message_port=message_port)

        self._proxy_pub_port = proxy_pub_port
        self._proxy_pub = None

        self._proxy_sub_port = proxy_sub_port
        self._proxy_sub = None

    def _setup(self):
        super(Broker, self)._setup()

        self._proxy_pub = self._context.socket(zmq.XPUB)
        self._proxy_pub.bind('tcp://*:{0}'.format(self._proxy_pub_port))
        self._register_socket(self._proxy_pub, zmq.POLLIN, self._broker_pub_message)

        self._proxy_sub = self._context.socket(zmq.XSUB)
        self._proxy_sub.bind('tcp://*:{0}'.format(self._proxy_sub_port))
        self._register_socket(self._proxy_sub, zmq.POLLIN, self._broker_sub_message)

    def _broker_pub_message(self, message):
        self._proxy_sub.send_multipart(message)

    def _broker_sub_message(self, message):
        self._proxy_pub.send_multipart(message)
