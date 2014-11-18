import zmq

from . import (DEFAULT_MSG_PUB_PORT, DEFAULT_MSG_SUB_PORT,
               DEFAULT_GLB_PUB_PORT, DEFAULT_GLB_SUB_PORT)
from .node import Node


class Broker(Node):
    def __init__(self, name,
                 direct_message_port=DEFAULT_MSG_SUB_PORT,
                 global_stream_port=DEFAULT_GLB_SUB_PORT,
                 proxy_direct_pub_port=DEFAULT_MSG_SUB_PORT,
                 proxy_direct_sub_port=DEFAULT_MSG_PUB_PORT,
                 proxy_global_pub_port=DEFAULT_GLB_SUB_PORT,
                 proxy_global_sub_port=DEFAULT_GLB_PUB_PORT):
        super(Broker, self).__init__(
            name, direct_message_port=direct_message_port,
            global_stream_port=global_stream_port)

        self._proxy_direct_pub_port = proxy_direct_pub_port
        self._proxy_direct_pub = None

        self._proxy_direct_sub_port = proxy_direct_sub_port
        self._proxy_direct_sub = None

        self._proxy_global_pub_port = proxy_global_pub_port
        self._proxy_global_pub = None

        self._proxy_global_sub_port = proxy_global_sub_port
        self._proxy_global_sub = None

    def _setup(self):
        super(Broker, self)._setup()

        self._proxy_direct_pub = self._context.socket(zmq.XPUB)
        # print '[{0}]'.format(self.id), 'binding direct xpub', self._proxy_direct_pub_port
        self._proxy_direct_pub.bind(
            'tcp://*:{0}'.format(self._proxy_direct_pub_port))
        self._register_socket(self._proxy_direct_pub, zmq.POLLIN,
                              self._broker_direct_pub)

        self._proxy_direct_sub = self._context.socket(zmq.XSUB)
        # print '[{0}]'.format(self.id), 'binding direct xsub', self._proxy_direct_sub_port
        self._proxy_direct_sub.bind(
            'tcp://*:{0}'.format(self._proxy_direct_sub_port))
        self._register_socket(self._proxy_direct_sub, zmq.POLLIN,
                              self._broker_direct_sub)

        self._proxy_global_pub = self._context.socket(zmq.XPUB)
        # print '[{0}]'.format(self.id), 'binding global xpub', self._proxy_global_pub_port
        self._proxy_global_pub.bind(
            'tcp://*:{0}'.format(self._proxy_global_pub_port))
        self._register_socket(self._proxy_global_pub, zmq.POLLIN,
                              self._broker_global_pub)

        self._proxy_global_sub = self._context.socket(zmq.XSUB)
        # print '[{0}]'.format(self.id), 'binding global xsub', self._proxy_global_sub_port
        self._proxy_global_sub.bind(
            'tcp://*:{0}'.format(self._proxy_global_sub_port))
        self._register_socket(self._proxy_global_sub, zmq.POLLIN,
                              self._broker_global_sub)

    def _broker_direct_pub(self, msg_data):
        # print '[{0}]'.format(self.id), 'direct pub', msg_data
        self._proxy_direct_sub.send_multipart(msg_data)

    def _broker_direct_sub(self, msg_data):
        # print '[{0}]'.format(self.id), 'direct sub', msg_data
        self._proxy_direct_pub.send_multipart(msg_data)

    def _broker_global_pub(self, msg_data):
        # print '[{0}]'.format(self.id), 'global pub', msg_data
        self._proxy_global_sub.send_multipart(msg_data)

    def _broker_global_sub(self, msg_data):
        # print '[{0}]'.format(self.id), 'global sub', msg_data
        self._proxy_global_pub.send_multipart(msg_data)
