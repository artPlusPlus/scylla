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
        self._proxy_direct_sub_port = proxy_direct_sub_port
        self._proxy_direct = None

        self._proxy_global_pub_port = proxy_global_pub_port
        self._proxy_global_sub_port = proxy_global_sub_port
        self._proxy_global = None

    def _setup(self):
        super(Broker, self)._setup()

        self._proxy_direct = zmq.devices.ProcessDevice(zmq.QUEUE, zmq.XSUB, zmq.XPUB)
        self._proxy_direct.bind_in(
            'tcp://*:{0}'.format(self._proxy_direct_sub_port))
        self._proxy_direct.bind_out(
            'tcp://*:{0}'.format(self._proxy_direct_pub_port))
        self._proxy_direct.start()

        self._proxy_global = zmq.devices.ProcessDevice(zmq.QUEUE, zmq.XSUB, zmq.XPUB)
        self._proxy_global.bind_in(
            'tcp://*:{0}'.format(self._proxy_global_sub_port))
        self._proxy_global.bind_out(
            'tcp://*:{0}'.format(self._proxy_global_pub_port))
        self._proxy_global.start()
