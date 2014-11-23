BROKER_ADDRESS = '192.168.0.130'

# Ports should fall somewhere between 49152 and 65535
_DEFAULT_PORT_START = 50000
DEFAULT_MSG_PUB_PORT = _DEFAULT_PORT_START + 1
DEFAULT_MSG_SUB_PORT = _DEFAULT_PORT_START + 2
DEFAULT_GLB_PUB_PORT = _DEFAULT_PORT_START + 3
DEFAULT_GLB_SUB_PORT = _DEFAULT_PORT_START + 4
DEFAULT_REQ_PORT = _DEFAULT_PORT_START + 5
DEFAULT_REP_PORT = _DEFAULT_PORT_START + 6

NODE_REQ_PORT_START = _DEFAULT_PORT_START + 100

from .node import Node
from .broker import Broker

from .base_envelope import BaseEnvelope
from .simple_envelope import SimpleEnvelope

from .util import publish, subscribe, ping, request, reply, terminate
