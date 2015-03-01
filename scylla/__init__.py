# Ports should fall somewhere between 49152 and 65535
_DEFAULT_PORT_START = 50000
DEFAULT_DISCOVERY_PORT = _DEFAULT_PORT_START + 1
DEFAULT_DIRECT_PORT_START = DEFAULT_DISCOVERY_PORT + 1

from .node import Node
from .node_handle import NodeHandle

# from .util import publish, subscribe, ping, request, reply, terminate
from .util import terminate