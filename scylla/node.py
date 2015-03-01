import time
from threading import Thread
from multiprocessing import Process, Pipe

import msgpack
import zmq
from zmq.eventloop.zmqstream import ZMQStream
from zmq.eventloop.ioloop import ZMQIOLoop, PeriodicCallback

from . import DEFAULT_DISCOVERY_PORT, DEFAULT_DIRECT_PORT_START
from ._udp import UDPSocket
from .peer import Peer
from .util import get_host_name, get_host_ip


class Node(object):
    @property
    def name(self):
        return self._name

    @property
    def id(self):
        return '{0}:{1}'.format(get_host_name().lower(), self.name.lower())

    @property
    def host_name(self):
        return get_host_name()

    @property
    def host_ip(self):
        return get_host_ip()

    @property
    def direct_port(self):
        return self._direct_port

    def __init__(self, name, port=None,
                 beacon_port=DEFAULT_DISCOVERY_PORT, beacon_rate=1.0):
        super(Node, self).__init__()

        self._name = name
        self._context = None
        self._loop = None
        self._peers = {}

        self._cnc_proxy_id = None
        self._cnc_cmd_socket = None
        self._cnc_result_port = None
        self._cnc_result_socket = None
        self._container = None

        self._beacon_port = beacon_port
        self._beacon_socket = None
        self._beacon_rate = beacon_rate
        self._next_beacon = time.time()
        self._beacon_callbacks = []

        self._user_port = port is not None
        self._direct_port = port or DEFAULT_DIRECT_PORT_START
        self._direct_socket = None
        self._direct_stream = None
        self._direct_handlers = {}  # TODO: weakref.WeakValueDictionary()

    def start(self, thread=False, process=False):
        if thread or process:
            _p1, _p2 = Pipe()
            self._cnc_proxy_id = '{0}_CNC'.format(self.id)
            if thread:
                self._container = Thread(name=self.id, target=self._start,
                                         args=(_p2, zmq.Context()))
                self._container.start()
            elif process:
                self._container = Process(name=self.id, target=self._start,
                                          args=(_p2, zmq.Context()))
                self._container.start()
            self._direct_port = _p1.recv()
            self._cnc_cmd_socket = zmq.Context.instance().socket(zmq.DEALER)
            self._cnc_cmd_socket.connect('tcp://localhost:{}'.format(self._direct_port))
            self._cnc_result_socket = zmq.Context.instance().socket(zmq.DEALER)
            self._cnc_result_port = self._cnc_result_socket.bind_to_random_port(
                'tcp://*', min_port=int(self._direct_port))
        else:
            self._start(None, None)

    def _start(self, cnc_address_pipe, context):
        self._setup(cnc_address_pipe, context)
        self._main()
        self._teardown()

    def _setup(self, cnc_address_pipe, context):
        print '[{0}] setting up'.format(self.id)
        self._context = context or zmq.Context.instance()
        self._loop = ZMQIOLoop()

        # Beacon Messages
        self._beacon_socket = UDPSocket(port=self._beacon_port)
        self._loop.add_handler(self._beacon_socket.fileno,
                               self._receive_beacon_message,
                               self._loop.READ)
        cb = PeriodicCallback(self._emit_beacon, self._beacon_rate * 1000,
                              io_loop=self._loop)
        self._beacon_callbacks.append(cb)
        cb = PeriodicCallback(self._cull_peers, self._beacon_rate * 2000,
                              io_loop=self._loop)
        self._beacon_callbacks.append(cb)

        # Direct Messages
        self._direct_socket = self._context.socket(zmq.ROUTER)
        self._direct_port = self._direct_socket.bind_to_random_port(
            'tcp://*', min_port=int(self._direct_port))
        self._direct_stream = ZMQStream(self._direct_socket, io_loop=self._loop)
        self._direct_stream.on_recv(self._receive_direct_message)
        self.on_recv_direct('stop', self._handle_stop)
        self.on_recv_direct('ping', self._handle_ping)
        self.on_recv_direct('pong', self._handle_pong)

        # Command and Control
        if cnc_address_pipe:
            cnc_address_pipe.send(str(self._direct_port))

    def _main(self):
        print '[{0}] running'.format(self.id)
        for bcb in self._beacon_callbacks:
            bcb.start()
        self._loop.start()

    def _teardown(self):
        print '[{0}] stopping'.format(self.id)
        for peer_id in self._peers.keys():
            self._remove_peer(peer_id)
        for s in [self._direct_socket]:
            try:
                s.close(linger=1000)
            except AttributeError:
                pass
        if self._context is not zmq.Context.instance():
            self._context.term()
        print '[{0}] offline'.format(self.id)

    def on_recv_direct(self, message_type, handler):
        if message_type in self._direct_handlers:
            msg = '{0} already handled by {1}'.format(
                message_type, self._direct_handlers[message_type])
            raise ValueError(msg)

        self._direct_handlers[message_type] = handler

    def _emit_beacon(self):
        if self._next_beacon < time.time():
            msg = self._construct_beacon_msg()
            msg = msgpack.dumps(msg)
            self._beacon_socket.send('{0} {1}'.format(len(msg), msg))
            self._next_beacon = time.time() + self._beacon_rate

    def _cull_peers(self):
        for peer in self._peers.values():
            if peer.id == self._cnc_proxy_id:
                continue
            if peer.expired:
                print '[{0}] culling peer:'.format(self.id), peer.id
                self._remove_peer(peer.id)

    def _process_message(self, raw_message):
        message = msgpack.loads(raw_message)
        peer_id, peer_address, peer_port = message.get('node', (None, None, None))
        msg_type = message.get('type', None)
        msg_body = message.get('body', None)

        peer = None
        if peer_id == self.id:
            peer = self
        elif all([peer_id, peer_address, peer_port]):
            try:
                peer = self._peers[peer_id]
                peer.touch()
            except KeyError:
                peer = self._add_peer(peer_id, peer_address, peer_port)

        return peer, msg_type, msg_body

    def _receive_beacon_message(self, fd, event):
        message = self._beacon_socket.receive()
        peers = self._peers.keys()
        peer, msg_type, message = self._process_message(message)

        if peer and peer is not self and peer.id not in peers:
            ping = self._construct_ping_msg()
            ping = msgpack.dumps(ping)
            peer.send(ping)

    def _receive_direct_message(self, raw_message):
        peer, message_type, message = self._process_message(raw_message[-1])
        if peer is self:
            return

        try:
            handler = self._direct_handlers[message_type]
        except KeyError:
            print '[{0}] DROPPED MESSAGE - NO HANDLER: {1}'.format(self.id, message_type)
            return
        handler(peer, message)

    def _handle_ping(self, peer, message):
        print '[{0}] handling ping from'.format(self.id), peer.id
        msg = self._construct_pong_msg()
        msg = msgpack.dumps(msg)
        peer.send(msg)

    def _handle_pong(self, peer, message):
        print '[{0}] handling pong from'.format(self.id), peer.id

    def _handle_stop(self, peer, message):
        self._stop()

    def _stop(self):
        self._loop.stop()

    def stop(self):
        if self._cnc_cmd_socket:
            if not self._cnc_cmd_socket.closed:
                cmd = self._construct_cmd('stop')
                self._cnc_cmd_socket.send(msgpack.dumps(cmd))
                self._container.join()
                self._cnc_cmd_socket.close(linger=1000)
                self._cnc_result_socket.close(linger=1000)
        else:
            self._stop()

    def join(self):
        try:
            self._container.join()
        except AttributeError:
            pass

    def __call__(self, cmd, *args, **kwargs):
        if self._cnc_cmd_socket:
            if not self._cnc_cmd_socket.closed:
                cmd = self._construct_cmd(cmd, *args, **kwargs)
                self._cnc_cmd_socket.send(msgpack.dumps(cmd))
                result = self._cnc_result_socket.recv()
                return result
        raise RuntimeError('Unable to process command. {0} is not running.'.format(self.name))

    def _add_peer(self, peer_id, peer_host, peer_port):
        peer = Peer(peer_id, peer_host, peer_port)
        peer.connect(self._context)
        self._peers[peer.id] = peer
        return peer

    def _remove_peer(self, peer_id):
        try:
            peer = self._peers.pop(peer_id)
        except KeyError:
            return
        peer.close()

    def _construct_beacon_msg(self):
        msg = {'type': 'beacon',
               'node': (self.id, get_host_ip(), str(self._direct_port)),
               'body': {}}
        return msg

    def _construct_ping_msg(self):
        msg = {'type': 'ping',
               'node': (self.id, get_host_ip(), str(self._direct_port)),
               'body': {'msg_types': self._direct_handlers.keys()}}
        return msg

    def _construct_pong_msg(self):
        msg = {'type': 'pong',
               'node': (self.id, get_host_ip(), str(self._direct_port)),
               'body': {'msg_types': self._direct_handlers.keys()}}
        return msg

    def _construct_stop_msg(self):
        msg = {'type': 'stop',
               'node': (self.id, get_host_ip(), str(self._direct_port)),
               'body': {}}
        return msg

    def _construct_cmd(self, cmd, *args, **kwargs):
        cmd = {'type': cmd,
               'node': (self._cnc_proxy_id, get_host_ip(), str(self._cnc_result_port)),
               'body': {'args': args, 'kwargs': kwargs}}
        return cmd