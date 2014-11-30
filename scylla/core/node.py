import time
import uuid
from threading import Thread
from multiprocessing import Process, Pipe

import msgpack
import zmq
from zmq.eventloop.zmqstream import ZMQStream
from zmq.eventloop.ioloop import ZMQIOLoop, PeriodicCallback

from . import DEFAULT_DISCOVERY_PORT, NODE_MSG_PORT_START
from ._udp import UDPSocket
from .peer import Peer
from .util import terminate, get_host_name, get_host_ip


class Node(object):
    @property
    def name(self):
        return self._name

    @property
    def id(self):
        return '{0}:{1}'.format(get_host_name().lower(), self.name.lower())

    def __init__(self, name, port=None,
                 beacon_port=DEFAULT_DISCOVERY_PORT, beacon_rate=1.0):
        super(Node, self).__init__()

        self._name = name
        self._loop = None
        self._peers = {}

        self._beacon_port = beacon_port
        self._beacon_socket = None
        self._beacon_rate = beacon_rate
        self._next_beacon = time.time()
        self._beacon_callbacks = []

        self._cnc_key = uuid.uuid1()
        self._cnc_type = None
        self._cnc_frontend = None
        self._cnc_backend = None
        self._cnc_backend_stream = None
        self._container = None
        self._cnc_handlers = {}

        self._user_port = port is not None
        self._direct_port = port or NODE_MSG_PORT_START
        self._direct_socket = None
        self._direct_stream = None
        self._direct_handlers = {}  # TODO: weakref.WeakValueDictionary()

    def start(self, thread=False, process=False):
        if thread:
            self._cnc_type = zmq.PAIR
            _p1, _p2 = Pipe()
            self._container = Thread(name=self.id, target=self._start, args=(_p2,))
            self._container.start()
            self._cnc_frontend = zmq.Context.instance().socket(self._cnc_type)
            cnc_address = 'inproc://{}'.format(self._cnc_key)
            self._cnc_frontend.bind(cnc_address)
            _p1.send(cnc_address)
        elif process:
            self._cnc_type = zmq.DEALER
            _p1, _p2 = Pipe()
            self._container = Process(name=self.id, target=self._start, args=(_p2,))
            self._container.start()
            self._cnc_frontend = zmq.Context.instance().socket(self._cnc_type)
            cnc_address = self._direct_port + 1
            while True:
                try:
                    self._cnc_frontend.bind('tcp://*:{}'.format(cnc_address))
                except zmq.ZMQError:
                    cnc_address += 1
                else:
                    break
            _p1.send('tcp://localhost:{}'.format(cnc_address))
        else:
            self._start(None)

    def _start(self, cnc_address_pipe):
        self._setup(cnc_address_pipe)
        self._main()
        self._teardown()

    def _setup(self, cnc_address_pipe):
        print '[{0}] setting up'.format(self.id)
        self._loop = ZMQIOLoop()
        context = zmq.Context.instance()

        # Command and Control
        if cnc_address_pipe:
            self._cnc_backend = context.socket(self._cnc_type)
            self._cnc_backend.connect(cnc_address_pipe.recv())
            self._cnc_backend_stream = ZMQStream(self._cnc_backend,
                                                 io_loop=self._loop)
            self._cnc_backend_stream.on_recv(self._receive_cnc_message)
            self.on_recv_cmd('stop', self._stop)

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
        self._direct_socket = context.socket(zmq.ROUTER)
        while True:
            try:
                self._direct_socket.bind('tcp://*:{}'.format(self._direct_port))
            except zmq.ZMQError:
                if self._user_port:
                    raise
                else:
                    self._direct_port += 1
            else:
                break
        self._direct_stream = ZMQStream(self._direct_socket, io_loop=self._loop)
        self._direct_stream.on_recv(self._receive_direct_message)
        self.on_recv_direct('stop', self._handle_stop)
        self.on_recv_direct('ping', self._handle_ping)
        self.on_recv_direct('pong', self._handle_pong)

    def _main(self):
        print '[{0}] running'.format(self.id)
        for bcb in self._beacon_callbacks:
            bcb.start()
        self._loop.start()

    def _teardown(self):
        print '[{0}] stopping'.format(self.id)
        for peer_id in self._peers.keys():
            self._remove_peer(peer_id)
        for s in [self._direct_socket, self._cnc_frontend, self._cnc_backend]:
            try:
                s.close(linger=1000)
            except AttributeError:
                pass
        if self._cnc_type == zmq.DEALER:
            terminate()
        print '[{0}] offline'.format(self.id)

    def on_recv_cmd(self, cmd, handler):
        if cmd in self._cnc_handlers:
            msg = '{0} already handled by {1}'.format(
                cmd, self._cnc_handlers[cmd])
            raise ValueError(msg)
        self._cnc_handlers[cmd] = handler

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
            if peer.expired:
                self._remove_peer(peer.id)

    def _process_message(self, raw_message):
        message = msgpack.loads(raw_message)
        host_id, host_address, host_port = message['host']
        if host_id == self.id:
            return self, None, None

        try:
            peer = self._peers[host_id]
            peer.touch()
        except KeyError:
            peer = self._add_peer(host_id, host_address, host_port)

        return peer, message['type'], message['body']

    def _receive_cnc_message(self, message):
        cmd, args, kwargs = msgpack.loads(message[0])
        try:
            handler = self._cnc_handlers[cmd]
        except KeyError:
            print '[{0}] DROPPED COMMAND - NO HANDLER: {1}'.format(self.id, cmd)
            return
        result = handler(*args, **kwargs)
        if self._cnc_backend:
            if not self._cnc_backend.closed:
                self._cnc_backend.send(msgpack.dumps(result))
        else:
            return result

    def _receive_beacon_message(self, fd, event):
        message = self._beacon_socket.receive()
        peers = self._peers.keys()
        peer, msg_type, message = self._process_message(message)

        if peer is not self and peer.id not in peers:
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
        if self._cnc_frontend:
            if not self._cnc_frontend.closed:
                cmd = ('stop', [], {})
                self._cnc_frontend.send(msgpack.dumps(cmd))
                self._container.join()
                self._cnc_frontend.close(linger=1000)
        else:
            self._stop()

    def join(self):
        try:
            self._container.join()
        except AttributeError:
            pass

    def _add_peer(self, peer_id, peer_host, peer_socket):
        peer = Peer(peer_id, peer_host, peer_socket)
        self._peers[peer.id] = peer
        return peer

    def _remove_peer(self, peer_id):
        try:
            peer = self._peers.pop(peer_id)
        except KeyError:
            return
        peer.stop()

    def _construct_beacon_msg(self):
        msg = {'type': 'beacon',
               'host': (self.id, get_host_ip(), str(self._direct_port)),
               'body': {}}
        return msg

    def _construct_ping_msg(self):
        msg = {'type': 'ping',
               'host': (self.id, get_host_ip(), str(self._direct_port)),
               'body': {'msg_types': self._direct_handlers.keys()}}
        return msg

    def _construct_pong_msg(self):
        msg = {'type': 'pong',
               'host': (self.id, get_host_ip(), str(self._direct_port)),
               'body': {'msg_types': self._direct_handlers.keys()}}
        return msg