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

import weakref
import uuid
from threading import Thread
from multiprocessing import Process

import zmq

from . import _ZMQ_CONTEXT
from ._udp import UDPSocket
from .output import Output
from .input import Input
from .request import Methods
from .response import Response, Statuses
from .request import Request
from .ping import Ping, Pong
import _util


class Node(object):
    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def running(self):
        return self._running

    def __init__(self, name, id=None, discovery_port=50000):
        super(Node, self).__init__()

        self._name = name
        self._id = id or uuid.uuid4()

        self._container = None

        self._slots = None
        self._outputs = None
        self._inputs = None
        self._running = False
        self._forked = False

        self._ping_port = discovery_port
        self._request_port = None
        self._ping_interface = None
        self._pong_interface = None
        self._request_interface = None
        self._peer_interfaces = {}

        self._host = None

    def __setattr__(self, key, value):
        if isinstance(value, Output):
            self._slots[value.id] = value
            self._outputs[value.id] = value
        elif isinstance(value, Input):
            self._slots[value.id] = value
            self._inputs[value.id] = value
        super(Node, self).__setattr__(key, value)

    def start(self, fork=False, **kwargs):
        if self._running:
            return
        self._forked = fork
        self._running = True

        kwargs['fork'] = fork
        if fork:
            container = Process(target=self._start, kwargs=kwargs)
            container.daemon = True
            container.start()
        else:
            container = Thread(target=self._start, kwargs=kwargs)
            container.daemon = True
            container.start()

        self._container = container

    def stop(self):
        if self._forked:
            raise NotImplementedError('Stop method implemented for forked Nodes.')
        if self._running:
            self._running = False
            self._container.join()

    def _start(self, fork=False, **kwargs):
        self._slots = weakref.WeakValueDictionary()
        self._outputs = weakref.WeakValueDictionary()
        self._inputs = weakref.WeakValueDictionary()

        self._init_state(fork=fork, **kwargs)
        self._init_interface(fork=fork, **kwargs)

        self._process_messages()

        self._close_interface()

    def _init_state(self, fork=False, **kwargs):
        self._id_out = Output(u'Id', self._compute_id, type_hint=uuid.UUID)

        self._new_name = None
        self._name_out = Output(u'Name', self._compute_name, type_hint=unicode)
        self._name_in = Input(u'Name', self._put_name, self._dirty_name,
                              self._pull_name, type_hint=unicode)

    def _init_interface(self, fork=False, **kwargs):
        self._ping_interface = UDPSocket()
        self._ping_interface.bind(self._ping_port)

        self._pong_interface = _ZMQ_CONTEXT.socket(zmq.PUSH)

        self._request_interface = _ZMQ_CONTEXT.socket(zmq.ROUTER)
        if fork:
            self._host = _util.get_tcp_host()
            self._request_port = self._request_interface.bind_to_random_port('tcp://*')
        else:
            self._host = 'inproc'
            self._request_port = self._id
            self._request_interface.bind('inproc://{0}'.format(self._id))

    def _process_messages(self):
        poller = zmq.Poller()
        poller.register(self._ping_interface.socket, zmq.POLLIN)
        poller.register(self._request_interface, zmq.POLLIN)
        while self._running:
            try:
                events = dict(poller.poll(1000))
            except KeyboardInterrupt:
                print "interrupt"
                break

            if self._ping_interface.fileno in events:
                ping = self._ping_interface.recv()
                if not ping:
                    continue

                ping = Ping.unpack(ping)
                pong = self._handle_ping(ping)
                pong = pong.pack()

                self._pong_interface.connect(ping.pong_addr)
                self._pong_interface.send(pong)
                self._pong_interface.disconnect(ping.pong_addr)

            if self._request_interface in events:
                origin, _, request = self._request_interface.recv_multipart()
                if not request:
                    continue

                request = Request.unpack(request)
                response = self._handle_request(request)
                response = response.pack()

                self._request_interface.send_multipart([origin, '', response])

    def _handle_ping(self, _ping):
        if self._forked:
            url = 'tcp://{0}:{1}'.format(self._host, self._request_port)
        else:
            url = 'inproc://{0}'.format(self._id)
        pong = Pong(self._id, self._name, self.__class__.__name__, url)

        return pong

    def _handle_request(self, request):
        if request.url.slot:
            try:
                slot = self._slots[request.url.slot]
            except KeyError:
                response = Response(request.client, Statuses.NOT_FOUND)
            else:
                response = slot(request)
        elif request.method == Methods.GET:
            response = Response(request.client, Statuses.OK,
                                data=self.to_json())
        elif request.method == Methods.DELETE:
            response = Response(request.client, Statuses.OK)
            self._running = False
        else:
            response = Response(request.client,
                                Statuses.METHOD_NOT_ALLOWED,
                                data=['GET'])
        return response

    def _close_interface(self):
        if self._pong_interface:
            self._ping_interface.close()
        if self._pong_interface:
            self._pong_interface.close()
        if self._request_interface:
            self._request_interface.close()

    def _compute_id(self, request):
        return self._id

    def _put_name(self, request):
        value = unicode(request.data)
        changed = self._new_name != value
        self._new_name = value
        return changed

    def _dirty_name(self, request):
        self._name_out.dirty()

    def _pull_name(self, request):
        return self._new_name

    def _compute_name(self, request):
        if self._name_in.is_dirty:
            self._name = self._name_in.pull(request).values()[0]
        return self._name

    def to_json(self):
        result = {'id': str(self._id_out.value),
                  'name': self._name_out.value,
                  'type': self.__class__.__name__,
                  'inputs': [i.to_json() for i in self._inputs.values()],
                  'outputs': [o.to_json() for o in self._outputs.values()]}
        return result

    def __del__(self):
        self._close_interface()