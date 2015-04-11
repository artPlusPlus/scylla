import weakref
import uuid
import socket
from threading import Thread
from multiprocessing import Process

import zmq

from ._udp import UDPSocket
from .output import Output
from .input import Input
from .request import Methods
from .response import Response, Statuses
from .request import Request


class Node(object):
    def __init__(self, name, id=None, discovery_port=50000):
        super(Node, self).__init__()

        self._name = name
        self._id = id or uuid.uuid4()

        self._zmq_context = None

        self._slots = None
        self._outputs = None
        self._inputs = None
        self._running = False
        self._forked = False

        self._request_port = None
        self._discovery_port = discovery_port
        self._discovery_interface = None
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
            process = Process(target=self._start, kwargs=kwargs)
            process.daemon = True
            process.start()
        else:
            thread = Thread(target=self._start, kwargs=kwargs)
            thread.daemon = True
            thread.start()

    def _start(self, fork=False, **kwargs):
        self._zmq_context = zmq.Context()

        self._slots = weakref.WeakValueDictionary()
        self._outputs = weakref.WeakValueDictionary()
        self._inputs = weakref.WeakValueDictionary()

        self._init_state(fork=fork, **kwargs)
        self._init_interface(fork=fork, **kwargs)
        self._process_requests()

    def _init_state(self, fork=False, **kwargs):
        self._id_out = Output(u'Id', self._compute_id, type_hint=uuid.UUID)

        self._new_name = None
        self._name_out = Output(u'Id', self._compute_name, type_hint=unicode)
        self._name_in = Input(u'Name', self._put_name, self._dirty_name,
                              self._pull_name, type_hint=unicode)

    def _init_interface(self, fork=False, **kwargs):
        self._discovery_interface = UDPSocket(self._discovery_port)
        self._request_interface = self._zmq_context.socket(zmq.ROUTER)
        if fork:
            for addr in socket.gethostbyname_ex(socket.gethostname())[-1]:
                if not addr.startswith('127'):
                    self._host = addr
                    break
            self._request_port = self._request_interface.bind_to_random_port('tcp://*')
        else:
            self._host = 'inproc'
            self._request_port = self._id
            self._request_interface.bind('inproc://{0}'.format(self._id))

    def _process_requests(self):
        poller = zmq.Poller()
        poller.register(self._discovery_interface.socket, zmq.POLLIN)
        poller.register(self._request_interface, zmq.POLLIN)
        while True:
            try:
                events = dict(poller.poll(1000))
            except KeyboardInterrupt:
                print "interrupt"
                break

            if self._discovery_interface.fileno in events:
                msg = self._discovery_interface.recv()
                self._handle_ping(msg)
            if self._request_interface in events:
                origin, _, request = self._request_interface.recv_multipart()
                if not request:
                    continue
                request = Request.unpack(request)
                response = self._handle_request(request)
                response = response.pack()
                self._request_interface.send_multipart([origin, '', response])

    def _handle_ping(self, msg):
        print self._name, self._host, self._request_port

    def _handle_request(self, request):
        if request.url.slot:
            try:
                response = self._slots[request.slot](request)
            except KeyError:
                response = Response(request.client, Statuses.NOT_FOUND)
        elif request.method == Methods.GET:
            response = Response(request.client, Statuses.OK,
                                data=self.to_json())
        else:
            response = Response(request.client,
                                Statuses.METHOD_NOT_ALLOWED,
                                data=['GET'])
        return response

    def _compute_id(self, request):
        return self._id

    def _put_name(self, request):
        value = request.data['value']
        changed = self._new_name != value
        self._new_name = value
        return changed

    def _dirty_name(self, request):
        self._name_out.dirty()

    def _pull_name(self, request):
        return self._new_name

    def _compute_name(self, request):
        was_dirty, new_name = self._name_in.pull_value(request)
        if was_dirty:
            self._name = new_name
        return self._name

    def to_json(self):
        result = {'id': str(self._id),
                  'name': self._name,
                  'inputs': [i.to_json() for i in self._inputs.values()],
                  'outputs': [o.to_json() for o in self._outputs.values()]}
        return result

