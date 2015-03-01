import uuid
import msgpack


class Graph(object):
    def __init__(self):
        super(Graph, self).__init__()

        self._nodes = {}

    def create_node(self, name, type, **node_data):
        print 'creating {0} node named {1}'.format(type, name)

        node_id = uuid.uuid4()
        self._nodes[node_id] = {'id': node_id, 'name': name, 'type': type, 'data': node_data}
        return msgpack.packb(node_id)

    def restore_node(self, node_data):
        id = node_data['id']
        name = node_data['name']
        type = node_data['type']
        data = node_data['data']

        self._nodes[id] = {'id': id, 'name': name, 'type': type, 'data': data}
        return msgpack.packb(node_data)

    def delete_node(self, node_id):
        print 'deleting node: {0}'
        try:
            del self._nodes[node_id]
        except KeyError:
            return msgpack.packb(False)
        return msgpack.packb(True)

    # def _process_client_requests(self, address):
    #     clients = zmq.Context.instance().socket(zmq.ROUTER)
    #     clients.bind(address)
    #
    #     handlers = zmq.Context.instance().socket(zmq.DEALER)
    #     handlers.bind('inproc://graph_handlers')
    #
    #     handler_pool = []
    #     while len(handler_pool) < 4:
    #         handler = Thread(target=self._handle_client_request)
    #         handler.start()
    #         handler_pool.append(handler)
    #
    #     poller = zmq.Poller()
    #     poller.register(clients, zmq.POLLIN)
    #     poller.register(handlers, zmq.POLLIN)
    #
    #     while True:
    #         sockets = dict(poller.poll(1000))
    #         if clients in sockets:
    #             client, msg = clients.recv_multipart()
    #             handlers.send_multipart([client, msg])
    #         if handlers in sockets:
    #             client, response = handlers.recv_multipart()
    #             clients.send_multipart([client, response])

    # def _handle_client_request(self):
    #     handler = zmq.Context.instance().socket(zmq.DEALER)
    #     handler.connect('inproc://graph_handlers')
    #
    #     while True:
    #         client, request = handler.recv_multipart()
    #         request = msgpack.unpackb(request)
    #         method = request['method']
    #         uri = request['uri']
    #         body = request['bod']
    #
    #         print 'Got "{0}" request from {1}'.format(request, client)
    #         result = msgpack.packb(False)
    #         if msg == 'dir':
    #             result = self._dir()
    #         elif msg == 'create_node':
    #             name = msg.get('name')
    #             result = self._create_node(name=name)
    #         elif msg == 'delete_node':
    #             id = msg.get('id')
    #             result = self._delete_node(id)
    #
    #         handler.send_multipart([client, result])


# class Client(object):
#     @property
#     def connected(self):
#         return self._graph is not None
#
#     def __init__(self):
#         super(Client, self).__init__()
#
#         self._graph = None
#
#     def connect(self, address):
#         self._graph = zmq.Context.instance().socket(zmq.DEALER)
#         self._graph.connect(address)
#
#     def disconnect(self):
#         if self._graph:
#             self._graph.close()
#         self._graph = None
#
#     def dir(self):
#         msg = 'dir'
#         msg = msgpack.packb(msg)
#         return self._issue_request(msg)
#
#     def create_node(self):
#         msg = 'create_node'
#         msg = msgpack.packb(msg)
#         return self._issue_request(msg)
#
#     def delete_node(self):
#         msg = 'delete_node'
#         msg = msgpack.packb(msg)
#         return self._issue_request(msg)
#
#     def _issue_request(self, message):
#         if not self.connected:
#             raise IOError('No Graph Connection!')
#         self._graph.send(message)
#
#         # response = self._graph.recv()
#         response = msgpack.packb(None)
#         response = msgpack.unpackb(response)
#         return response
#
#
# def _main():
#     address = 'inproc://graph'
#
#     graph = Graph(address)
#     # graph = Thread(target=_server, args=[address])
#     # graph.start()
#
#     # client = Thread(target=_client, args=[address])
#     # client.start()
#     client = Client()
#     client.connect(address)
#     print client.dir()
#     print client.create_node()
#     print client.delete_node()
#
#
# def _server(address):
#     s = zmq.Context.instance().socket(zmq.ROUTER)
#     s.bind(address)
#
#     poller = zmq.Poller()
#     poller.register(s, zmq.POLLIN)
#
#     while True:
#         sockets = dict(poller.poll(1000))
#         if s in sockets:
#             client, request = s.recv_multipart()
#             request = _unpack_request(request)
#             print 'Server Received {0} from {1}'.format(request, client)
#
#             response = _pack_response(True, {})
#             s.send_multipart([client, response])
#
#
# def _client(address):
#     s = zmq.Context.instance().socket(zmq.DEALER)
#     s.connect(address)
#
#     count = 0
#     while count < 100:
#         request = _pack_request('post', '/nodes', {'name': count})
#         s.send(request)
#
#         response = s.recv()
#         response = _unpack_response(response)
#         print 'Client Received {0}'.format(response)
#
#         count += 1
#
#
# class Request(object):
#     @property
#     def method(self):
#         return self._method
#
#     @property
#     def uri(self):
#         return self._uri
#
#     @property
#     def body(self):
#         return self._body
#
#     def __init__(self, method, uri, body):
#         self._method = method
#         self._uri = uri
#         self._body = body
#
#     def pack(self):
#
#     @staticmethod
#     def pack(method, uri, body=None):
#         if not body:
#             body = {}
#
#
#
# def _pack_request(method, uri, body):
#     request = {'method': method,
#                'uri': uri,
#                'body': body}
#     return msgpack.packb(request)
#
#
# def _unpack_request(request):
#     return msgpack.unpackb(request)
#
#
# def _pack_response(result, body):
#     response = {'result': result,
#                 'body': body}
#     return msgpack.packb(response)
#
#
# def _unpack_response(response):
#     return msgpack.unpackb(response)
#
# if __name__ == '__main__':
#     _main()