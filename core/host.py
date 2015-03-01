from flask import Flask
from flask import abort, request, json, Response
import msgpack

from .graph import Graph

_graphs = {}
host = Flask(__name__)


@host.route('/graphs/', methods=['GET'])
def get_graphs():
    result = {'graphs': [{'id': g.id, 'name': g.name} for g in _graphs]}
    return result


@host.route('/graphs/', methods=['POST'])
def create_graph(graph_name):
    graph = Graph(name=graph_name)
    _graphs[graph.id] = graph

    result = graph.to_json()
    result = msgpack.packb(result)
    return result


@host.route('/graphs/<graph_id>/', methods=['GET'])
def get_graph(graph_id):
    try:
        graph = _graphs[graph_id]
    except KeyError:
        abort(404)
        return

    result = graph.to_json()
    result = msgpack.packb(result)
    return result


@host.route('/graphs/<graph_id>/nodes/')
def get_nodes(graph_id, node_path):
    try:
        graph = _graphs[graph_id]
    except KeyError:
        abort(404)
        return

    node_ids = node_path.split('/')
    node = graph.get_node(*node_ids)
    if node is None:
        abort(404)
        return

    result = node.to_json()
    result = msgpack.packb(result)
    return result


@host.route('/graphs/<graph_id>/nodes/<node_id>', methods=['GET'])
def get_node(graph_id, node_id):
    try:
        graph = _graphs[graph_id]
    except KeyError:
        abort(404)
        return

    node = graph.get_node(node_id)
    if node is None:
        abort(404)
        return

    result = node.to_json()
    result = msgpack.packb(result)
    return result


@host.route('/graphs/<graph_id>/nodes/<node_id>', methods=['PUT'])
def put_node(graph_id, node_id):
    try:
        graph = _graphs[graph_id]
    except KeyError:
        abort(404)
        return

    if request.headers['Content-Type'] == 'application/json':
        request_data = json.dumps(request.json)
    elif request.headers['Content-Type'] == 'application/octet-stream':
        request_data = msgpack.unpackb(request.data)
    else:
        abort(415)
        return

    if not graph.put_node(node_id, **request_data):
        abort(400)
        return

    return Response(status=204)


if __name__ == '__main__':
    host.run()

