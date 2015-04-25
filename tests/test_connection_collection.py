import uuid

import pytest

import scylla


@pytest.fixture()
def _collection(request):
    return scylla.connection_collection.ConnectionCollection()


@pytest.fixture(scope='module')
def _input_connection(request):
    slot_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    url = 'tcp://192.168.0.0:50000/slots/{0}/connections/{1}'.format(slot_id, connection_id)

    return scylla.input_connection.InputConnection(url, lambda x: x)


@pytest.fixture(scope='module')
def _output_connection(request):
    slot_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    url = 'tcp://192.168.0.0:50000/slots/{0}/connections/{1}'.format(slot_id, connection_id)

    return scylla.output_connection.OutputConnection(url)


def test_construction():
    collection = scylla.connection_collection.ConnectionCollection()
    assert isinstance(collection, scylla.connection_collection.ConnectionCollection)


def test_append(_collection, _input_connection):
    _collection.append(_input_connection)


def test_len(_collection, _input_connection, _output_connection):
    _collection.append(_input_connection)
    _collection.append(_output_connection)

    assert len(_collection) == 2


def test_indexing(_collection, _input_connection, _output_connection):
    _collection.append(_input_connection)
    _collection.append(_output_connection)

    assert _collection[0] is _input_connection
    assert _collection[1] is _output_connection


def test_mapping(_collection, _input_connection, _output_connection):
    _collection.append(_input_connection)
    _collection.append(_output_connection)

    assert _collection[_input_connection.url.slot] is _input_connection
    assert _collection[_output_connection.url.slot] is _output_connection


def test_remove(_collection, _input_connection, _output_connection):
    _collection.append(_input_connection)
    _collection.append(_output_connection)

    assert len(_collection) == 2

    _collection.remove(_output_connection)

    assert len(_collection) == 1
    assert _collection[0] is _input_connection


def test_truthiness(_collection, _input_connection):
    assert not _collection

    _collection.append(_input_connection)

    assert _collection


def test_iteration(_collection, _input_connection):
    _collection.append(_input_connection)

    for item in _collection:
        assert item is _input_connection
