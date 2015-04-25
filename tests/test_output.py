import uuid

import pytest

import scylla


class _Handlers(object):
    def __init__(self):
        self._value = 'Success'

    def compute_value(self, request):
        return self._value


@pytest.fixture
def _handler_fixture(request):
    return _Handlers()


def test_construction(_handler_fixture):
    output = scylla.output.Output('test_output',
                                  _handler_fixture.compute_value,
                                  type_hint=None)

    assert isinstance(output, scylla.output.Output)


def test_value(_handler_fixture):
    output = scylla.output.Output('test_output',
                                  _handler_fixture.compute_value,
                                  type_hint=None)

    assert output.value == 'Success'


def test_post(_handler_fixture):
    req_client = 'test_client'
    req_method = scylla.request.Methods.POST
    req_url = 'tcp://192.168.0.0:5000/slots/{0}'.format(uuid.uuid4())
    request = scylla.request.Request(req_client, req_method, req_url)

    output = scylla.output.Output('test_output',
                                  _handler_fixture.compute_value,
                                  type_hint=None)
    response = output(request)

    assert response
    assert response.status == scylla.Statuses.METHOD_NOT_ALLOWED


def test_get(_handler_fixture):
    req_client = 'test_client'
    req_method = scylla.request.Methods.GET
    req_url = 'tcp://192.168.0.0:5000/slots/{0}'.format(uuid.uuid4())
    request = scylla.request.Request(req_client, req_method, req_url)

    output = scylla.output.Output('test_output',
                                  _handler_fixture.compute_value,
                                  type_hint=None)
    response = output(request)

    assert response
    assert response.status == scylla.Statuses.OK
    assert response.data['value'] == 'Success'


def test_put(_handler_fixture):
    req_client = 'test_client'
    req_method = scylla.request.Methods.PUT
    req_url = 'tcp://192.168.0.0:5000/slots/{0}'.format(uuid.uuid4())
    request = scylla.request.Request(req_client, req_method, req_url)

    output = scylla.output.Output('test_output',
                                  _handler_fixture.compute_value,
                                  type_hint=None)
    response = output(request)

    assert response
    assert response.status == scylla.Statuses.METHOD_NOT_ALLOWED


def test_delete(_handler_fixture):
    req_client = 'test_client'
    req_method = scylla.request.Methods.DELETE
    req_url = 'tcp://192.168.0.0:5000/slots/{0}'.format(uuid.uuid4())
    request = scylla.request.Request(req_client, req_method, req_url)

    output = scylla.output.Output('test_output',
                                  _handler_fixture.compute_value,
                                  type_hint=None)
    response = output(request)

    assert response
    assert response.status == scylla.Statuses.METHOD_NOT_ALLOWED


def test_put_connection(_handler_fixture):
    input_slot_id = uuid.uuid4()
    input_url = 'tcp://192.168.0.0:5001/slots/{0}'.format(input_slot_id)

    req_client = 'test_client'
    req_method = scylla.request.Methods.PUT
    req_url = 'tcp://192.168.0.0:5000/slots/{0}/connections'.format(uuid.uuid4())
    req_data = {'url': input_url}
    request = scylla.request.Request(req_client, req_method, req_url,
                                     data=req_data)

    output = scylla.output.Output('test_output',
                                  _handler_fixture.compute_value,
                                  type_hint=None)
    response = output(request)

    assert response

    # Will fail due to inability to dirty the input
    assert response.status == scylla.Statuses.BAD_REQUEST