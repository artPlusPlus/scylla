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

import uuid

import pytest

import scylla


class _Handlers(object):
    def __init__(self):
        self.put = False
        self.dirtied = False
        self.pulled = False

    def put_handler(self, request):
        self.put = True
        return True

    def dirty_handler(self, request):
        self.dirtied = True

    def pull_handler(self, request):
        self.pulled = True


@pytest.fixture()
def _handler_fixture(request):
    return _Handlers()


def test_construction(_handler_fixture):
    input = scylla.input.Input(
        'test_input', _handler_fixture.put_handler,
        _handler_fixture.dirty_handler, _handler_fixture.pull_handler,
        type_hint=None, multi=False)

    assert isinstance(input, scylla.input.Input)


def test_get(_handler_fixture):
    req_client = 'test_client'
    req_method = scylla.request.Methods.GET
    req_url = 'tcp://192.168.0.0:5000/slots/{0}'.format(uuid.uuid4())
    request = scylla.request.Request(req_client, req_method, req_url)

    input = scylla.input.Input(
        'test_input', _handler_fixture.put_handler,
        _handler_fixture.dirty_handler, _handler_fixture.pull_handler,
        type_hint=None, multi=False)
    response = input(request)

    assert response
    assert response.status == scylla.Statuses.OK

    assert not _handler_fixture.put
    assert not _handler_fixture.dirtied
    assert not _handler_fixture.pulled


def test_put(_handler_fixture):
    req_client = 'test_client'
    req_method = scylla.request.Methods.PUT
    req_url = 'tcp://192.168.0.0:5000/slots/{0}'.format(uuid.uuid4())
    request = scylla.request.Request(req_client, req_method, req_url)

    input = scylla.input.Input(
        'test_input', _handler_fixture.put_handler,
        _handler_fixture.dirty_handler, _handler_fixture.pull_handler,
        type_hint=None, multi=False)
    response = input(request)

    assert response
    assert response.status == scylla.Statuses.OK

    assert _handler_fixture.put
    assert _handler_fixture.dirtied
    assert not _handler_fixture.pulled


def test_post(_handler_fixture):
    req_client = 'test_client'
    req_method = scylla.request.Methods.POST
    req_url = 'tcp://192.168.0.0:5000/slots/{0}'.format(uuid.uuid4())
    request = scylla.request.Request(req_client, req_method, req_url)

    input = scylla.input.Input(
        'test_input', _handler_fixture.put_handler,
        _handler_fixture.dirty_handler, _handler_fixture.pull_handler,
        type_hint=None, multi=False)
    response = input(request)

    assert response
    assert response.status == scylla.Statuses.METHOD_NOT_ALLOWED

    assert not _handler_fixture.put
    assert not _handler_fixture.dirtied
    assert not _handler_fixture.pulled


def test_delete(_handler_fixture):
    req_client = 'test_client'
    req_method = scylla.request.Methods.DELETE
    req_url = 'tcp://192.168.0.0:5000/slots/{0}'.format(uuid.uuid4())
    request = scylla.request.Request(req_client, req_method, req_url)

    input = scylla.input.Input(
        'test_input', _handler_fixture.put_handler,
        _handler_fixture.dirty_handler, _handler_fixture.pull_handler,
        type_hint=None, multi=False)
    response = input(request)

    assert response
    assert response.status == scylla.Statuses.METHOD_NOT_ALLOWED

    assert not _handler_fixture.put
    assert not _handler_fixture.dirtied
    assert not _handler_fixture.pulled


def test_put_connection(_handler_fixture):
    output_slot_id = uuid.uuid4()
    output_url = 'tcp://192.168.0.0:5001/slots/{0}'.format(output_slot_id)

    req_client = 'test_client'
    req_method = scylla.request.Methods.PUT
    req_url = 'tcp://192.168.0.0:5000/slots/{0}/connections'.format(uuid.uuid4())
    req_data = {'url': output_url}
    request = scylla.request.Request(req_client, req_method, req_url,
                                     data=req_data)

    input = scylla.input.Input(
        'test_input', _handler_fixture.put_handler,
        _handler_fixture.dirty_handler, _handler_fixture.pull_handler,
        type_hint=None, multi=False)
    response = input(request)

    connection_url = '{0}/{1}'.format(req_url, output_slot_id)

    assert response
    assert response.status == scylla.Statuses.OK
    assert response.data['url'] == connection_url

    assert not _handler_fixture.put
    assert _handler_fixture.dirtied
    assert not _handler_fixture.pulled