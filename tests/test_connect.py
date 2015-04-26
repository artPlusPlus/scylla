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

import time

import pytest

import scylla


def _make_node(name, fork):
    node = scylla.Node(name)
    node.start(fork=fork)
    time.sleep(1)

    pongs = scylla.ping()
    node_url = [pong.url for pong in pongs if pong.id == node.id][0]
    return node, node_url


@pytest.fixture(params=[True])
def source_node_fixture(request):
    node, node_url = _make_node('test_fixture_source', request.param)

    def teardown():
        scylla.delete(node_url)
    request.addfinalizer(teardown)

    return node, node_url


@pytest.fixture(params=[True])
def target_node_fixture(request):
    node, node_url = _make_node('text_fixture_target', request.param)

    def teardown():
        scylla.delete(node_url)
    request.addfinalizer(teardown)

    return node, node_url


def test_unconnected(source_node_fixture, target_node_fixture):
    source_node, source_url = source_node_fixture
    target_node, target_url = target_node_fixture

    resp = scylla.get(source_url)
    assert resp.status == scylla.Statuses.OK, 'Failed to GET source node data'
    source_data = resp.data
    source_name_id = [s['id'] for s in source_data['outputs'] if s['name'] == 'Name'][0]

    resp = scylla.get(target_url)
    assert resp.status == scylla.Statuses.OK, 'Failed to GET target node data'
    target_data = resp.data
    target_name_id = [s['id'] for s in target_data['inputs'] if s['name'] == 'Name'][0]

    resp = scylla.connect('{0}/slots/{1}'.format(source_url, source_name_id),
                          '{0}/slots/{1}'.format(target_url, target_name_id))
    assert resp.status == scylla.response.Statuses.OK

    resp = scylla.get(target_url)
    assert resp.status == scylla.Statuses.OK, 'Failed to GET target node data after connecting'
    assert resp.data['name'] == source_data['name']