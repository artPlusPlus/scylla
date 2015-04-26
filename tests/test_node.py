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
import uuid

import pytest

import scylla


def _make_node(name, fork):
    node = scylla.Node(name)
    node.start(fork=fork)
    time.sleep(1)

    pongs = scylla.ping()
    node_url = [pong.url for pong in pongs if pong.id == node.id][0]
    return node, node_url


@pytest.fixture(params=[True, False])
def _node(request):
    node_name = 'node_fixture__{0}'.format(request.fixturename)
    if request.param:
        node_name = '{0}__forked'.format(node_name)
    else:
        node_name = '{0}__non_forked'.format(node_name)
    node, node_url = _make_node(node_name, request.param)

    def teardown():
        delete_response = scylla.delete(node_url)
        assert delete_response.status == scylla.Statuses.OK
        pongs = scylla.ping()
        pongs = [p for p in pongs if p.id == node.id]
        assert len(pongs) == 0
    request.addfinalizer(teardown)

    return node, node_url


@pytest.fixture(scope='module')
def _cleanup(request):
    def teardown():
        orphans = []
        pongs = scylla.ping()
        for pong in pongs:
            resp = scylla.delete(pong.url)
            if resp.status != scylla.Statuses.OK:
                orphans.append((pong.id, pong.name, pong.url))
        if orphans:
            msg = '\n\t'.join(['{0} {1:<20} {2}'.format(*o) for o in orphans])
            raise RuntimeWarning('Failed to cleanup nodes:\n\t{0}'.format(msg))
    request.addfinalizer(teardown)


def test_nonforked_construction(_cleanup):
    node = scylla.Node('test_node_construction_nonforked')
    node.start(fork=False)
    time.sleep(1)

    pongs = scylla.ping()
    pongs = [p for p in pongs if p.id == node.id]
    assert len(pongs) == 1
    assert pongs[0].name == node.name


def test_forked_construction(_cleanup):
    node = scylla.Node('test_node_construction_forked')
    node.start(fork=True)
    time.sleep(1)

    pongs = scylla.ping()
    pongs = [p for p in pongs if p.id == node.id]
    assert len(pongs) == 1
    assert pongs[0].name == node.name


def test_delete_nonforked():
    node = scylla.Node('test_node_delete_nonforked')
    node.start(fork=False)
    time.sleep(1)

    pongs = scylla.ping()
    pongs = [p for p in pongs if p.id == node.id]
    assert len(pongs) == 1
    assert pongs[0].name == node.name

    delete_response = scylla.delete(pongs[0].url)
    assert delete_response.status == scylla.Statuses.OK

    pongs = scylla.ping()
    pongs = [p for p in pongs if p.id == node.id]
    assert len(pongs) == 0


def test_delete_forked():
    node = scylla.Node('test_node_delete_forked')
    node.start(fork=True)
    time.sleep(1)

    pongs = scylla.ping()
    pongs = [p for p in pongs if p.id == node.id]
    assert len(pongs) == 1
    assert pongs[0].name == node.name

    delete_response = scylla.delete(pongs[0].url)
    assert delete_response.status == scylla.Statuses.OK

    pongs = scylla.ping()
    pongs = [p for p in pongs if p.id == node.id]
    assert len(pongs) == 0


def test_get(_node):
    node, node_url = _node

    response = scylla.get(node_url)
    assert response.status == scylla.Statuses.OK
    assert uuid.UUID(response.data['id']) == node.id