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

from uuid import UUID, uuid4

import scylla


def test_inproc_host_no_trailing_slash():
    url = 'inproc://ef549bc4-a7d1-4d29-8cfd-e938bc4a9694'
    uri = scylla._uri.URI(url)
    assert uri.scheme == 'inproc'
    assert uri.host == 'ef549bc4-a7d1-4d29-8cfd-e938bc4a9694'


def test_host_no_trailing_slash():
    url = 'tcp://192.168.0.0:50000'
    uri = scylla._uri.URI(url)
    assert uri.scheme == 'tcp'
    assert uri.host == '192.168.0.0:50000'
    assert uri.path == ''


def test_host_with_trailing_slash():
    url = 'tcp://192.168.0.0:50000/'
    uri = scylla._uri.URI(url)
    assert uri.scheme == 'tcp'
    assert uri.host == '192.168.0.0:50000'
    assert uri.path == ''


def test_slots_no_trailing_slash():
    url = 'tcp://192.168.0.0:50000/slots'
    uri = scylla._uri.URI(url)
    assert uri.scheme == 'tcp'
    assert uri.host == '192.168.0.0:50000'
    assert uri.has_slots
    assert uri.path == 'slots'


def test_slots_with_trailing_slash():
    url = 'tcp://192.168.0.0:50000/slots/'
    uri = scylla._uri.URI(url)
    assert uri.scheme == 'tcp'
    assert uri.host == '192.168.0.0:50000'
    assert uri.has_slots
    assert uri.path == 'slots/'


def test_slot_no_trailing_slash():
    slot_id = str(uuid4())
    url = 'tcp://192.168.0.0:50000/slots/{0}'.format(slot_id)
    uri = scylla._uri.URI(url)
    assert uri.scheme == 'tcp'
    assert uri.host == '192.168.0.0:50000'
    assert uri.has_slots
    assert uri.slot == UUID(slot_id)
    assert uri.path == 'slots/{0}'.format(slot_id)


def test_connections_no_trailing_slash():
    slot_id = str(uuid4())
    url = 'tcp://192.168.0.0:50000/slots/{0}/connections'.format(slot_id)
    uri = scylla._uri.URI(url)
    assert uri.scheme == 'tcp'
    assert uri.host == '192.168.0.0:50000'
    assert uri.has_connections
    assert uri.path == 'slots/{0}/connections'.format(slot_id)


def test_connections_with_trailing_slash():
    slot_id = str(uuid4())
    url = 'tcp://192.168.0.0:50000/slots/{0}/connections/'.format(slot_id)
    uri = scylla._uri.URI(url)
    assert uri.scheme == 'tcp'
    assert uri.host == '192.168.0.0:50000'
    assert uri.has_connections
    assert uri.path == 'slots/{0}/connections/'.format(slot_id)


def test_connection_no_trailing_slash():
    slot_id = str(uuid4())
    connection_id = str(uuid4())
    url = 'tcp://192.168.0.0:50000/slots/{0}/connections/{1}'.format(slot_id, connection_id)
    uri = scylla._uri.URI(url)
    assert uri.scheme == 'tcp'
    assert uri.host == '192.168.0.0:50000'
    assert uri.has_connections
    assert uri.connection == UUID(connection_id)
    assert uri.path == 'slots/{0}/connections/{1}'.format(slot_id, connection_id)


def test_connection_with_trailing_slash():
    slot_id = str(uuid4())
    connection_id = str(uuid4())
    url = 'tcp://192.168.0.0:50000/slots/{0}/connections/{1}/'.format(slot_id, connection_id)
    uri = scylla._uri.URI(url)
    assert uri.scheme == 'tcp'
    assert uri.host == '192.168.0.0:50000'
    assert uri.has_connections
    assert uri.connection == UUID(connection_id)
    assert uri.path == 'slots/{0}/connections/{1}/'.format(slot_id, connection_id)