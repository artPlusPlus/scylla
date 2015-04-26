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

from .request import get, put, delete
from .response import Response, Statuses
from ._uri import URI


class ConnectionError(Exception):
    pass


def connect(output_slot_url, input_slot_url, force=False):
    """ Connects a source output slot to a target input slot.

    Args:
        output_slot_url (string): URL of an output slot whose value will drive
            the slot referenced at ``input_slot_url``
        input_slot_url (string): URL of an input slot whose value will be
            driven by the slot referenced at ``output_slot_url``
        force (bool, optional): Force the connection. If ``force`` is ``true``
            and the input slot is not a multi and has an existing connection,
            the existing connection will be severed.

    Returns:
        bool: True if successful, False otherwise

    Raises:
        ConnectionError: If any action on a slot fails, ConnectionError
            is raised.
    """
    input_data = get(input_slot_url)
    input_connections = input_data.data['connections']
    input_is_multi = input_data.data['multi']

    # Disconnect the input's existing connection if it's not a multi
    if input_connections and not input_is_multi:
        if not force:
            msg = 'Failed to connect {0} to {1}: {1} has incoming connection'.format(
                output_slot_url, input_slot_url)
            raise ConnectionError(msg)

        source_url = URI(input_connections[0])

        # Drop the target from the existing source
        drop_url = '{0}/connections/{1}'.format(source_url.path, input_data['id'])
        drop_response = delete(drop_url)
        if drop_response.status not in (Statuses.OK, Statuses.NOT_FOUND):
            msg = '[ERROR] Failed to connect {0} to {1}: {2} - {3}'.format(
                output_slot_url, input_slot_url, drop_response.status,
                drop_response.data)
            raise ConnectionError(msg)

        # Drop the existing source from the target
        drop_url = '{0}/connections/{1}'.format(input_slot_url, source_url.slot)
        drop_response = delete(drop_url)
        if drop_response.status not in (Statuses.OK, Statuses.NOT_FOUND):
            msg = '[ERROR] Failed to connect {0} to {1}: {2} - {3}'.format(
                output_slot_url, input_slot_url, drop_response.status,
                drop_response.data)
            raise ConnectionError(msg)

    # Connect the target
    input_response = put('{0}/connections'.format(input_slot_url),
                         data={'url': output_slot_url, 'force': force})
    if input_response.status != Statuses.OK:
        msg = '[ERROR] Failed to connect {0} to {1}: {2} - {3}'.format(
            output_slot_url, input_slot_url, input_response.status,
            input_response.data)
        raise ConnectionError(msg)

    # Connect the source
    source_response = put('{0}/connections'.format(output_slot_url),
                          data={'url': input_slot_url, 'force': force})
    if source_response.status != Statuses.OK:
        msg = '[ERROR] Failed to connect {0} to {1}: {2} - {3}'.format(
            output_slot_url, input_slot_url, source_response.status,
            source_response.data)
        raise ConnectionError(msg)

    # Done!
    return Response(None, Statuses.OK)