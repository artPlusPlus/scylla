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

import msgpack

import scylla


def test_slot_init():
    s = scylla.slot.Slot('test_slot', type_hint=unicode)
    assert isinstance(s, scylla.slot.Slot)


def test_slot_json():
    s = scylla.slot.Slot('test_slot', type_hint=unicode)
    json = s.to_json()
    assert isinstance(json, dict)


def test_slot_pack():
    s = scylla.slot.Slot('test_slot', type_hint=unicode)
    json = s.to_json()
    msgpack.packb(json)