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