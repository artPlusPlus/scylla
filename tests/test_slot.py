import msgpack

import scylla


class TestSlot(object):
    def test_slot_init(self):
        s = scylla.slot.Slot('test_slot', type_hint=unicode)
        assert isinstance(s, scylla.slot.Slot)

    def test_slot_json(self):
        s = scylla.slot.Slot('test_slot', type_hint=unicode)
        json = s.to_json()
        assert isinstance(json, dict)

    def test_slot_pack(self):
        s = scylla.slot.Slot('test_slot', type_hint=unicode)
        json = s.to_json()
        msgpack.packb(json)