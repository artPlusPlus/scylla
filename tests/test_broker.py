import scylla


class TestBroker(object):
    def test_broker_init(self):
        b = scylla.Broker('test_broker')
        assert isinstance(b, scylla.Broker)

    def test_broker_run(self):
        b = scylla.Broker('test_broker')
        b.start()
        assert b.is_alive()
        b.stop()
        b.join()
        assert not b.is_alive()