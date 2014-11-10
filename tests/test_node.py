import time

import scylla


class TestNode(object):
    def test_node_init(self):
        n = scylla.Node('test_node')
        assert isinstance(n, scylla.Node)

    def test_node_run(self):
        n = scylla.Node('test_node')
        n.start()
        while not n.is_alive():
            time.sleep(0.1)
        assert n.is_alive()
        n.stop()
        n.join()
        assert not n.is_alive()

    def test_node_message_send(self):
        n = scylla.Node('test_receiver')
        n.add_message_handler('TEST', _test_message_handler)
        n.start()

        time.sleep(1)

        b = scylla.Broker('test_broker')
        b.start()

        time.sleep(1)

        scylla.send_message('TEST', 'PASS')

        time.sleep(1)

        print 'stopping {0}'.format(n.id)
        n.stop()
        n.join()

        print 'stopping {0}'.format(b.id)
        b.stop()
        b.join()


def _test_message_handler(content):
    print 'GOT CONTENT: {0}'.format(content)
    assert content == 'FAIL'