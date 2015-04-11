import time

import scylla


class TestNode(object):
    def test_node_init(self):
        n = scylla.Node('test_init')
        assert isinstance(n, scylla.Node)

    def test_node_non_fork_run(self):
        n = scylla.Node('test_non_forked')
        n.start()
        scylla.ping()