import time
import zmq
import msgpack
from threading import Thread, Lock
from multiprocessing import Process

import scylla


_RECEIVE_START = None
_END_LOCK = Lock()
_RECEIVE_END = None
_MESSAGE_COUNT = 10000


def _get_receive_end():
    with _END_LOCK:
        return _RECEIVE_END


def _set_receive_end(value):
    with _END_LOCK:
        global _RECEIVE_END
        _RECEIVE_END = value


def _task_handler(peer, message):
    global _RECEIVE_START

    msg_id, msg_total = message
    if msg_id == 0:
        _RECEIVE_START = time.time()
        _set_receive_end(False)
    if not msg_id % (msg_total / 100):
        print msg_id
    if msg_id + 1 >= msg_total:
        print '[debug] received {0} messages:'.format(msg_total), time.time() - _RECEIVE_START
        _set_receive_end(True)
    print 'fuuck', msg_id


def _test_pub(node_handle, msg_type):
    node_handle.connect(zmq.Context.instance())
    start = time.time()
    for i in xrange(0, _MESSAGE_COUNT):
        # print 'sending {0}'.format(i)
        msg = {'type': msg_type,
               'node': ('debug', None, None),
               'body': (i, _MESSAGE_COUNT)}
        node_handle.send(msgpack.dumps(msg))
    node_handle.close()
    print '[debug] sent {0} messages:'.format(_MESSAGE_COUNT), time.time() - start


def _test_sub():
    ctx = zmq.Context.instance()

    sub = ctx.socket(zmq.SUB)
    sub.connect('tcp://localhost:{0}'.format(scylla.INBOUND_PORT))
    sub.setsockopt(zmq.SUBSCRIBE, 'SUB')
    sub.setsockopt(zmq.SUBSCRIBE, 'UPDATE')

    rep = ctx.socket(zmq.REQ)
    rep.connect('tcp://localhost:{0}'.format(scylla.DEBUG_REQ_PORT))
    rep.send('Hello')

    print rep.recv()

    while True:
        msg_type, msg_content = sub.recv_multipart()
        msg_content = msgpack.unpackb(msg_content)
        print 'received {0}'.format(msg_content)
        if msg_content == 'Update - 99':
            break

    print 'receieved all'


def _test_broker():
    print '[BROKER] - Hello'
    ctx = zmq.Context.instance()

    sub = ctx.socket(zmq.SUB)
    sub.connect('tcp://localhost:{0}'.format(scylla.DEFAULT_SUB_PORT))
    sub.setsockopt(zmq.SUBSCRIBE, 'BROKER')

    xpub = ctx.socket(zmq.XPUB)
    xpub.bind('tcp://*:{0}'.format(scylla.DEFAULT_SUB_PORT))

    xsub = ctx.socket(zmq.XSUB)
    xsub.bind('tcp://*:{0}'.format(scylla.DEFAULT_PUB_PORT))

    poller = zmq.Poller()
    poller.register(xpub, zmq.POLLIN)
    poller.register(xsub, zmq.POLLIN)
    poller.register(sub, zmq.POLLIN)

    while True:
        events = dict(poller.poll())
        if xpub in events and events[xpub] == zmq.POLLIN:
            message = xpub.recv_multipart()
            try:
                msg_type, msg_source, msg_body = message
                msg_source = msgpack.unpackb(msg_source)
                msg_body = msgpack.unpackb(msg_body)
            except ValueError:
                msg_body = message
                msg_type = None
                msg_source = None
            print '[BROKER] - IN - {0}:{1}'.format(msg_type, msg_body)
            xsub.send_multipart(message)
        if xsub in events and events[xsub] == zmq.POLLIN:
            message = xsub.recv_multipart()
            try:
                msg_type, msg_source, msg_body = message
                msg_source = msgpack.unpackb(msg_source)
                msg_body = msgpack.unpackb(msg_body)
            except ValueError:
                msg_body = message
                msg_type = None
                msg_source = None
            print '[BROKER] - OUT - {0}:{1}'.format(msg_type, msg_body)
            xpub.send_multipart(message)
        if sub in events and events[sub] == zmq.POLLIN:
            msg_type, msg_source, msg_body = sub.recv_multipart()
            msg_source = msgpack.unpackb(msg_source)
            msg_body = msgpack.unpackb(msg_body)
            if msg_body == 'STOP':
                break
            print '[BROKER] - GOT - "{0}" FROM {1}'.format(msg_body, msg_source)

    sub.close(linger=0)
    xpub.close(linger=0)
    xsub.close(linger=0)

    print '[BROKER] - Goodbye'


def _remote_testing():
    # print 'setting up broker'
    # b = scylla.Broker('Broker')
    # b.start()

    print 'setting up subscriber'
    s = scylla.Node('SUBSCRIBER')
    s.on_recv_direct('TASK', _task_handler)
    s.start()

    print 'setting up publisher'
    p = Thread(target=_test_pub, args=[s.id, 'TASK'])
    p.start()

    print 'requesting tasks'
    print scylla.request('request', 'debug', 'start').message_body

    print 'waiting for work to complete'
    with scylla.reply() as r:
        if r.request.message_body == 'sent all':
            r.reply('util', 'done', 'work complete')
    p.join()

    print 'killing subscriber'
    scylla.publish(str(s.id), 'debug', 'stop', 'stop', host=scylla.BROKER_ADDRESS)
    s.join()

    # print 'killing broker'
    # scylla.publish(str(b.id), 'debug', 'stop', 'stop')
    # b.join()

    print 'Cleaning up'
    scylla.terminate()

    print 'DONE'


def _discovery_testing():
    a = scylla.Node('alpha')
    a.on_recv_direct('TASK', _task_handler)
    a.start(process=True)

    b = scylla.Node('bravo')
    b.update = _test_pub
    b.start(process=True)

    c = scylla.Node('charlie')
    c.start(thread=True)

    d = scylla.Node('delta')
    d.start(thread=True)

    time.sleep(2)

    handle = scylla.NodeHandle(a.id, a.host_ip, a.direct_port)
    msg_type = 'TASK'
    pub_thread = Thread(target=_test_pub, args=(handle, msg_type))
    pub_thread.start()
    pub_thread.join()

    while not _get_receive_end():
        print 'waiting'
        time.sleep()

    a.stop()
    b.stop()
    c.stop()
    d.stop()


if __name__ == '__main__':
    _discovery_testing()
    scylla.terminate()
