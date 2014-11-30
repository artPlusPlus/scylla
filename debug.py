import time
import zmq
import msgpack
from threading import Thread
from multiprocessing import Process

import scylla


def _task_handler(envelope):
    # print 'GOT TASK: {0}:{1}'.format(envelope.message_type, envelope.message_body)
    pass


def _test_pub(target_subscriber, *msg_types):
    with scylla.reply() as r:
        if r.request.message_body == 'start':
            r.reply('_test_pub', 'starting', 'sending tasks to {0}'.format(target_subscriber))

    for i in xrange(0, 100 * 1000):
        # print 'sending {0}'.format(i)
        for msg_type in msg_types:
            msg_body = str(i)
            scylla.publish(target_subscriber, '_test_pub', msg_type, msg_body, host=scylla.BROKER_ADDRESS)

    print scylla.request('done', 'debug', 'sent all').message_body


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
    a.start(process=True)

    time.sleep(3)

    b = scylla.Node('bravo')
    b.start(process=True)

    c = scylla.Node('charlie')
    c.start(thread=True)

    time.sleep(10)

    d = scylla.Node('delta')
    d.start(thread=True)

    a.stop()

    time.sleep(30)

    b.stop()
    c.stop()
    d.stop()


if __name__ == '__main__':
    _discovery_testing()
    scylla.terminate()
