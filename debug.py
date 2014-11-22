import time
import zmq
import msgpack
import threading

import scylla


def _task_handler(envelope):
    print 'GOT TASK: {0}:{1}'.format(envelope.message_type, envelope.message_body)


def _test_pub(target_subscriber, *msg_types):
    with scylla.reply() as r:
        if r.request.message_body == 'start':
            r.reply('_test_pub', 'starting', 'sending tasks to {0}'.format(target_subscriber))

    for i in xrange(0, 100):
        # print 'sending {0}'.format(i)
        for msg_type in msg_types:
            msg_body = str(i)
            scylla.publish(target_subscriber, '_test_pub', msg_type, msg_body)

    print 'sent all'


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


def _main():
    print 'setting up broker'
    b = scylla.Broker('Broker')
    b.start()

    print 'setting up subscriber'
    s = scylla.Node('SUBSCRIBER')
    s.register_direct_message_handler('TASK', _task_handler)
    s.start()

    print 'setting up publisher'
    p = threading.Thread(target=_test_pub, args=[s.id, 'TASK'])
    p.start()

    print 'requesting tasks'
    print scylla.request('request', 'debug', 'start').message_body

    print 'waiting for work to complete'
    time.sleep(3)
    print 'work complete'

    print 'killing subscriber'
    scylla.publish(str(s.id), 'debug', 'stop', 'stop')
    s.join()

    print 'killing broker'
    scylla.publish(str(b.id), 'debug', 'stop', 'stop')
    b.join()

    print 'Cleaning up'
    scylla.terminate()

    print 'DONE'

if __name__ == '__main__':
    _main()
