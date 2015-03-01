import argparse

import scylla


node = scylla.Node('maya_job_server', pending_jobs={}, tasks={})




class Channel(object):
    @property
    def _input(self):
        try:
            return self._input()
        except TypeError:
            return self._input

    def __init__(self):
        self.name = None
        self.value = None
        self._input_ref = None
        self._outputs = []


class Core(object):
    def __init__(self, name, callable):
        self._name = name
        self._callable = callable
        self._inputs = []
        self._outputs = {}

    def add_slot(self, name, input=False, output=False, default_value=None):
        if input:
            self._inputs.append((name, default_value))
        if output:
            self._outputs[name] = default_value

    def __call__(self):
        try:
            callable = self._callable()
        except TypeError:
            callable = self._callable
        if not callable:
            return
        self._outputs.update(self._callable(**dict(self._inputs)))


@node.input(zmq.DEALER, 'job', update_on_receive=True)
def jobs(node, client, msg_type, message):
    job = Job.from_json(message)
    node.pending_jobs[job.id] = job



@node.input(zmq.ROUTER, update_on_receive=True)
def _workers(node, message):


@node.core('tasks')
def _job_core(node, job):
    for i in xrange(job):
        msg = {'type': 'TASK',
               'node': ('debug', None, None),
               'body': (i, message_count)}
        yield msgpack.dumps(msg)
    print '[debug] sent {0} messages:'.format(_MESSAGE_COUNT), time.time() - start


@node.on_connect('tasks')
def task_connect(node, client):
    try:
        connections = node.outputs['tasks']
    except KeyError:
        connections = []
        node.outputs['tasks'] = connections
    connection = node.
    connections.append(node.context.socket(zmq.))

def _node_main(self):
    for channel, msg in self._core(**self._current_values):
        self._channels[channel].send(msg)0


def _main(name=None):
    name = name or 'Broker'

    print '[BROKER-{0}] starting'.format(name)
    b = scylla.Broker(name)
    b.start()

    print '[BROKER-{0}] online'.format(name)
    b.join()

    print '[BROKER-{0}] offline'.format(name)

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-n', '--name', help='The name for the Broker')

    args = arg_parser.parse_args()

    _main(args.name)