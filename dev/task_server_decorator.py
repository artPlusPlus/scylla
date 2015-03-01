class Job(object):
    @property
    def complete(self):
        return len(self.tasks) == len(self.data)

    def __init__(self, client_data, priority, ops=None):
        self.id = id
        self.client = client_data
        self.targets = []
        self.ops = []
        self.tasks = {}
        self.priority = priority
        self.status = None

        if ops:
            self.add_ops(ops)

    def add_target(self, input, output=None):
        self.targets.append((input, output))

    def add_ops(self, ops):
        if isinstance(ops, basestring):
            ops = [ops]
        try:
            self.ops.extend(ops)
        except TypeError:
            self.ops.append(ops)

    def __iter__(self):
        for target in self.targets:
            task = Task(id, target, self.ops[:])
            self.tasks[task.id] = task
            yield task

    def __call__(self, task_id, status):
        self.tasks[task_id].status = status

    @classmethod
    def from_json(cls, job_data):
        job = Job(job_data['id'], job_data['node'], )
        return job


class Task(object):
    def __init__(self, id, ops, input, output=None):
        self.id = id
        self.input = input
        self.output = output
        self.ops = tuple(ops)
        self.status = None

    @classmethod
    def from_json(cls, *msg):
        return Task(*msg)


server = Node('task_server')


@server.input('jobs', 'job', False, pending_jobs=dict, pending_tasks=dict)
def _handle_job(node, peer, message):
    job = Job.from_json(message)
    node.pending_jobs[job] = peer


@server.input('workers', 'ping', True, available_workers=dict)
def _handle_worker(node, peer, message):
    for msg_type in peer.inputs['tasks'].msg_types:
        try:
            worker_list = node.available_workers[msg_type]
        except KeyError:
            worker_list = []
            node.available_workers[msg_type] = worker_list
        worker_list.append(msg_type)

@server.input('completed_tasks', None, True)
def _handle_completed_task(node, peer, message):

    self._available_workers

@server.core
def _distribute_tasks(None):


maya_worker = Node('maya_worker')

@maya_worker.input('tasks', 'maya_task', True, pending_tasks=dict)
def _handle_new_task(node, peer, message):
    task = Task.from_json(message)
    node.pending_tasks[task] = peer


@maya_worker.core(None)
def _process_task(node):
    peer, task = node.pending_tasks.pop()
    cmds.file(task.input, open=True, force=True)
    for op in task.ops:
        op()
    if task.output:
        cmds.file(task.output, rename=True, force=True)
        cmds.file(task.output, save=True)
    task.status = 0
    node.completed_tasks[task] = peer

@maya_worker.output('tasks', 'maya_task', completed_tasks=dict)
def _output_completed_task(node):
    peer, task = node.completed_tasks.pop()
    peer.send(task.to_json())



