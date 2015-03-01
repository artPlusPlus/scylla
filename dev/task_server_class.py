import scylla


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
    def __init__(self, id, job_id, ops, input, output=None):
        self.id = id
        self.job_id = job_id
        self.input = input
        self.output = output
        self.ops = tuple(ops)
        self.status = None

    @classmethod
    def from_json(cls, *msg):
        return Task(*msg)


class TaskServer(scylla.Node):
    def __init__(self, name):
        super(TaskServer, self).__init__(name)

        self._pending_jobs = {}
        self._pending_tasks = {}

    def _setup(self, cnc_address_pipe, context):
        super(TaskServer, self)._setup(cnc_address_pipe, context)

        self.on_recv_direct('job', self._handle_job)
        self.on_recv_direct('task', self._handle_completed_task)

    def _handle_job(self, peer, message):
        job = Job.from_json(message)
        try:
            peer_jobs = self._pending_jobs[peer]
        except KeyError:
            peer_jobs = []
            self._pending_jobs[peer] = peer_jobs
        peer_jobs.append(job)

    def _handle_completed_task(self, peer, message):
        task = Task.from_json(message)
        self._pending_jobs[task.job_id]