import simpy
from models.DBSM.executing_thread import ExecutingThread
from models.DBSM.workload_generator import WorkloadGenerator
from models.DBSM.terminating_thread import TerminatingThread


class Server:
    def __init__(self, id, env, vars, network, model):
        self.id = id
        self.vars = vars
        self.network = network
        self.model = model

        self.env = env
        self.cpu = simpy.Resource(self.env, capacity=self.vars["CPU_CORES"])
        self.io = simpy.Resource(self.env, capacity=self.vars["IO_DISKS"])
        self.log = simpy.Resource(self.env, capacity=self.vars["LOG_DISKS"])

        self.variable_locks = dict(
            ("v" + elem, simpy.Resource(self.env, capacity=1))
            for elem in map(str, range(0, self.vars["DATABASE_SIZE"]))
        )

        self.generator = WorkloadGenerator(self.env, self.vars, self)
        self.executing_threads = dict(
            (i, [ExecutingThread(self.env, self.vars, self, i), True])
            for i in range(0, self.vars["POOL_SIZE"])
        )
        self.terminating_thread = TerminatingThread(self.env, self.vars, self)

        self.executing_queue_normal = list()
        self.executing_queue_priority = list()
        self.terminating_queue = list()
        self.commit_log = list()

        self.process = self.env.process(self.run())
        self.queue_passive = self.env.event()
        self.is_queue_passive = False
        self.thread_passive = self.env.event()
        self.is_thread_passive = False

    # Método responsável por selecionar uma thread
    def get_executing_thread(self):
        thread = False
        for tid in self.executing_threads:
            if self.executing_threads[tid][1]:
                thread = True
                t_id = tid

        if thread:
            return t_id
        else:
            return -1

    # Método responsável por atribuir um trabalho a uma thread
    def start_executing_thread(self, tid, msg):
        self.executing_threads[tid][1] = False
        self.executing_threads[tid][0].add_queue(msg)

    # Método responsável por marcar thread como idle
    def stop_executing_thread(self, tid):
        self.executing_threads[tid][1] = True
        self.wakeup_thread()

    # Métodos da executing queue
    def len_executing_queue(self):
        return len(self.executing_queue_normal) + len(self.executing_queue_priority)

    def pop_executing_queue(self):
        if len(self.executing_queue_priority):
            return self.executing_queue_priority.pop(0)
        else:
            return self.executing_queue_normal.pop(0)

    def add_executing_queue(self, msg):
        if msg[0] == "executing":
            self.executing_queue_normal.append(msg)
        else:
            self.executing_queue_priority.append(msg)

        self.wakeup_queue()

    # Métodos da terminating queue
    def len_terminating_queue(self):
        return len(self.terminating_queue)

    def pop_terminating_queue(self):
        return self.terminating_queue.pop(0)

    def add_terminating_queue(self, msg):
        self.terminating_queue.append(msg)
        self.terminating_thread.wakeup()

    # Métodos do commit log
    def len_commit_log(self):
        return len(self.commit_log)

    def add_commit_log(self, write_set):
        self.commit_log.append(write_set)

    def get_commit_log(self, index):
        return self.commit_log[index]

    # Métodos responsáveis por acordar o processo
    def wakeup_queue(self):
        if self.is_queue_passive:
            self.is_queue_passive = False
            self.queue_passive.succeed()
            self.queue_passive = self.env.event()

    def wakeup_thread(self):
        if self.is_thread_passive:
            self.is_thread_passive = False
            self.thread_passive.succeed()
            self.thread_passive = self.env.event()

    # Método run do processo
    def run(self):
        while True:
            self.is_queue_passive = True
            yield self.queue_passive

            while self.len_executing_queue():
                msg = self.pop_executing_queue()

                while True:
                    # Obtem uma thread
                    if (t_id := self.get_executing_thread()) == -1:
                        self.is_thread_passive = True
                        yield self.thread_passive
                    else:
                        break

                # Atribui o trabalho à thread obtida
                self.start_executing_thread(t_id, msg)
