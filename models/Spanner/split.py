import simpy
from models.Spanner.thread import Thread

class Split:
    def __init__(self, id, env, vars, network, zone):
        self.id = id
        self.vars = vars
        self.network = network
        self.zone = zone

        self.env = env
        self.cpu = simpy.PriorityResource(self.env, capacity=self.vars["CPU_CORES"])
        self.io = simpy.PriorityResource(self.env, capacity=self.vars["IO_DISKS"])

        shard_size = int(self.vars["DATABASE_SIZE"] / self.vars["NUM_SPLITS"])

        self.leader_zone = 1
        if self.leader_zone == self.zone.id:
            self.variable_locks = dict(
                (
                    "v" + elem,
                    [simpy.PreemptiveResource(self.env, capacity=1), True],
                )
                for elem in map(
                    str, range(shard_size * (self.id - 1), shard_size * self.id)
                )
            )

        self.data_storage = dict(
            ("v" + elem, (0, 0))
            for elem in map(
                str, range(shard_size * (self.id - 1), shard_size * self.id)
            )
        )  # Key -> [value, timestamp]
        self.waiting_reads = dict()  # Id -> [(waiting_timestamps) ,start_time]
        self.locks = dict()
        # msg_id -> [transaction, start_time]
        # msg_id -> [[locks], start_time, acks] write
        #   (msg_id, split) -> [[locks], start_time, acks1, acks2, transaction, true_time, sent ] coordenador dos splits
        #   (msg_id, split) -> [[locks], acks, transaction, src] lider do split
        #   (msg_id, split) -> [transaction] follower do split

        self.msg_id = 1
        self.tid = 1

        self.client_queue = list()
        self.server_queue = list()

        self.process_client = self.env.process(self.run_client_pool())
        self.process_server = self.env.process(self.run_server_pool())

        self.client_queue_passive = self.env.event()
        self.is_client_queue_passive = False
        self.server_queue_passive = self.env.event()
        self.is_server_queue_passive = False

        self.partition_server = self.env.event()
        self.partition_client = self.env.event()
        self.is_partition = False

    def wakeup_client_queue(self):
        if self.is_client_queue_passive:
            self.is_client_queue_passive = False
            self.client_queue_passive.succeed()
            self.client_queue_passive = self.env.event()

    def wakeup_server_queue(self):
        if self.is_server_queue_passive:
            self.is_server_queue_passive = False
            self.server_queue_passive.succeed()
            self.server_queue_passive = self.env.event()

    # Métodos para adicionar e remover da queue do clientes
    def add_client_queue(self, msg):
        self.client_queue.append(msg)
        self.wakeup_client_queue()

    def pop_client_queue(self):
        return self.client_queue.pop(0)

    def len_client_queue(self):
        return len(self.client_queue)

    # Métodos para adicionar e remover da queue dos servidores
    def add_server_queue(self, msg):
        self.server_queue.append(msg)
        self.wakeup_server_queue()

    def pop_server_queue(self):
        return self.server_queue.pop(0)

    def len_server_queue(self):
        return len(self.server_queue)

    # Métodos responsáveis pelos processos
    def run_client_pool(self):
        while True:
            # Se tiver partição espera
            if self.is_partition:
                yield self.partition_client

            while not self.len_client_queue():
                self.is_client_queue_passive = True
                yield self.client_queue_passive

            msg = self.pop_client_queue()

            t = Thread(self.tid, self.env, self.vars, "client", self)
            t.queue.append(msg)
            t.wakeup_queue()

            self.tid += 1

    def run_server_pool(self):
        while True:
            # Se tiver partição espera
            if self.is_partition:
                yield self.partition_server

            while not self.len_server_queue():
                self.is_server_queue_passive = True
                yield self.server_queue_passive

            msg = self.pop_server_queue()

            t = Thread(self.tid, self.env, self.vars, "server", self)
            t.queue.append(msg)
            t.wakeup_queue()

            self.tid += 1