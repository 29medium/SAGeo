import simpy
from models.Wren.thread import Thread

class Shard:
    def __init__(self, id, env, vars, network, datacenter):
        self.id = id
        self.vars = vars
        self.network = network
        self.datacenter = datacenter

        self.env = env
        self.cpu = simpy.Resource(self.env, capacity=self.vars["CPU_CORES"])
        self.io = simpy.Resource(self.env, capacity=self.vars["IO_DISKS"])

        shard_size = int(self.vars["DATABASE_SIZE"] / self.vars["NUM_SHARDS"])

        self.data_storage = dict(
            (("v" + elem), list())
            for elem in map(
                str, range(shard_size * (self.id - 1), shard_size * self.id)
            )
        )  # k -> [(v, ut, rdt, idt, sr)] value, local t, remote t, id transaction, source replica

        self.msg_id = 1

        self.client_threads = dict(
            (i, [Thread(i, self.env, self.vars, "client", self), True])
            for i in range(0, self.vars["CLIENT_POOL_SIZE"])
        )
        self.server_threads = dict(
            (i, [Thread(i, self.env, self.vars, "server", self), True])
            for i in range(0, self.vars["SERVER_POOL_SIZE"])
        )

        self.clock = 0
        self.lst = 0
        self.rst = 0

        self.waiting_slice = dict()  # msg_id -> [reads, acks, start_time, client]
        self.waiting_prepare = (
            dict()
        )  # msg_id -> [proposed_timestamps, acks, start_time, shards, client]
        self.prepared = dict()  # msg_id -> [pt, rt, transaction]
        self.committed = dict()  # msg_id -> [ct, rt, transaction]

        self.client_queue = list()
        self.server_queue = list()

        self.process_client = self.env.process(self.run_client_pool())
        self.process_server = self.env.process(self.run_server_pool())
        self.lst_update_process = self.env.process(self.lst_update())
        self.rst_update_process = self.env.process(self.rst_update())
        self.commit_process = self.env.process(self.commit())

        self.client_queue_passive = self.env.event()
        self.is_client_queue_passive = False
        self.client_thread_passive = self.env.event()
        self.is_client_thread_passive = False

        self.server_queue_passive = self.env.event()
        self.is_server_queue_passive = False
        self.server_thread_passive = self.env.event()
        self.is_server_thread_passive = False

        self.partition_server = self.env.event()
        self.partition_client = self.env.event()
        self.is_partition = False

    def wakeup_client_queue(self):
        if self.is_client_queue_passive:
            self.is_client_queue_passive = False
            self.client_queue_passive.succeed()
            self.client_queue_passive = self.env.event()

    def wakeup_client_thread(self):
        if self.is_client_thread_passive:
            self.is_client_thread_passive = False
            self.client_thread_passive.succeed()
            self.client_thread_passive = self.env.event()

    def wakeup_server_queue(self):
        if self.is_server_queue_passive:
            self.is_server_queue_passive = False
            self.server_queue_passive.succeed()
            self.server_queue_passive = self.env.event()

    def wakeup_server_thread(self):
        if self.is_server_thread_passive:
            self.is_server_thread_passive = False
            self.server_thread_passive.succeed()
            self.server_thread_passive = self.env.event()

    def generate_id(self):
        self.msg_id += 1
        return (self.msg_id, self.id)

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

    # Método responsável por selecionar uma thread
    def get_client_thread(self):
        for tid in self.client_threads:
            if self.client_threads[tid][1]:
                return tid

        return -1

    def get_server_thread(self):
        for tid in self.server_threads:
            if self.server_threads[tid][1]:
                return tid

        return -1

    # Método responsável por atribuir um trabalho a uma thread
    def start_client_thread(self, tid, msg):
        self.client_threads[tid][1] = False
        self.client_threads[tid][0].queue.append(msg)
        self.client_threads[tid][0].wakeup_queue()

    def start_server_thread(self, tid, msg):
        self.server_threads[tid][1] = False
        self.server_threads[tid][0].queue.append(msg)
        self.server_threads[tid][0].wakeup_queue()

    # Método responsável por marcar thread como idle
    def stop_thread(self, tid, type):
        if type == "client":
            self.client_threads[tid][1] = True
            self.wakeup_client_thread()
        elif type == "server":
            self.server_threads[tid][1] = True
            self.wakeup_server_thread()

    # Métodos responsáveis pelos processos
    def run_client_pool(self):
        while True:
            while not self.len_client_queue():
                self.is_client_queue_passive = True
                yield self.client_queue_passive

            msg = self.pop_client_queue()

            while True:
                # Se tiver partição espera
                if self.is_partition:
                    yield self.partition_client

                # Obtem uma thread
                if (t_id := self.get_client_thread()) == -1:
                    self.is_client_thread_passive = True
                    yield self.client_thread_passive
                else:
                    break

            # Atribui o trabalho à thread obtida
            self.start_client_thread(t_id, msg)

    def run_server_pool(self):
        while True:
            while not self.len_server_queue():
                self.is_server_queue_passive = True
                yield self.server_queue_passive

            msg = self.pop_server_queue()

            while True:
                # Se tiver partição espera
                if self.is_partition:
                    yield self.partition_server

                # Obtem uma thread
                if (t_id := self.get_server_thread()) == -1:
                    self.is_server_thread_passive = True
                    yield self.server_thread_passive
                else:
                    break

            # Atribui o trabalho à thread obtida
            self.start_server_thread(t_id, msg)

    def lst_update(self):
        while True:
            # Simula o tempo de gossip
            rtt = self.datacenter.generator.generate_timeout(
                self.vars["NETWORK_LATENCY_INTRA_DC"]
            )
            yield self.env.timeout(rtt)

            # Coleta todos os relogios
            clocks = list()
            for s in self.datacenter.shards.values():
                clocks.append(s.clock)

            # Guarda o minimo
            self.lst = min(clocks)

    def rst_update(self):
        while True:
            # Simula o tempo de gossip
            rtt = self.datacenter.generator.generate_timeout(
                self.vars["NETWORK_LATENCY_INTER_DC"]
            )
            yield self.env.timeout(rtt)

            # Coleta todos os relogios
            lsts = list()
            for dc in self.datacenter.model.datacenters.values():
                if dc.id != self.datacenter.id:
                    for s in dc.shards:
                        lsts.append(dc.shards[s].lst)

            # Guarda o minimo
            self.rst = min(lsts)

    def commit(self):
        while True:
            # Simula o intervalo entre commits
            ci = self.datacenter.generator.generate_timeout(
                self.vars["COMMIT_INTERVAL_SERVER"]
            )
            yield self.env.timeout(ci)

            msg = {"type": "apply_commit"}

            while True:
                # Obtem uma thread
                if (t_id := self.get_server_thread()) == -1:
                    self.is_server_thread_passive = True
                    yield self.server_thread_passive
                else:
                    break

            # Atribui o trabalho à thread obtida
            self.start_server_thread(t_id, msg)
