from models.Wren.network import Network

class Thread:
    def __init__(self, tid, env, vars, type, shard):
        self.tid = tid
        self.vars = vars
        self.type = type
        self.shard = shard

        self.queue = list()

        self.env = env
        self.process = self.env.process(self.run())

        self.queue_passive = self.env.event()
        self.is_queue_passive = False

    # Métodos para acordar as filas de espera

    def wakeup_queue(self):
        if self.is_queue_passive:
            self.is_queue_passive = False
            self.queue_passive.succeed()
            self.queue_passive = self.env.event()

    # Métodos para utilizar os recursos CPU e IO

    def use_cpu_read(self):
        cpu_req = self.shard.cpu.request()
        yield cpu_req

        cpu_time = self.shard.datacenter.generator.generate_timeout(
            self.vars["CPU_READ"]
        )
        yield self.env.timeout(cpu_time)

        self.shard.cpu.release(cpu_req)

    def use_cpu_execute(self):
        cpu_req = self.shard.cpu.request()
        yield cpu_req

        cpu_time = self.shard.datacenter.generator.generate_timeout(
            self.vars["CPU_EXECUTE"]
        )
        yield self.env.timeout(cpu_time)

        self.shard.cpu.release(cpu_req)

    def use_io(self):
        io_req = self.shard.io.request()
        yield io_req

        io_time = self.shard.datacenter.generator.generate_timeout(
            self.vars["IO_WRITE"]
        )
        yield self.env.timeout(io_time)

        self.shard.io.release(io_req)

    def read_transaction(self, transaction):
        for _ in transaction:
            yield from self.use_cpu_read()

    def execute_transaction(self, transaction):
        for _ in transaction:
            yield from self.use_cpu_execute()

    # Método para adicionar estatísticas

    def add_stats(self, start_time, ttype):
        self.shard.datacenter.model.add_statistics(
            self.env.now
            - start_time
            + self.shard.datacenter.generator.generate_timeout(
                self.vars["NETWORK_LATENCY_CLIENT"]
            )
            + self.shard.datacenter.generator.generate_timeout(
                self.vars["NETWORK_LATENCY_CLIENT"]
            ),
            ttype,
        )

    # Métodos para enviar mensagens

    def send(self, datacenter_id, shard_id, msg, latency=0):
        shard = self.shard.datacenter.model.datacenters[datacenter_id].shards[shard_id]
        if latency == 0:
            if datacenter_id == self.shard.datacenter.id:
                latency = self.shard.datacenter.generator.generate_timeout(
                    self.vars["NETWORK_LATENCY_INTRA_DC"]
                )
            else:
                latency = self.shard.datacenter.generator.generate_timeout(
                    self.vars["NETWORK_LATENCY_INTER_DC"]
                )
        Network(
            self.env,
            self.vars,
            msg,
            shard,
            latency,
            self.shard.network,
        )

    def send_slice_intra(self, msg_id, transactions, lt, rt):
        latency = self.shard.datacenter.generator.generate_timeout(
            self.vars["NETWORK_LATENCY_INTRA_DC"]
        )
        for shard, transaction in transactions.items():
            self.send(
                self.shard.datacenter.id,
                shard,
                {
                    "type": "slice",
                    "src": (self.shard.datacenter.id, self.shard.id),
                    "id": msg_id,
                    "transaction": transaction,
                    "lt": lt,
                    "rt": rt,
                },
                latency,
            )

    def send_prepare_intra(self, msg_id, transactions, lt, rt, ht):
        latency = self.shard.datacenter.generator.generate_timeout(
            self.vars["NETWORK_LATENCY_INTRA_DC"]
        )
        for shard, transaction in transactions.items():
            self.send(
                self.shard.datacenter.id,
                shard,
                {
                    "type": "prepare",
                    "src": (self.shard.datacenter.id, self.shard.id),
                    "id": msg_id,
                    "transaction": transaction,
                    "lt": lt,
                    "rt": rt,
                    "ht": ht,
                },
                latency,
            )

    def send_commit_intra(self, msg_id, shards, ct):
        latency = self.shard.datacenter.generator.generate_timeout(
            self.vars["NETWORK_LATENCY_INTRA_DC"]
        )
        for shard in shards:
            self.send(
                self.shard.datacenter.id,
                shard,
                {
                    "type": "commit",
                    "src": (self.shard.datacenter.id, self.shard.id),
                    "id": msg_id,
                    "ct": ct,
                },
                latency,
            )

    # Métodos para aplicar as escritas e as leituras

    def apply_read(self, transaction, lt, rt):
        D = set()
        for key in transaction:
            Dkv = set()
            for item in self.shard.data_storage[key]:
                if (
                    item[4] == self.shard.datacenter.id
                    and item[1] <= lt
                    and item[2] <= rt
                ) or (
                    item[4] != self.shard.datacenter.id
                    and item[1] <= rt
                    and item[2] <= lt
                ):
                    Dkv.add(tuple(item))

            max = 0
            freshest = None
            for item in Dkv:
                if item[0] > max:
                    max = item[0]
                    freshest = item
            D.add(tuple([key, freshest]))

        return D

    # Método do processo

    def run(self):
        while True:
            while len(self.queue):
                msg = self.queue.pop(0)

                # type, start_time, transaction, client, lstc, rstc,
                if msg["type"] == "read":
                    # Fase Start
                    client = self.shard.datacenter.clients[msg["client"]]

                    self.shard.rst = max(self.shard.rst, msg["rstc"])
                    self.shard.lst = max(self.shard.rst, msg["lstc"])

                    lt = self.shard.lst
                    rt = min(self.shard.rst, self.shard.lst - 1)

                    client.start(rt, lt)

                    # Cria estrutura para guardar respostas
                    msg_id = self.shard.generate_id()
                    self.shard.waiting_slice[msg_id] = [
                        set(),
                        len(msg["transaction"]),
                        msg["start_time"],
                        msg["client"],
                    ]

                    # Enviar mensagem a outros shards
                    self.send_slice_intra(msg_id, msg["transaction"], lt, rt)

                # type, id, transaction, lt, rt
                elif msg["type"] == "slice":
                    # Atualiza o relogio
                    self.shard.lst = max(self.shard.lst, msg["lt"])
                    self.shard.rst = max(self.shard.rst, msg["rt"])

                    # Simular tempo de leitura
                    yield from self.read_transaction(msg["transaction"])

                    # Efetuar a leitura
                    read = self.apply_read(msg["transaction"], msg["lt"], msg["rt"])

                    # Responder ao coordenador
                    self.send(
                        msg["src"][0],
                        msg["src"][1],
                        {"type": "slice_resp", "id": msg["id"], "read": read},
                    )
                # type, id, read
                elif msg["type"] == "slice_resp":
                    # Acrescentar a estrutura
                    self.shard.waiting_slice[msg["id"]][0].update(msg["read"])
                    self.shard.waiting_slice[msg["id"]][1] -= 1

                    # Se receber todos finaliza
                    if self.shard.waiting_slice[msg["id"]][1] == 0:
                        self.add_stats(self.shard.waiting_slice[msg["id"]][2], "read")

                        # # Adiciona ao read-set do cliente
                        # client = self.shard.datacenter.clients[
                        #     self.shard.waiting_slice[msg["id"]][3]
                        # ]
                        # client.read_response(self.shard.waiting_slice[msg["id"]][0])

                # type, id, transaction, client, rstc, lstc, hwtc
                elif msg["type"] == "write":
                    # Fase start
                    client = self.shard.datacenter.clients[msg["client"]]

                    self.shard.rst = max(self.shard.rst, msg["rstc"])
                    self.shard.lst = max(self.shard.rst, msg["lstc"])

                    lt = self.shard.lst
                    rt = min(self.shard.rst, self.shard.lst - 1)

                    client.start(rt, lt)

                    ht = min(lt, rt, msg["hwtc"])

                    # Cria estrutura para guardar respostas
                    msg_id = self.shard.generate_id()
                    self.shard.waiting_prepare[msg_id] = [
                        list(),
                        len(msg["transaction"]),
                        msg["start_time"],
                        list(msg["transaction"].keys()),
                        msg["client"],
                        sum(msg["transaction"].values(), []),
                    ]

                    # Enviar mensagem a outros shards
                    self.send_prepare_intra(msg_id, msg["transaction"], lt, rt, ht)

                elif msg["type"] == "prepare":
                    # Atualiza o relogio
                    self.shard.clock = max(self.shard.clock + 1, msg["ht"] + 1)
                    self.shard.lst = max(self.shard.lst, msg["lt"])
                    self.shard.rst = max(self.shard.rst, msg["rt"])

                    # Timestamp proposto
                    pt = self.shard.clock

                    # Simula o tempo de execução da transação
                    yield from self.execute_transaction(msg["transaction"])

                    # Guarda a transação em memoria
                    self.shard.prepared[msg["id"]] = [
                        pt,
                        msg["rt"],
                        msg["transaction"],
                    ]

                    # Responde ao prepare do coordenador
                    self.send(
                        msg["src"][0],
                        msg["src"][1],
                        {"type": "prepare_resp", "id": msg["id"], "pt": pt},
                    )
                elif msg["type"] == "prepare_resp":
                    # Acrescenta à estrutura
                    self.shard.waiting_prepare[msg["id"]][0].append(msg["pt"])
                    self.shard.waiting_prepare[msg["id"]][1] -= 1

                    # Se receber de todos envia commit com o timestamp mais alto e finaliza
                    if self.shard.waiting_prepare[msg["id"]][1] == 0:
                        ct = max(self.shard.waiting_prepare[msg["id"]][0])
                        self.send_commit_intra(
                            msg["id"], self.shard.waiting_prepare[msg["id"]][3], ct
                        )

                        self.add_stats(
                            self.shard.waiting_prepare[msg["id"]][2], "write"
                        )

                        # Adiciona ao commit-set do cliente
                        client = self.shard.datacenter.clients[
                            self.shard.waiting_prepare[msg["id"]][4]
                        ]
                        client.write_response(
                            ct, self.shard.waiting_prepare[msg["id"]][5]
                        )

                elif msg["type"] == "commit":
                    # Atualiza o relogio
                    self.shard.clock = max(self.shard.clock, msg["ct"])

                    # Remove dos waiting prepare
                    [_, rt, transaction] = self.shard.prepared.pop(msg["id"])

                    # Adiciona aos commits em espera
                    self.shard.committed[msg["id"]] = [msg["ct"], rt, transaction]
                # type
                elif msg["type"] == "apply_commit":
                    if len(self.shard.prepared):
                        ub = min(map(lambda x: x[0], self.shard.prepared.values()))
                    else:
                        ub = self.shard.clock

                    if len(self.shard.committed):
                        # Ordenar pelo ct
                        C = sorted(self.shard.committed.items(), key=lambda x: x[1][0])

                        for msg_id, item in C:
                            if item[0] <= ub:
                                for key, value in item[2]:
                                    yield from self.use_io()

                                    self.shard.data_storage[key].append(
                                        [
                                            value,
                                            item[0],
                                            item[1],
                                            msg_id,
                                            self.shard.datacenter.id,
                                        ]
                                    )

                                latency = (
                                    self.shard.datacenter.generator.generate_timeout(
                                        self.vars["NETWORK_LATENCY_INTER_DC"]
                                    )
                                )
                                for dc in range(1, self.vars["NUM_DATA_CENTERS"] + 1):
                                    self.send(
                                        dc,
                                        self.shard.id,
                                        {
                                            "type": "replicate",
                                            "id": msg_id,
                                            "item": item,
                                        },
                                        latency,
                                    )
                                if msg_id in self.shard.committed:
                                    self.shard.committed.pop(msg_id)
                elif msg["type"] == "replicate":
                    for key, value in msg["item"][2]:
                        yield from self.use_io()

                        self.shard.data_storage[key].append(
                            [
                                value,
                                msg["item"][0],
                                msg["item"][1],
                                msg["id"],
                                self.shard.datacenter.id,
                            ]
                        )

            self.shard.stop_thread(self.tid, self.type)

            self.is_queue_passive = True
            yield self.queue_passive
