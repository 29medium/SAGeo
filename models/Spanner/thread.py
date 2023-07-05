from models.Spanner.network import Network
import math

class Thread:
    def __init__(self, tid, env, vars, type, split):
        self.vars = vars
        self.split = split
        if type == "server":
            self.priority = 1
        else:
            self.priority = 2
        self.tid = tid

        # Fila de espera
        self.queue = list()

        # Processos e eventos
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
        cpu_req = self.split.cpu.request(priority=self.priority)
        yield cpu_req

        cpu_time = self.split.zone.generator.generate_timeout(self.vars["CPU_READ"])
        yield self.env.timeout(cpu_time)

        self.split.cpu.release(cpu_req)

    def use_cpu_execute(self):
        cpu_req = self.split.cpu.request(priority=self.priority)
        yield cpu_req

        cpu_time = self.split.zone.generator.generate_timeout(self.vars["CPU_EXECUTE"])
        yield self.env.timeout(cpu_time)

        self.split.cpu.release(cpu_req)

    def use_io(self):
        io_req = self.split.io.request(priority=self.priority)
        yield io_req

        io_time = self.split.zone.generator.generate_timeout(self.vars["IO_WRITE"])
        yield self.env.timeout(io_time)

        self.split.io.release(io_req)

    def read_transaction(self, transaction):
        for _ in transaction:
            yield from self.use_cpu_read()

    def execute_transaction(self, transaction):
        for _ in transaction:
            yield from self.use_cpu_execute()

    def apply_transaction(self, transaction, timestamp):
        for key, value in transaction:
            yield from self.use_io()
            self.split.data_storage[key] = (value, timestamp)

    # Métodos para locks

    def order_locks(self, transaction):
        vars_lock_order = list(dict.fromkeys(map(lambda x: x[0], transaction)))
        vars_lock_order.sort()
        return vars_lock_order

    def aquire_locks(self, transaction, msg_id, timestamp, block=False):
        # Ordena os locks
        variables = self.order_locks(transaction)
        priority = timestamp * 10000000000 + self.tid

        for var in variables:
            lock = self.split.variable_locks[var][0].request(
                preempt=self.split.variable_locks[var][1], priority=priority
            )
            if block:
                self.split.variable_locks[var][1] = False
            self.split.locks[msg_id][0][var] = lock
            yield lock

    def block_locks(self, msg_id):
        for var in self.split.locks[msg_id][0]:
            self.split.variable_locks[var][1] = False

    def realease_locks(self, msg_id):
        for var, lock in self.split.locks[msg_id][0].items():
            for i in range(0, len(self.split.variable_locks[var][0].users)):
                if self.split.variable_locks[var][0].users[i] == lock:
                    self.split.variable_locks[var][0].release(
                        self.split.variable_locks[var][0].users[i]
                    )
                    break

    # Método para adicionar estatísticas

    def add_stats(self, start_time, ttype):
        self.split.zone.model.add_statistics(
            self.env.now - start_time,
            ttype,
        )

    # Métodos para enviar mensagens

    def send(self, zone_id, split_id, msg, latency=0):
        split = self.split.zone.model.zones[zone_id].splits[split_id]
        if latency == 0:
            if zone_id == self.split.zone.id:
                latency = self.split.zone.generator.generate_timeout(
                    self.vars["NETWORK_LATENCY_INTRA_ZONE"]
                )
            else:
                latency = self.split.zone.generator.generate_timeout(
                    self.vars["NETWORK_LATENCY_INTER_ZONE"]
                )
        Network(
            self.env,
            self.vars,
            msg,
            split,
            latency,
            self.split.network,
        )

    def send_split(self, msg):
        latency = self.split.zone.generator.generate_timeout(
            self.vars["NETWORK_LATENCY_INTER_ZONE"]
        )
        for i in range(1, self.vars["NUM_ZONES"] + 1):
            if i != self.split.zone.id:
                self.send(
                    i,
                    self.split.id,
                    msg,
                    latency,
                )

    def send_splits_multi_write_locks(self, transactions, lock_timestamp, msg_id):
        latency_inter = self.split.zone.generator.generate_timeout(
            self.vars["NETWORK_LATENCY_INTER_ZONE"]
        )
        latency_intra = self.split.zone.generator.generate_timeout(
            self.vars["NETWORK_LATENCY_INTRA_ZONE"]
        )
        for split, transaction in transactions.items():
            if split != self.split.id:
                zone = self.split.zone.splits[split].leader_zone
                if self.split.zone.id == zone:
                    latency = latency_intra
                else:
                    latency = latency_inter
                self.send(
                    zone,
                    split,
                    {
                        "type": "multi_write_locks",
                        "src": [self.split.zone.id, self.split.id],
                        "id": msg_id,
                        "transaction": transaction,
                        "lock_timestamp": lock_timestamp,
                    },
                    latency,
                )

    def send_splits_multi_write_commit(self, msg_id):
        latency_inter = self.split.zone.generator.generate_timeout(
            self.vars["NETWORK_LATENCY_INTER_ZONE"]
        )
        latency_intra = self.split.zone.generator.generate_timeout(
            self.vars["NETWORK_LATENCY_INTRA_ZONE"]
        )

        for split in self.split.locks[msg_id][4]:
            if split != self.split.id:
                zone = self.split.zone.splits[split].leader_zone
                if self.split.zone.id == zone:
                    latency = latency_intra
                else:
                    latency = latency_inter
                self.send(
                    zone,
                    split,
                    {
                        "type": "multi_write_commit",
                        "src": [self.split.zone.id, self.split.id],
                        "id": msg_id,
                        "true_time": self.split.locks[msg_id][5],
                    },
                    latency,
                )

    # Métodos para verificar reads em espera
    def check_waiting_reads(self, id, timestamps):
        for key, timestamp in timestamps:
            yield from self.use_cpu_read()

            # Verifica se timestamp recebido corresponde ao timestamp em memoria
            if self.split.data_storage[key][1] == timestamp:
                if (
                    id in self.split.waiting_reads
                    and key in self.split.waiting_reads[id][0]
                ):
                    self.split.waiting_reads[id][0].remove(key)

        # Verifica se todos os itens da transação se encontram atualizados
        if id in self.split.waiting_reads and not len(self.split.waiting_reads[id][0]):
            # Adiciona valores às estatísticas
            self.add_stats(self.split.waiting_reads[id][1], "read")

            # Remove tranação dos reads em espera
            self.split.waiting_reads.pop(id)

    def check_waiting_reads_after_write(self, transaction):
        remove_keys = list()
        # Para todas as entradas waiting_reads verifica se esperam pelo timestamp
        for read_key in list(self.split.waiting_reads.keys()):
            for key in transaction:
                yield from self.use_cpu_read()

                if (
                    read_key in self.split.waiting_reads
                    and key in self.split.waiting_reads[read_key][0]
                ):
                    self.split.waiting_reads[read_key][0].remove(key)

            if read_key in self.split.waiting_reads and not len(
                self.split.waiting_reads[read_key][0]
            ):
                # Adiciona valores às estatísticas
                self.add_stats(self.split.waiting_reads[read_key][1], "read")
                remove_keys.append(read_key)

        for read_key in remove_keys:
            self.split.waiting_reads.pop(read_key)

    def check_commit(self, msg_id):
        if (
            self.split.locks[msg_id][3] == len(self.split.locks[msg_id][4])
            and self.split.locks[msg_id][2] > math.ceil(self.vars["NUM_ZONES"] / 2)
            and not self.split.locks[msg_id][6]
        ):
            # Marca que já enviou commit
            self.split.locks[msg_id][6] = True

            # Envia commit a todos os splits
            self.send_splits_multi_write_commit(msg_id)

            # Envia mensagem para aplicar os dados
            self.send_split(
                {
                    "type": "multi_write_apply",
                    "id": msg_id,
                    "true_time": self.split.locks[msg_id][5],
                }
            )

            # Aplicar no próprio
            yield from self.apply_transaction(
                self.split.locks[msg_id][4][self.split.id],
                self.split.locks[msg_id][5],
            )

            # Liberta os locks
            self.realease_locks(msg_id)

            # Adiciona valores às estatísticas
            self.add_stats(self.split.locks[msg_id][1], "multi_write")

    # Other

    def is_leader(self):
        return self.split.zone.id == self.split.leader_zone

    # Método do processo

    def run(self):
        while len(self.queue):
            msg = self.queue.pop(0)

            if msg["type"] == "read":
                # Precisa dos timestamps do lider
                if not self.is_leader():
                    self.split.msg_id += 1
                    msg_id = self.split.msg_id

                    # Envia pedido de timestamps ao lider
                    self.send(
                        self.split.leader_zone,
                        self.split.id,
                        {
                            "type": "read_timestamp",
                            "src": [self.split.zone.id, self.split.id],
                            "id": msg_id,
                            "transaction": msg["transaction"],
                            "start_time": msg["start_time"],
                        },
                    )
                # Se for o lider, responde diretamente
                else:
                    yield from self.read_transaction(msg["transaction"])

                    # Adiciona valores às estatísticas
                    self.add_stats(msg["start_time"], "read")
            elif msg["type"] == "read_timestamp":
                # Coleta os timestamps das variáveis
                timestamps = list()
                for key in msg["transaction"]:
                    yield from self.use_cpu_read()
                    timestamps.append((key, self.split.data_storage[key][1]))

                # Envia ack com os timestamps
                self.send(
                    msg["src"][0],
                    msg["src"][1],
                    {
                        "type": "read_timestamp_ack",
                        "id": msg["id"],
                        "timestamps": timestamps,
                        "transaction": msg["transaction"],
                        "start_time": msg["start_time"],
                    },
                )
            elif msg["type"] == "read_timestamp_ack":
                # Cria lista com os timestamps a espera de ser confirmados
                self.split.waiting_reads[msg["id"]] = [
                    set(msg["transaction"]),
                    msg["start_time"],
                ]

                # Confirma se os timestamps estão corretos
                yield from self.check_waiting_reads(msg["id"], msg["timestamps"])
            elif msg["type"] == "write":
                self.split.msg_id += 1
                msg_id = self.split.msg_id

                self.split.locks[msg_id] = [dict(), msg["start_time"], 1]

                # Adquire os locks
                lock_timestamp = self.env.now
                yield from self.aquire_locks(
                    msg["transaction"], msg_id, lock_timestamp, True
                )

                # Simula o tempo de execução da transação
                yield from self.execute_transaction(msg["transaction"])

                # Enviar a todos com o mesmo split
                true_time = self.env.now
                self.send_split(
                    {
                        "type": "write_replicate",
                        "id": msg_id,
                        "src": [self.split.zone.id, self.split.id],
                        "transaction": msg["transaction"],
                        "true_time": true_time,
                    }
                )

                # Aplicar alterações
                yield from self.apply_transaction(msg["transaction"], true_time)
            elif msg["type"] == "write_replicate":
                # Aplicar no próprio
                yield from self.apply_transaction(
                    msg["transaction"], msg["true_time"]
                )

                # Responde ao servidor
                self.send(
                    msg["src"][0],
                    msg["src"][1],
                    {
                        "type": "write_replicate_ack",
                        "id": msg["id"],
                    },
                )

                # Verifica se existiam reads em espera
                yield from self.check_waiting_reads_after_write(msg["transaction"])
            elif msg["type"] == "write_replicate_ack":
                msg_id = msg["id"]
                # Incrementa o numero de acks recebidos
                self.split.locks[msg_id][2] += 1

                # Se o numero de acks for uma maioria
                if self.split.locks[msg_id][2] == math.ceil(
                    self.vars["NUM_ZONES"] / 2
                ):
                    # Liberta os locks
                    self.realease_locks(msg_id)

                    # Adiciona valores às estatísticas
                    self.add_stats(self.split.locks[msg_id][1], "write")

            elif msg["type"] == "multi_write":
                self.split.msg_id += 1
                msg_id = (self.split.msg_id, self.split.id)

                self.split.locks[msg_id] = [
                    dict(),
                    msg["start_time"],
                    1,
                    1,
                    msg["transaction"],
                    0,
                    False,
                ]

                # Timestamp para os locks
                lock_timestamp = self.env.now

                # Enviar mensagem para adquirir os locks
                self.send_splits_multi_write_locks(
                    msg["transaction"], lock_timestamp, msg_id
                )

                # Adquire os locks
                yield from self.aquire_locks(
                    msg["transaction"][self.split.id], msg_id, lock_timestamp, True
                )

                # Simula o tempo de execução da transação
                yield from self.execute_transaction(
                    msg["transaction"][self.split.id]
                )

                # Envia mensagem para replicar os dados
                self.send_split(
                    {
                        "type": "multi_write_replicate",
                        "id": msg_id,
                        "src": [self.split.zone.id, self.split.id],
                        "transaction": msg["transaction"][self.split.id],
                    }
                )
            elif msg["type"] == "multi_write_locks":
                self.split.locks[msg["id"]] = [
                    dict(),
                    1,
                    msg["transaction"],
                    msg["src"],
                ]

                lock_timestamp = msg["lock_timestamp"]

                # Adquire os locks
                yield from self.aquire_locks(
                    msg["transaction"], msg["id"], lock_timestamp, True
                )

                # Simula o tempo de execução da transação
                yield from self.execute_transaction(msg["transaction"])

                # Envia mensagem para replicar os dados
                self.send_split(
                    {
                        "type": "multi_write_replicate",
                        "id": msg["id"],
                        "src": [self.split.zone.id, self.split.id],
                        "transaction": msg["transaction"],
                    }
                )
            elif msg["type"] == "multi_write_replicate":
                # Guarda transação mas não aplica
                self.split.locks[msg["id"]] = msg["transaction"]

                # Responde com ack
                self.send(
                    msg["src"][0],
                    msg["src"][1],
                    {
                        "type": "multi_write_replicate_ack",
                        "id": msg["id"],
                    },
                )

            elif (
                msg["type"] == "multi_write_replicate_ack"
                and msg["id"][1] != self.split.id
            ):
                self.split.locks[msg["id"]][1] += 1

                if self.split.locks[msg["id"]][1] == math.ceil(
                    self.vars["NUM_ZONES"] / 2
                ):
                    # Bloqueia os locks
                    self.block_locks(msg["id"])

                    src = self.split.locks[msg["id"]][3]

                    # Envia ready commit ao coordenador
                    self.send(
                        src[0],
                        src[1],
                        {
                            "type": "multi_write_ready_commit",
                            "src": [self.split.zone.id, self.split.id],
                            "id": msg["id"],
                            "timestamp": self.env.now,
                        },
                    )
            elif (
                msg["type"] == "multi_write_replicate_ack"
                and msg["id"][1] == self.split.id
            ):
                self.split.locks[msg["id"]][2] += 1

                if self.split.locks[msg["id"]][2] == math.ceil(
                    self.vars["NUM_ZONES"] / 2
                ):
                    # Bloqueia os locks
                    self.block_locks(msg["id"])

                    yield from self.check_commit(msg["id"])
            elif msg["type"] == "multi_write_ready_commit":
                self.split.locks[msg["id"]][3] += 1

                if msg["timestamp"] > self.split.locks[msg["id"]][5]:
                    self.split.locks[msg["id"]][5] = msg["timestamp"]

                yield from self.check_commit(msg["id"])
            elif msg["type"] == "multi_write_commit":
                # Enviar apply todos as zonas com o mesmo split
                self.send_split(
                    {
                        "type": "multi_write_apply",
                        "id": msg["id"],
                        "true_time": msg["true_time"],
                    }
                )

                # Aplicar no próprio
                yield from self.apply_transaction(
                    self.split.locks[msg["id"]][2], msg["true_time"]
                )

                # Liberta os locks
                self.realease_locks(msg["id"])
            elif msg["type"] == "multi_write_apply":
                # Aplicar no próprio
                yield from self.apply_transaction(
                    self.split.locks[msg["id"]], msg["true_time"]
                )