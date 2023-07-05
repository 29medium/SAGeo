from models.DBSM.network import Network

class ExecutingThread:
    def __init__(self, env, vars, server, tid):
        self.vars = vars
        self.server = server
        self.tid = tid
        self.queue = list()

        self.env = env
        self.process = self.env.process(self.run())
        self.passive = self.env.event()
        self.is_passive = False

    def add_queue(self, msg):
        self.queue.append(msg)
        self.wakeup()

    def pop_queue(self):
        return self.queue.pop(0)

    def len_queue(self):
        return len(self.queue)

    def wakeup(self):
        if self.is_passive:
            self.is_passive = False
            self.passive.succeed()
            self.passive = self.env.event()

    def run(self):
        while True:
            while self.len_queue():
                [
                    state,
                    transaction_type,
                    transaction,
                    start_time,
                    commit_size,
                    server_id,
                    read_set,
                    write_set,
                ] = self.pop_queue()

                # Verifica o estado da transação
                if state == "executing":
                    # Ordena os locks
                    vars_lock_order = list(
                        dict.fromkeys(map(lambda x: x[0], transaction))
                    )
                    vars_lock_order.sort()
                    locks = dict()

                    # Adquire os locks
                    for vlo in vars_lock_order:
                        lock = self.server.variable_locks[vlo].request()
                        locks[vlo] = lock
                        yield lock

                    for var, var_type in transaction:
                        # Adquire o recurso cpu
                        cpu_req = self.server.cpu.request()
                        yield cpu_req

                        # Simula o tempo de execução da transação
                        if var_type == "read":
                            cpu_time = self.server.generator.generate_timeout(
                                self.vars["CPU_READ"]
                            )
                        elif var_type == "write":
                            cpu_time = self.server.generator.generate_timeout(
                                self.vars["CPU_EXECUTE"]
                            )
                            
                        yield self.env.timeout(cpu_time)

                        # Libera o recurso cpu
                        self.server.cpu.release(cpu_req)

                    # Libera os locks
                    for vlo in vars_lock_order:
                        self.server.variable_locks[vlo].release(locks[vlo])

                    if (
                        transaction_type == "update"
                        or self.vars["CERTIFICATION"] == "SERIALIZABILITY"
                    ):
                        # Gera o delay com os outros servidores
                        network_delay = self.server.generator.generate_timeout(
                            self.vars["NETWORK_LATENCY_SERVER"]
                        )

                        # Gera o read set e o write set
                        read_set = set(x for x, y in transaction)
                        write_set = set(x for x, y in transaction if y == "write")

                        # Envia a mensagem para todos os servidores
                        for i in range(1, self.vars["NUM_SERVERS"] + 1):
                            Network(
                                self.env,
                                self.vars,
                                [
                                    "commiting",
                                    transaction_type,
                                    transaction,
                                    start_time,
                                    commit_size,
                                    server_id,
                                    read_set,
                                    write_set,
                                ],
                                self.server.model.servers[i],
                                network_delay,
                                self.server.network,
                            )
                    else:
                        # Gero o delay entre o servidor e o cliente
                        network_delay_1 = self.server.generator.generate_timeout(
                            self.vars["NETWORK_LATENCY_CLIENT"]
                        )
                        network_delay_2 = self.server.generator.generate_timeout(
                            self.vars["NETWORK_LATENCY_CLIENT"]
                        )

                        # Adiciona valores às estatísticas
                        self.server.model.add_statistics(
                            self.env.now
                            - start_time
                            + network_delay_1
                            + network_delay_2,
                            "read",
                            False,
                        )
                elif state == "commited":
                    # Ordena os locks
                    vars_lock_order = list(
                        dict.fromkeys(map(lambda x: x[0], transaction))
                    )
                    vars_lock_order.sort()
                    locks = dict()

                    # Adquire os locks
                    for vlo in vars_lock_order:
                        lock = self.server.variable_locks[vlo].request()
                        locks[vlo] = lock
                        yield lock

                    for var in transaction:
                        if var[1] == "write":
                            io_req = self.server.io.request()
                            yield io_req

                            io_time = self.server.generator.generate_timeout(
                                self.vars["IO_WRITE"]
                            )
                            yield self.env.timeout(io_time)

                            self.server.io.release(io_req)

                    # Libera os locks
                    for vlo in vars_lock_order:
                        self.server.variable_locks[vlo].release(locks[vlo])

                    # Gera o delay entre o servidor e o cliente
                    network_delay_1 = self.server.generator.generate_timeout(
                        self.vars["NETWORK_LATENCY_CLIENT"]
                    )
                    network_delay_2 = self.server.generator.generate_timeout(
                        self.vars["NETWORK_LATENCY_CLIENT"]
                    )

                    # Adiciona valores às estatísticas
                    if self.server.id == server_id:
                        if transaction_type == "update":
                            ttype = "write"
                        else:
                            ttype = "read"
                        self.server.model.add_statistics(
                            self.env.now
                            - start_time
                            + network_delay_1
                            + network_delay_2,
                            ttype,
                            False,
                        )
                elif state == "aborted":
                    # Gera o delay entre o servidor e o cliente
                    network_delay_1 = self.server.generator.generate_timeout(
                        self.vars["NETWORK_LATENCY_CLIENT"]
                    )
                    network_delay_2 = self.server.generator.generate_timeout(
                        self.vars["NETWORK_LATENCY_CLIENT"]
                    )

                    # Adiciona valores às estatísticas
                    if transaction_type == "update":
                        ttype = "write"
                    else:
                        ttype = "read"
                    if self.server.id == server_id:
                        self.server.model.add_statistics(
                            self.env.now
                            - start_time
                            + network_delay_1
                            + network_delay_2,
                            ttype,
                            True,
                        )

            # Marca a thread como idle
            self.server.stop_executing_thread(self.tid)

            # Adormece o processo, caso não haja mais mensagens na fila
            self.is_passive = True
            yield self.passive
