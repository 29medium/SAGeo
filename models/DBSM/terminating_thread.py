class TerminatingThread:
    def __init__(self, env, vars, server):
        self.vars = vars
        self.server = server

        self.env = env
        self.process = self.env.process(self.run())
        self.passive = self.env.event()
        self.is_passive = False

    # Método responsável por acordar o processo
    def wakeup(self):
        if self.is_passive:
            self.is_passive = False
            self.passive.succeed()
            self.passive = self.env.event()

    # Método run do processo da terminating thread
    def run(self):
        while True:
            while self.server.len_terminating_queue():
                [
                    _,
                    transaction_type,
                    transaction,
                    start_time,
                    commit_size,
                    server_id,
                    read_set,
                    write_set,
                ] = self.server.pop_terminating_queue()

                # Adquire o recurso cpu
                cpu_req = self.server.cpu.request()
                yield cpu_req

                # Adquire o recurso log
                log_req = self.server.log.request()
                yield log_req

                # Simula o tempo de certificação
                certificate_time = self.server.generator.generate_timeout(
                    self.vars["CPU_CERTIFICATE"]
                )
                yield self.env.timeout(certificate_time)

                # Liberta o recurso cpu
                self.server.cpu.release(cpu_req)

                # Compara o write set e o read set com o write set dos commits efetuados
                aborted = False
                curr_commit_size = self.server.len_commit_log()
                for i in range(commit_size, curr_commit_size):
                    ws = self.server.get_commit_log(i)

                    if self.vars["CERTIFICATION"] == "SNAPSHOT_ISOLATION":
                        if write_set.intersection(ws):
                            aborted = True
                            break

                    if self.vars["CERTIFICATION"] == "SERIALIZABILITY":
                        if read_set.intersection(ws):
                            aborted = True
                            break

                if aborted:
                    msg = [
                        "aborted",
                        transaction_type,
                        transaction,
                        start_time,
                        commit_size,
                        server_id,
                        read_set,
                        write_set,
                    ]
                else:
                    # Adiciona o write set ao commit log
                    self.server.add_commit_log(write_set)

                    msg = [
                        "commited",
                        transaction_type,
                        transaction,
                        start_time,
                        commit_size,
                        server_id,
                        read_set,
                        write_set,
                    ]

                # Liberta o recurso log
                self.server.log.release(log_req)

                # Adicionar o pedido ao servidor
                self.server.add_executing_queue(msg)

            # Adormece o processo, caso não haja mais mensagens na fila
            self.is_passive = True
            yield self.passive
