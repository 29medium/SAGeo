import numpy as np
import random


class WorkloadGenerator:
    def __init__(self, env, vars, server):
        self.rnd = random.Random(vars["SEED"])
        np.random.seed(vars["SEED"])
        self.vars = vars
        self.server = server
        self.job_vars = list(
            ("v" + elem) for elem in map(str, range(0, self.vars["DATABASE_SIZE"]))
        )

        self.env = env
        self.process = self.env.process(self.run())

    # Método responsável por gerar um timeout seguindo uma distribuição de poisson
    def generate_timeout(self, mean):
        return np.random.poisson(mean)

    # Método responsável por gerar uma transação
    def generate_transaction(self):
        transaction = list()

        # Gera o tamanho da transação
        size = np.random.poisson(self.vars["TRANSACTION_SIZE"])

        # Gere se a transação é uma query ou um update
        update = self.rnd.randrange(100) < self.vars["UPDATE_TRANSACTIONS"]

        for i in range(size):
            # Gera uma variável aleatória
            if self.vars["DATA_DISTRIBUTION"][0] == "ZIPF":
                    key = np.random.zipf(a=self.vars["DATA_DISTRIBUTION"][1]) - 1
                    if key > self.vars["DATABASE_SIZE"] - 1:
                        key = self.vars["DATABASE_SIZE"] - 1
            elif self.vars["DATA_DISTRIBUTION"][0] == "UNIFORM":
                    key = int(np.random.uniform()) * self.shard_size

            # Adiciona a operação à transação
            if update:
                if self.rnd.randrange(100) < self.vars["WRITE_IN_UPDATE"]:
                    transaction.append((self.job_vars[key], "write"))
                else:
                    transaction.append((self.job_vars[key], "read"))
            else:
                transaction.append((self.job_vars[key], "read"))

        return "update" if update else "query", transaction

    # Método run do processo
    def run(self):
        while True:
            # Simula o tempo entre chegadas
            yield self.env.timeout(self.generate_timeout(self.vars["ARRIVAL_MEAN"]))

            # Gera uma transação
            start_time = self.env.now
            t_type, transaction = self.generate_transaction()

            # Adquire o recurso log
            log_req = self.server.log.request()
            yield log_req

            commit_len = self.server.len_commit_log()

            # Liberta o recurso log
            self.server.log.release(log_req)

            # Adicionar o pedido ao servidor
            self.server.add_executing_queue(
                [
                    "executing",
                    t_type,
                    transaction,
                    start_time,
                    commit_len,
                    self.server.id,
                    None,
                    None,
                ]
            )
