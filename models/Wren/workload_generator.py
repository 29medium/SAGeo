import numpy as np
import random

class WorkloadGenerator:
    def __init__(self, env, vars, datacenter):
        self.rnd = random.Random(vars["SEED"] + datacenter.id)
        np.random.seed(vars["SEED"])
        self.vars = vars
        self.datacenter = datacenter

        self.shard_size = int(self.vars["DATABASE_SIZE"] / self.vars["NUM_SHARDS"])
        self.variables = dict()
        for i in range(1, vars["NUM_SHARDS"] + 1):
            self.variables[i] = list(
                ("v" + elem)
                for elem in map(
                    str, range(self.shard_size * (i - 1), self.shard_size * i)
                )
            )

        self.env = env
        self.process = self.env.process(self.run())

    # Método responsável por gerar um timeout seguindo uma distribuição de poisson
    def generate_timeout(self, mean):
        return np.random.poisson(mean)

    # Método responsável por gerar uma transação
    def generate_transaction(self):
        # Gera o tamanho da transação
        size = np.random.poisson(self.vars["TRANSACTION_SIZE"])

        # Gere se a transação é uma query ou um update
        ttype = self.rnd.randrange(100)

        if ttype < self.vars["TRANSACTION_DISTRIBUTION"][0]:
            # num_shards = self.rnd.randrange(1, self.vars["NUM_SHARDS"] + 1)
            num_shards = self.vars["TRANSACTION_PARTITIONS"]
            shards = self.rnd.sample(range(1, self.vars["NUM_SHARDS"] + 1), num_shards)
            transaction = dict()

            for s in shards:
                transaction[s] = list()

            for i in range(size):
                shard = shards[i % len(shards)]

                if self.vars["DATA_DISTRIBUTION"][0] == "ZIPF":
                    key = np.random.zipf(a=self.vars["DATA_DISTRIBUTION"][1]) - 1
                    if key > self.shard_size - 1:
                        key = self.shard_size - 1
                elif self.vars["DATA_DISTRIBUTION"][0] == "UNIFORM":
                    key = int(np.random.uniform()) * self.shard_size
                
                transaction[shard].append(self.variables[shard][key])

            return "read", transaction
        else:
            # num_shards = self.rnd.randrange(1, self.vars["NUM_SHARDS"] + 1)
            num_shards = self.vars["TRANSACTION_PARTITIONS"]
            shards = self.rnd.sample(range(1, self.vars["NUM_SHARDS"] + 1), num_shards)
            transaction = dict()

            for s in shards:
                transaction[s] = list()

            for i in range(size):
                shard = shards[i % len(shards)]
                
                if self.vars["DATA_DISTRIBUTION"][0] == "ZIPF":
                    key = np.random.zipf(a=self.vars["DATA_DISTRIBUTION"][1]) - 1
                    if key > self.shard_size - 1:
                        key = self.shard_size - 1
                elif self.vars["DATA_DISTRIBUTION"][0] == "UNIFORM":
                    key = int(np.random.uniform()) * self.shard_size

                value = self.rnd.randrange(1000)
                transaction[shard].append((self.variables[shard][key], value))

            return "write", transaction

    def generate_client(self):
        return self.rnd.randrange(1, self.vars["NUM_CLIENTS_DC"] + 1)

    # Método run do processo
    def run(self):
        while True:
            # Simula o tempo entre chegadas
            yield self.env.timeout(self.generate_timeout(self.vars["ARRIVAL_MEAN"]))

            # Gera uma transação
            ttype, transaction = self.generate_transaction()
            client = self.generate_client()

            if ttype == "read":
                self.datacenter.clients[client].read_request(transaction)
            else:
                self.datacenter.clients[client].write_request(transaction)
