import numpy as np
import random


class WorkloadGenerator:
    # Construtor da classe
    def __init__(self, env, vars, zone):
        self.rnd = random.Random(vars["SEED"] + zone.id)
        np.random.seed(vars["SEED"])
        self.vars = vars
        self.zone = zone

        self.shard_size = int(self.vars["DATABASE_SIZE"] / self.vars["NUM_SPLITS"])
        self.variables = dict()
        for i in range(1, vars["NUM_SPLITS"] + 1):
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
            split = self.rnd.randrange(self.vars["NUM_SPLITS"]) + 1
            transaction = list()
            for _ in range(size):
                if self.vars["DATA_DISTRIBUTION"][0] == "ZIPF":
                    key = np.random.zipf(a=self.vars["DATA_DISTRIBUTION"][1]) - 1
                    if key > self.shard_size - 1:
                        key = self.shard_size - 1
                elif self.vars["DATA_DISTRIBUTION"][0] == "UNIFORM":
                    key = int(np.random.uniform()) * self.shard_size

                transaction.append(self.variables[split][key])

            return "read", split, transaction
        elif ttype < (
            self.vars["TRANSACTION_DISTRIBUTION"][0]
            + self.vars["TRANSACTION_DISTRIBUTION"][1]
        ):
            split = self.rnd.randrange(self.vars["NUM_SPLITS"]) + 1
            transaction = list()

            for _ in range(size):
                if self.vars["DATA_DISTRIBUTION"][0] == "ZIPF":
                    key = np.random.zipf(a=self.vars["DATA_DISTRIBUTION"][1]) - 1
                    if key > self.shard_size - 1:
                        key = self.shard_size - 1
                elif self.vars["DATA_DISTRIBUTION"][0] == "UNIFORM":
                    key = int(np.random.uniform()) * self.shard_size

                value = self.rnd.randrange(1000)
                transaction.append((self.variables[split][key], value))

            return "write", split, transaction

        else:
            if self.vars["NUM_SPLITS"]==1:
                num_splits = 1
            else:
                num_splits = self.rnd.randrange(1, self.vars["NUM_SPLITS"]) + 1
            splits = self.rnd.sample(range(1, self.vars["NUM_SPLITS"] + 1), num_splits)
            transaction = dict()
            for s in splits:
                transaction[s] = list()

            for i in range(size):
                split = splits[i % len(splits)]

                if self.vars["DATA_DISTRIBUTION"][0] == "ZIPF":
                    key = np.random.zipf(a=self.vars["DATA_DISTRIBUTION"][1]) - 1
                    if key > self.shard_size - 1:
                        key = self.shard_size - 1
                elif self.vars["DATA_DISTRIBUTION"][0] == "UNIFORM":
                    key = int(np.random.uniform()) * self.shard_size

                value = self.rnd.randrange(1000)
                transaction[split].append((self.variables[split][key], value))

            return "multi_write", splits, transaction

    # Método run do processo
    def run(self):
        while True:
            # Simula o tempo entre chegadas
            yield self.env.timeout(self.generate_timeout(self.vars["ARRIVAL_MEAN"]))

            # Gera uma transação
            start_time = self.env.now
            ttype, split, transaction = self.generate_transaction()

            latency = self.generate_timeout(2 * self.vars["NETWORK_LATENCY_CLIENT"])

            if ttype == "read":
                self.zone.splits[split].add_client_queue(
                    {
                        "type": ttype,
                        "start_time": start_time - latency,
                        "transaction": transaction,
                    },
                )
            elif ttype == "write":
                leader_zone = self.zone.splits[split].leader_zone
                if leader_zone != self.zone.id:
                    latency += self.generate_timeout(
                        2 * self.vars["NETWORK_LATENCY_INTER_ZONE"]
                    )
                self.zone.model.zones[leader_zone].splits[split].add_client_queue(
                    {
                        "type": ttype,
                        "start_time": start_time - latency,
                        "transaction": transaction,
                    },
                )
            elif ttype == "multi_write":
                coordinator_split = min(split)

                leader_zone = self.zone.splits[coordinator_split].leader_zone
                if leader_zone != self.zone.id:
                    latency += self.generate_timeout(
                        2 * self.vars["NETWORK_LATENCY_INTER_ZONE"]
                    )
                self.zone.model.zones[leader_zone].splits[
                    coordinator_split
                ].add_client_queue(
                    {
                        "type": ttype,
                        "start_time": start_time - latency,
                        "transaction": transaction,
                    },
                )
