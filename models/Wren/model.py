import simpy
from models.Wren.datacenter import DataCenter


class Model:
    def __init__(self, vars):
        self.env = simpy.Environment()
        self.vars = vars

        if self.vars["NETWORK_CAPACITY"]!=-1:
            self.network = simpy.Resource(self.env, capacity=self.vars["NETWORK_CAPACITY"])
        else:
            self.network = None

        self.datacenters = dict(
            (i, DataCenter(i, self.env, self.vars, self.network, self))
            for i in range(1, self.vars["NUM_DATA_CENTERS"] + 1)
        )

        # Escalonar partições
        for dc, partitions in self.vars["PARTITIONS"].items():
            for shard, partition in partitions.items():
                for p in partition:
                    self.env.process(self.start_partition(p[0], int(dc), int(shard)))
                    self.env.process(self.end_partition(p[1], int(dc), int(shard)))

        self.transaction_time = {
            "read": 0.0,
            "write": 0.0,
            "total": 0.0,
        }
        self.transactions = {"read": 0, "write": 0, "total": 0}

    def start_partition(self, start, datacenter, shard):
        yield self.env.timeout(start)

        self.datacenters[datacenter].shards[shard].is_partition = True

    def end_partition(self, end, datacenter, shard):
        yield self.env.timeout(end)

        self.datacenters[datacenter].shards[shard].is_partition = False
        self.datacenters[datacenter].shards[shard].partition_client.succeed()
        self.datacenters[datacenter].shards[shard].partition_client = self.env.event()
        self.datacenters[datacenter].shards[shard].partition_server.succeed()
        self.datacenters[datacenter].shards[shard].partition_server = self.env.event()

    def add_statistics(self, time, ttype):
        self.transaction_time[ttype] += time
        self.transactions[ttype] += 1

        self.transaction_time["total"] += time
        self.transactions["total"] += 1

    def results(self):
        return self.transaction_time, self.transactions

    def run(self):
        self.env.run(until=self.vars["MAX_SIM_TIME"])
