import simpy
from models.Spanner.zone import Zone

class Model:
    def __init__(self, vars):
        self.env = simpy.Environment()
        self.vars = vars

        if self.vars["NETWORK_CAPACITY"]!=-1:
            self.network = simpy.Resource(self.env, capacity=self.vars["NETWORK_CAPACITY"])
        else:
            self.network = None

        self.zones = dict(
            (i, Zone(i, self.env, self.vars, self.network, self))
            for i in range(1, self.vars["NUM_ZONES"] + 1)
        )

        for zone, partitions in self.vars["PARTITIONS"].items():
            for split, partition in partitions.items():
                for p in partition:
                    self.env.process(self.start_partition(p[0], int(zone), int(split)))
                    self.env.process(self.end_partition(p[1], int(zone), int(split)))

        self.transaction_time = {
            "read": 0.0,
            "write": 0.0,
            "multi_write": 0.0,
            "total": 0.0,
        }
        self.transactions = {"read": 0, "write": 0, "multi_write": 0, "total": 0}

    def start_partition(self, start, zone, split):
        yield self.env.timeout(start)

        self.zones[zone].splits[split].is_partition = True

    def end_partition(self, end, zone, split):
        yield self.env.timeout(end)

        self.zones[zone].splits[split].is_partition = False
        self.zones[zone].splits[split].partition_client.succeed()
        self.zones[zone].splits[split].partition_client = self.env.event()
        self.zones[zone].splits[split].partition_server.succeed()
        self.zones[zone].splits[split].partition_server = self.env.event()

    def add_statistics(self, time, ttype):
        self.transaction_time[ttype] += time
        self.transactions[ttype] += 1

        self.transaction_time["total"] += time
        self.transactions["total"] += 1

    def results(self):
        return self.transaction_time, self.transactions

    def run(self):
        self.env.run(until=self.vars["MAX_SIM_TIME"])
