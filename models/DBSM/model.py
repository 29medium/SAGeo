import simpy
from models.DBSM.server import Server

class Model:
    def __init__(self, vars):
        self.env = simpy.Environment()
        self.vars = vars

        if self.vars["NETWORK_CAPACITY"]!=-1:
            self.network = simpy.Resource(self.env, capacity=self.vars["NETWORK_CAPACITY"])
        else:
            self.network = None

        self.servers = dict(
            (i, Server(i, self.env, self.vars, self.network, self))
            for i in range(1, self.vars["NUM_SERVERS"] + 1)
        )

        self.transaction_time = {
            "read": 0.0,
            "write": 0.0,
            "total": 0.0,
            "aborted": 0.0,
        }
        self.transactions = {"read": 0, "write": 0, "total": 0, "aborted": 0}

    def add_statistics(self, time, ttype, aborted):
        self.transaction_time[ttype] += time
        self.transactions[ttype] += 1

        self.transaction_time["total"] += time
        self.transactions["total"] += 1

        if aborted:
            self.transaction_time["aborted"] += time
            self.transactions["aborted"] += 1

    def results(self):
        return self.transaction_time, self.transactions

    def run(self):
        self.env.run(until=self.vars["MAX_SIM_TIME"])
