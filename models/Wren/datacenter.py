from models.Wren.workload_generator import WorkloadGenerator
from models.Wren.shard import Shard
from models.Wren.client import Client


class DataCenter:
    def __init__(self, id, env, vars, network, model):
        self.id = id
        self.vars = vars
        self.network = network
        self.model = model

        self.env = env

        self.shards = dict(
            (i, Shard(i, self.env, self.vars, self.network, self))
            for i in range(1, self.vars["NUM_SHARDS"] + 1)
        )

        self.generator = WorkloadGenerator(self.env, self.vars, self)

        self.clients = dict(
            (i, Client(i, self.env, self.vars, self))
            for i in range(1, self.vars["NUM_CLIENTS_DC"] + 1)
        )
