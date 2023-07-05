from models.Spanner.workload_generator import WorkloadGenerator
from models.Spanner.split import Split

class Zone:
    def __init__(self, id, env, vars, network, model):
        self.id = id
        self.vars = vars
        self.network = network
        self.model = model

        self.env = env

        self.splits = dict(
            (i, Split(i, self.env, self.vars, self.network, self))
            for i in range(1, self.vars["NUM_SPLITS"] + 1)
        )

        self.generator = WorkloadGenerator(self.env, self.vars, self)
