class Client:
    def __init__(self, id, env, vars, datacenter):
        self.id = id
        self.env = env
        self.vars = vars
        self.datacenter = datacenter

        self.rst = 0
        self.lst = 0
        self.hwtc = 0

        self.commit_set = list()  # list (keys, values, timestamp)

    def start(self, rst, lst):
        if self.vars["CLIENT_CACHE"]:
            self.read_set = list()

            self.commit_set[:] = [
                (k, v, t) for (k, v, t) in self.commit_set if t >= lst
            ]

        self.rst = rst
        self.lst = lst

    # requests
    def empty_transaction(self, transaction):
        for v in transaction.values():
            if v != []:
                return False
        return True

    def send(self, ttype, transaction):
        if self.empty_transaction(transaction):
            self.datacenter.model.add_statistics(0, ttype)
        else:
            start_time = self.env.now
            latency = self.datacenter.generator.generate_timeout(
                2 * self.vars["NETWORK_LATENCY_CLIENT"]
            )
            # coordinator_shard = min(transaction.keys())
            coordinator_shard = (self.id % self.vars["NUM_SHARDS"]) + 1

            if ttype == "read":
                self.datacenter.shards[coordinator_shard].add_client_queue(
                    {
                        "type": ttype,
                        "start_time": start_time - latency,
                        "transaction": transaction,
                        "client": self.id,
                        "lstc": self.lst,
                        "rstc": self.rst,
                    },
                )
            else:
                self.datacenter.shards[coordinator_shard].add_client_queue(
                    {
                        "type": ttype,
                        "start_time": start_time - latency,
                        "transaction": transaction,
                        "client": self.id,
                        "lstc": self.lst,
                        "rstc": self.rst,
                        "hwtc": self.hwtc,
                    },
                )

    def read_request(self, transaction):
        if self.vars["CLIENT_CACHE"]:
            newTransaction = dict()

            for s, tran in transaction.items():
                newTransaction[s] = list()

                for t in tran:
                    inMemory = False

                    # check commit_set
                    if not inMemory:
                        for c in self.commit_set:
                            if c[0] == t:
                                inMemory = True

                    if not inMemory:
                        newTransaction[s].append(t)
        else:
            newTransaction = transaction

        self.send("read", newTransaction)

    def write_request(self, transaction):
        self.send("write", transaction)

    # responses

    def write_response(self, ct, write_set):
        if self.vars["CLIENT_CACHE"]:
            for w in write_set:
                self.commit_set.append((w[0], w[1], ct))

        self.hwtc = ct
