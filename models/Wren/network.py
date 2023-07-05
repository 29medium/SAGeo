class Network:
    def __init__(self, env, vars, msg, split, network_delay, network):
        self.vars = vars
        self.msg = msg
        self.split = split
        self.network_delay = network_delay
        self.network = network

        self.env = env
        self.process = self.env.process(self.run())

    def run(self):
        if self.network:
            net_req = self.network.request()
            yield net_req

        yield self.env.timeout(self.network_delay)

        self.split.add_server_queue(self.msg)

        if self.network:
            self.network.release(net_req)
