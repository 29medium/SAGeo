class Network:
    def __init__(self, env, vars, msg, split, network_delay, network):
        self.vars = vars
        self.msg = msg
        self.split = split
        self.network_delay = network_delay
        self.network = network

        self.env = env
        self.process = self.env.process(self.run())

    # Método run do processo da network
    def run(self):
        if self.network:
            net_req = self.network.request()
            yield net_req

        yield self.env.timeout(self.network_delay)

        if self.msg["type"] in [
            "read_timestamp_ack",
            "write_replicate_ack",
            "multi_write_replicate_ack",
            "multi_write_ready_commit",
            "multi_write_ready_abort",
            "multi_write_commit",
            "multi_write_abort",
        ]:
            self.split.add_server_queue(self.msg)
        else:
            self.split.add_client_queue(self.msg)

        if self.network:
            self.network.release(net_req)
