import json
from eth_graph.ethereum.block import GethBlock
from web3 import IPCProvider
import requests


class Geth():
    def __init__(self, ipc_path):
        self.provider = IPCProvider(ipc_path, timeout=60)

    def last_block(self):
        pass

    def get_block(self, block_num):
        """Get a specific block from the blockchain and filter the data."""
        data = self.provider.make_request("eth_getBlockByNumber", [hex(block_num), True])["result"]
        return GethBlock(data)

    def get_blocks(self, start_num, end_num):
        for n in range(start_num, end_num):
            yield self.get_block(n)
