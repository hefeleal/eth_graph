from typing import List, Iterable
from eth_graph.ethereum.block import Block
from neo4j.exceptions import ClientError
from neo4j.v1 import GraphDatabase


class Neo4J:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), max_retry_time=1)

    def close(self):
        self._driver.close()

    def _save_block_in_transaction(self, tx, block: Block):
        tx.run("""      MERGE (pb:Block {hash:$parentHash})
                        MERGE (b:Block {hash:$hash})
                            ON CREATE SET b.number=$number, b.parentHash=$parentHash, b.timestamp=$timestamp
                          MERGE (b)<-[p:PARENT_BLOCK_OF]-(pb)
                        """,
                       hash=block.hash, number=block.number, parentHash=block.parent_hash,
                       timestamp=block.timestamp.timestamp() * 1000)

        for trans in block.transactions:
            tx.run("""MATCH (b:Block {hash:$block_hash})
                        MERGE (tx:Transaction {hash:$tx_hash})
                        SET tx.value=$value
                        SET tx.nonce=$nonce
                        SET tx.input=$input
                        SET tx.index=$index
                        SET tx.gas=$gas
                        SET tx.gasPrice=$gas_price
                      MERGE (b)<-[:TX_FROM_BLOCK]-(tx)
                      MERGE (from:Address {hash:$from_address})
                      MERGE (from)<-[f:TX_FROM]-(tx)""",
                   tx_hash=trans.hash, value=trans.value, block_hash=block.hash,
                   from_address=trans.from_address, nonce=trans.nonce, input=trans.input,
                   index=trans.index, gas=trans.gas, gas_price=trans.gas_price)

            recipient = trans.to_address
            if not recipient:
              recipient = 0
            tx.run("""
                    MATCH (tx:Transaction {hash:$tx_hash})
                    MERGE (to:Address {hash:$to_address})
                    MERGE (to)<-[f:TX_TO]-(tx)
                    """,
                   tx_hash=trans.hash, to_address=recipient)

    def save_blocks(self, blocks: Iterable[Block]):
        with self._driver.session() as session:
            with session.begin_transaction() as tx:
                try:
                    for block in blocks:
                        self._save_block_in_transaction(tx, block)
                except ClientError as e:
                    print(e)
