"""
node.py
Node klasa predstavlja jednu repliku. Svaki Node se izvrsava kao nezavisan
asyncio task (konkurentno izvrsavanje), poseduje sopstveno stanje i
komunicira iskljucivo razmenom poruka preko Network objekta.
"""
import asyncio
import time
from data_layer import DataLayer
from consensus import Consensus
from messages import (ProposeCar, VoteCar, Prepare, PrepVote, Confirm,
                       ConfirmAck, CommitMsg, Timeout, SyncRequest, SyncReply,
                       StatusRequest, StatusReply)


class Node(DataLayer, Consensus):
    def __init__(self, node_id: int, n: int, network, start_time: float, logger):
        self.id = node_id
        self.n = n
        self.f = (n - 1) // 3
        self.net = network
        self.start_time = start_time
        self._logger = logger
        self.running = True
        self.init_data_layer()
        self.init_consensus()

    def log(self, msg: str):
        t = time.monotonic() - self.start_time
        self._logger.info(f"[t={t:6.2f}s][Cvor {self.id}] {msg}")

    async def run(self, tx_interval: float = 0.3, consensus_check_interval: float = 0.2):
        inbox = self.net.inboxes[self.id]
        producer = asyncio.ensure_future(self._tx_producer(tx_interval))
        proposer = asyncio.ensure_future(self._consensus_loop(consensus_check_interval))
        try:
            while self.running:
                try:
                    msg = await asyncio.wait_for(inbox.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue
                self._dispatch(msg)
        finally:
            producer.cancel()
            proposer.cancel()

    async def _tx_producer(self, interval: float):
        while self.running:
            await asyncio.sleep(interval)
            if self.id not in self.net.crashed:
                await self.maybe_propose_car()

    async def _consensus_loop(self, interval: float):
        while self.running:
            await asyncio.sleep(interval)
            if self.id not in self.net.crashed:
                await self.try_start_next_slot()

    def _dispatch(self, msg):
        handlers = {
            ProposeCar: self.handle_propose_car,
            VoteCar: self.handle_vote_car,
            Prepare: self.handle_prepare,
            PrepVote: self.handle_prep_vote,
            Confirm: self.handle_confirm,
            ConfirmAck: self.handle_confirm_ack,
            CommitMsg: self.handle_commit,
            Timeout: self.handle_timeout,
            SyncRequest: self.handle_sync_request,
            SyncReply: self.handle_sync_reply,
            StatusRequest: self.handle_status_request,
            StatusReply: self.handle_status_reply,
        }
        handler = handlers.get(type(msg))
        if handler:
            handler(msg)

    def stop(self):
        self.running = False