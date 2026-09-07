"""
network.py
Simulira mrezu: kasnjenje poruka, gubitak poruka, mrezne particije i pad
cvorova (crash faults). Sve greske koje se ubacuju u simulaciju prolaze
kroz ovu klasu.
"""
import asyncio
import random
from typing import Dict, Set, Optional, Tuple


class Network:
    def __init__(self, n: int, base_delay=0.03, jitter=0.03, loss_rate=0.0, log=None):
        self.n = n
        self.base_delay = base_delay
        self.jitter = jitter
        self.loss_rate = loss_rate
        self.inboxes: Dict[int, asyncio.Queue] = {i: asyncio.Queue() for i in range(n)}
        self.crashed: Set[int] = set()
        self.byzantine_silent: Set[int] = set()  # cvorovi koji "cute" (ne salju nista)
        self.partition: Optional[Tuple[Set[int], Set[int]]] = None
        self.extra_delay = 0.0  # dodatno kasnjenje - za simulaciju "blip"-a
        self.log = log or (lambda *a, **k: None)

    # ---- kontrola gresaka ----
    def crash(self, node_id: int):
        self.crashed.add(node_id)
        self.log(f"[FAULT] Cvor {node_id} je PAO (crash).")

    def recover(self, node_id: int):
        self.crashed.discard(node_id)
        self.log(f"[FAULT] Cvor {node_id} se OPORAVIO.")

    def set_partition(self, group_a: Set[int], group_b: Set[int]):
        self.partition = (set(group_a), set(group_b))
        self.log(f"[FAULT] Mrezna particija: {group_a} | {group_b}")

    def heal_partition(self):
        self.partition = None
        self.log("[FAULT] Particija zalecena.")

    def set_blip(self, extra_delay: float):
        self.extra_delay = extra_delay
        if extra_delay:
            self.log(f"[FAULT] Blip - dodatno kasnjenje {extra_delay:.2f}s uvedeno.")
        else:
            self.log("[FAULT] Blip zavrsen - kasnjenje vraceno na normalu.")

    def _is_dropped(self, sender: int, receiver: int) -> bool:
        if sender in self.crashed or receiver in self.crashed:
            return True
        if self.partition:
            a, b = self.partition
            if (sender in a and receiver in b) or (sender in b and receiver in a):
                return True
        if random.random() < self.loss_rate:
            return True
        return False

    async def _deliver(self, receiver: int, message):
        delay = self.base_delay + random.uniform(0, self.jitter) + self.extra_delay
        await asyncio.sleep(delay)
        if receiver not in self.crashed:
            await self.inboxes[receiver].put(message)

    def send(self, sender: int, receiver: int, message):
        if sender in self.crashed or sender in self.byzantine_silent:
            return
        if self._is_dropped(sender, receiver):
            return
        asyncio.ensure_future(self._deliver(receiver, message))

    def broadcast(self, sender: int, message, include_self: bool = True):
        if sender in self.crashed or sender in self.byzantine_silent:
            return
        for r in range(self.n):
            if r == sender and not include_self:
                continue
            if self._is_dropped(sender, r):
                continue
            asyncio.ensure_future(self._deliver(r, message))