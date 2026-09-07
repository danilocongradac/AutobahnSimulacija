"""
data_layer.py
Sloj za diseminaciju podataka: 'lanes' i 'cars' (Propose & Vote sablon iz
rada, Alg. 1). Svaka replika siri sopstvene transakcije gradeci svoju lane,
dok ostale replike glasaju za primljene care-ove cime nastaje Proof of
Availability (PoA). Ovde je i jednostavna sinhronizacija nedostajucih
podataka (SyncRequest/SyncReply).
"""
from typing import Dict, Optional
from messages import Car, Vote, PoA, new_batch, ProposeCar, VoteCar, SyncRequest, SyncReply


class DataLayer:
    def init_data_layer(self):
        self.lanes: Dict[int, Dict[int, Car]] = {i: {} for i in range(self.n)}
        self.certified_tip: Dict[int, int] = {i: 0 for i in range(self.n)}
        self.voted_pos: Dict[int, int] = {i: 0 for i in range(self.n)}
        self.pending_votes: Dict[str, set] = {}
        self.my_last_pos = 0
        self.my_last_poa: Optional[PoA] = None
        self.batch_size = 5

    # ---- proizvodnja sopstvenih cars ----
    async def maybe_propose_car(self, batch_size: int = 5):
        pos = self.my_last_pos + 1
        parent_car = self.lanes[self.id].get(self.my_last_pos)
        parent_dig = parent_car.dig if parent_car else None
        car = Car(proposer=self.id, pos=pos, batch=new_batch(batch_size),
                  parent=parent_dig, cert=self.my_last_poa)
        self.lanes[self.id][pos] = car
        self.voted_pos[self.id] = pos
        self.my_last_pos = pos
        self.log(f"Kreiran novi CAR pos={pos} u sopstvenoj lane.")
        self.net.broadcast(self.id, ProposeCar(sender=self.id, car=car))

    # ---- obrada primljenog cara (glasanje) ----
    def handle_propose_car(self, msg: ProposeCar):
        car = msg.car
        lane, pos = car.proposer, car.pos
        if pos <= self.voted_pos[lane]:
            return  # vec obradjeno / duplikat
        self.lanes[lane][pos] = car  # bafferuj bez obzira na redosled
        if pos != self.voted_pos[lane] + 1:
            # Nedostaju nam ranije pozicije u ovoj lanci (npr. propusteno tokom
            # particije). Bez aktivnog zahteva za sync, ostajemo trajno
            # zaglavljeni: strogo pravilo "samo sledeca pozicija" bi tiho
            # odbacivalo SVE buduce care-ove ove lanke, i proposer nikad vise
            # ne bi mogao da skupi dovoljno glasova za PoA.
            missing_from = self.voted_pos[lane] + 1
            if missing_from < pos:
                self.request_sync(lane, missing_from, pos - 1)
            return
        self._process_buffered_car(lane, car)


    def _process_buffered_car(self, lane: int, car: Car):
        """Glasa za 'car' (sledecu ocekivanu poziciju), zatim kaskadno
        obradjuje sve naredne pozicije koje smo vec bafferovali van reda."""
        pos = car.pos
        if pos > 1:
            parent = self.lanes[lane].get(pos - 1)
            if parent is None or parent.dig != car.parent:
                return
            if car.cert:
                self.certified_tip[lane] = max(self.certified_tip[lane], pos - 1)
        self.voted_pos[lane] = pos
        vote = Vote(voter=self.id, lane=lane, pos=pos, dig=car.dig)
        self.log(f"Glasam za CAR lane={lane} pos={pos}.")
        self.net.send(self.id, car.proposer, VoteCar(sender=self.id, vote=vote))
        next_pos = pos + 1
        while next_pos in self.lanes[lane]:
            self._process_buffered_car(lane, self.lanes[lane][next_pos])
            next_pos += 1


    

    def handle_vote_car(self, msg: VoteCar):
        v = msg.vote
        if v.lane != self.id:
            return
        self.pending_votes.setdefault(v.dig, set()).add(v.voter)
        if len(self.pending_votes[v.dig]) >= self.f + 1:
            poa = PoA(lane=self.id, pos=v.pos, dig=v.dig, voters=set(self.pending_votes[v.dig]))
            self.my_last_poa = poa
            self.certified_tip[self.id] = max(self.certified_tip[self.id], v.pos)
            self.log(f"Formiran PoA za sopstvenu lane pos={v.pos} ({len(poa.voters)} glasova).")

    # ---- sinhronizacija nedostajucih cars ----
    def missing_range(self, lane: int, up_to: int):
        have = self.lanes[lane]
        last = self.last_commit.get(lane, 0)
        return [p for p in range(last + 1, up_to + 1) if p not in have]

    def request_sync(self, lane: int, frm: int, to: int):
        self.log(f"Zahtevam SYNC za lane={lane} od pos={frm} do {to}.")
        self.net.broadcast(self.id, SyncRequest(sender=self.id, lane=lane, frm=frm, to=to),
                            include_self=False)

    def handle_sync_request(self, msg: SyncRequest):
        cars = [self.lanes[msg.lane][p] for p in range(msg.frm, msg.to + 1) if p in self.lanes[msg.lane]]
        if len(cars) == (msg.to - msg.frm + 1):
            self.net.send(self.id, msg.sender, SyncReply(sender=self.id, lane=msg.lane, cars=cars))

    def handle_sync_reply(self, msg: SyncReply):
        for c in msg.cars:
            self.lanes[msg.lane][c.pos] = c
        # Sustizemo voted_pos onoliko koliko imamo neprekinut niz pozicija -
        # ovo omogucava da posle sync-a mozemo opet normalno da glasamo za
        # NAREDNE care-ove u ovoj lanci (bez ovoga bi svaki novi car i dalje
        # dolazio "van reda" i bio odbacen).
        lane = msg.lane
        pos = self.voted_pos[lane] + 1
        while pos in self.lanes[lane]:
            self.voted_pos[lane] = pos
            pos += 1
        if msg.cars:
            highest = max(c.pos for c in msg.cars)
            self.certified_tip[lane] = max(self.certified_tip[lane], highest)
        self.log(f"Primljen SYNC odgovor za lane={lane} ({len(msg.cars)} cars), voted_pos sada {self.voted_pos[lane]}.")