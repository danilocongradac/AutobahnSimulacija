"""
consensus.py
Sloj konsenzusa: slotovi, view-ovi, Prepare/Confirm/Commit faze (PBFT stil,
Sec. 5.2.1), fast-path za graciozne intervale i view-change (Timeout/TC)
logika (Sec. 5.3). Konsenzus napreduje sekvencijalno kroz slotove
(pojednostavljenje u odnosu na paralelne slotove iz Sec. 5.4 rada).
"""
import asyncio
import time
from typing import Dict, List, Optional
from messages import (ConsensusProposal, TipRef, QC, Ticket, Prepare, PrepVote, Confirm,
                       ConfirmAck, CommitMsg, Timeout, StatusRequest, StatusReply)

FAST_WAIT = 0.15      # koliko lider ceka dodatne glasove nakon n-f (za fast-path)
VIEW_TIMEOUT = 1.5    # osnovni tajmaut za view


class Consensus:
    def init_consensus(self):
        self.view: Dict[int, int] = {}
        self.last_commit: Dict[int, int] = {i: 0 for i in range(self.n)}
        self.log_out: List = []          # (slot, lane, pos, br_transakcija)
        self.committed_slots: set = set()
        self.commit_qc: Dict[int, QC] = {}
        self.props: Dict[tuple, ConsensusProposal] = {}
        self.prep_votes: Dict[tuple, set] = {}
        self.confirms: Dict[tuple, QC] = {}
        self.confirm_acks: Dict[tuple, set] = {}
        self.timeouts_seen: Dict[tuple, dict] = {}
        self.mutinied: set = set()
        self.timer_tasks: Dict[tuple, asyncio.Task] = {}
        self.high_qc: Dict[int, Optional[QC]] = {}
        self.next_free_slot = 0
        self._last_status_req_slot = -1
        self._last_status_req_time = 0.0
        self._counted_positions = {i: set() for i in range(self.n)}
        # pending commits for slots that arrived out of order
        self.pending_commits: Dict[int, tuple] = {}  # slot -> (proposal, qc)

    def leader_for(self, slot: int, view: int) -> int:
        return (slot + view) % self.n

    # ---------------- POKRETANJE NOVOG SLOTA (lider) ----------------

    async def try_start_next_slot(self):
        slot = self.next_free_slot
        if slot in self.committed_slots:
            return
        view = self.view.get(slot, 0)
        self.start_timer(slot, view)

        leader = self.leader_for(slot, view)
        if leader != self.id:
            if (slot, view) not in self.props:
                now = time.monotonic()
                if slot != self._last_status_req_slot or now - self._last_status_req_time > 1.0:
                    self._last_status_req_slot = slot
                    self._last_status_req_time = now
                    self.log(f"[SYNC] Nemam informaciju o slotu={slot}, pitam ostale (StatusRequest).")
                    self.net.broadcast(self.id, StatusRequest(sender=self.id, slot=slot), include_self=False)
            return

        if (slot, view) in self.props:
            return
        cut = self.build_cut()
        new_tips = sum(1 for t in cut if t.pos > self.last_commit[t.lane])
        if slot != 0 and new_tips < self.n - self.f:
            return
        if slot == 0:
            ticket = Ticket(kind="genesis", slot=-1, view=0, payload=None)
        elif (slot - 1) in self.commit_qc:
            ticket = Ticket(kind="commitqc", slot=slot - 1, view=self.commit_qc[slot - 1].view,
                            payload=self.commit_qc[slot - 1])
        else:
            return
        proposal = ConsensusProposal(slot=slot, view=view, cut=cut)
        self.props[(slot, view)] = proposal
        self.log(f"[LIDER] Predlazem slot={slot} view={view} cut={[(t.lane, t.pos) for t in cut]}")
        self.net.broadcast(self.id, Prepare(sender=self.id, proposal=proposal, ticket=ticket))

    def build_cut(self) -> List[TipRef]:
        cut = []
        for lane in range(self.n):
            pos = self.certified_tip[lane] if self.certified_tip[lane] else self.last_commit[lane]
            car = self.lanes[lane].get(pos)
            dig = car.dig if car else ""
            cut.append(TipRef(lane=lane, pos=pos, dig=dig, certified=True))
        return cut

    # ---------------- PREPARE FAZA ----------------

    def handle_prepare(self, msg: Prepare):
        p = msg.proposal
        slot, view = p.slot, p.view
        if self.leader_for(slot, view) != msg.sender:
            return
        if view < self.view.get(slot, 0):
            return
        self.view[slot] = view
        self.props[(slot, view)] = p
        self.start_timer(slot, view)
        self.log(f"Primljen PREPARE za slot={slot} view={view}, glasam.")
        self.net.send(self.id, msg.sender, PrepVote(sender=self.id, slot=slot, view=view, dig=p.dig))
        for t in p.cut:
            missing = self.missing_range(t.lane, t.pos)
            if missing:
                self.request_sync(t.lane, missing[0], missing[-1])

    def handle_prep_vote(self, msg: PrepVote):
        key = (msg.slot, msg.view)
        if self.leader_for(*key) != self.id or key[0] in self.committed_slots:
            return
        votes = self.prep_votes.setdefault(key, set())
        votes.add(msg.sender)
        if len(votes) == self.n:
            self._form_and_commit(key, votes, kind="fast_commit")
        elif len(votes) == self.n - self.f:
            asyncio.get_event_loop().call_later(
                FAST_WAIT, lambda: asyncio.ensure_future(self._resolve_prepare(key)))

    async def _resolve_prepare(self, key):
        if key[0] in self.committed_slots or key in self.confirms:
            return
        votes = self.prep_votes.get(key, set())
        if len(votes) == self.n:
            self._form_and_commit(key, votes, kind="fast_commit")
            return
        if len(votes) >= self.n - self.f:
            proposal = self.props[key]
            qc = QC(slot=key[0], view=key[1], dig=proposal.dig, voters=set(votes), kind="prepare")
            self.confirms[key] = qc
            self.high_qc[key[0]] = qc
            self.log(f"[LIDER] Formiran PrepareQC za slot={key[0]} view={key[1]} - prelazim na CONFIRM.")
            self.net.broadcast(self.id, Confirm(sender=self.id, slot=key[0], view=key[1], prepare_qc=qc))

    def _form_and_commit(self, key, voters, kind):
        if key[0] in self.committed_slots:
            return
        proposal = self.props[key]
        qc = QC(slot=key[0], view=key[1], dig=proposal.dig, voters=set(voters), kind=kind)
        self.commit_qc[key[0]] = qc
        self.log(f"[LIDER] FAST-PATH commit za slot={key[0]} view={key[1]}.")
        self.net.broadcast(self.id, CommitMsg(sender=self.id, slot=key[0], view=key[1], commit_qc=qc, fast=True))
        self.do_commit(key[0], proposal, qc)

    # ---------------- CONFIRM FAZA ----------------

    def handle_confirm(self, msg: Confirm):
        key = (msg.slot, msg.view)
        if key not in self.props:
            return
        self.confirms[key] = msg.prepare_qc
        proposal = self.props[key]
        self.log(f"Primljen CONFIRM za slot={msg.slot} view={msg.view}, saljem ACK.")
        self.net.send(self.id, msg.sender, ConfirmAck(sender=self.id, slot=msg.slot, view=msg.view, dig=proposal.dig))

    def handle_confirm_ack(self, msg: ConfirmAck):
        key = (msg.slot, msg.view)
        if self.leader_for(*key) != self.id or key[0] in self.committed_slots:
            return
        acks = self.confirm_acks.setdefault(key, set())
        acks.add(msg.sender)
        if len(acks) == self.n - self.f:
            proposal = self.props[key]
            qc = QC(slot=key[0], view=key[1], dig=proposal.dig, voters=set(acks), kind="commit")
            self.commit_qc[key[0]] = qc
            self.log(f"[LIDER] SLOW-PATH CommitQC formiran za slot={key[0]} view={key[1]}.")
            self.net.broadcast(self.id, CommitMsg(sender=self.id, slot=key[0], view=key[1], commit_qc=qc, fast=False))
            self.do_commit(key[0], proposal, qc)

    # ---------------- COMMIT ----------------

    def handle_commit(self, msg: CommitMsg):
        key = (msg.slot, msg.view)
        if key not in self.props:
            return
        self.commit_qc[msg.slot] = msg.commit_qc
        self.do_commit(msg.slot, self.props[key], msg.commit_qc)

    def handle_status_request(self, msg: StatusRequest):
        slot = msg.slot
        if slot in self.commit_qc:
            qc = self.commit_qc[slot]
            proposal = self.props.get((slot, qc.view))
            if proposal:
                self.log(f"[SYNC] Saljem StatusReply za slot={slot} cvoru {msg.sender}.")
                self.net.send(self.id, msg.sender,
                            StatusReply(sender=self.id, slot=slot, proposal=proposal, commit_qc=qc))

    def handle_status_reply(self, msg: StatusReply):
        if msg.slot in self.committed_slots:
            return
        self.log(f"[SYNC] Primljen StatusReply za slot={msg.slot}, sustizem propusteno.")
        self.props[(msg.slot, msg.commit_qc.view)] = msg.proposal
        self.commit_qc[msg.slot] = msg.commit_qc
        self.do_commit(msg.slot, msg.proposal, msg.commit_qc)

    def do_commit(self, slot: int, proposal: ConsensusProposal, qc: QC):
        """Komituje slot samo ako je sledeći po redu; inače ga čuva kao pending."""
        if slot in self.committed_slots:
            return
        if slot != self.next_free_slot:
            # Čuvamo za kasnije i tražimo propuštene slotove
            self.pending_commits[slot] = (proposal, qc)
            self.log(f"[SYNC] Slot {slot} nije sledeći (očekujem {self.next_free_slot}), čuvam pending i tražim propuštene.")
            for s in range(self.next_free_slot, slot):
                self._send_status_request(s)
            return

        # Komitujemo ovaj slot
        self.committed_slots.add(slot)
        self.cancel_timer(slot, proposal.view)
        for t in proposal.cut:
            missing = self.missing_range(t.lane, t.pos)
            if missing:
                self.request_sync(t.lane, missing[0], missing[-1])
            counted = self._counted_positions[t.lane]
            for p in range(self.last_commit[t.lane] + 1, t.pos + 1):
                if p in counted:
                    continue
                counted.add(p)
                car = self.lanes[t.lane].get(p)
                count = len(car.batch) if car else self.batch_size
                self.log_out.append((slot, t.lane, p, count))
            self.last_commit[t.lane] = max(self.last_commit[t.lane], t.pos)
        total_tx = sum(x[3] for x in self.log_out)
        self.log(f"*** KOMITOVAN slot={slot} (view={proposal.view}) - ukupno transakcija: {total_tx} ***")
        self.next_free_slot = slot + 1

        # Nakon što smo komitovali, pokušavamo da komitujemo sledeće pending slotove
        while self.next_free_slot in self.pending_commits:
            p, q = self.pending_commits.pop(self.next_free_slot)
            self.do_commit(self.next_free_slot, p, q)

    def _send_status_request(self, slot: int):
        """Pomoćna funkcija za slanje StatusRequest za dati slot."""
        self.log(f"[SYNC] Zahtevam status za slot={slot} (propušten).")
        self.net.broadcast(self.id, StatusRequest(sender=self.id, slot=slot), include_self=False)

    # ---------------- VIEW CHANGE ----------------

    def start_timer(self, slot: int, view: int):
        key = (slot, view)
        if key in self.timer_tasks:
            return
        self.timer_tasks[key] = asyncio.ensure_future(self._timer(slot, view))

    def cancel_timer(self, slot: int, view: int):
        t = self.timer_tasks.pop((slot, view), None)
        if t:
            t.cancel()

    async def _timer(self, slot: int, view: int):
        while True:
            await asyncio.sleep(VIEW_TIMEOUT)
            if slot in self.committed_slots:
                return
            if self.view.get(slot, 0) > view:
                return
            self.log(f"[TIMEOUT] Slot={slot} view={view} - saljem TIMEOUT poruku (ponovni pokusaj).")
            tmsg = Timeout(sender=self.id, slot=slot, view=view,
                            high_qc=self.high_qc.get(slot), high_prop=self.props.get((slot, view)))
            self.net.broadcast(self.id, tmsg)

    def handle_timeout(self, msg: Timeout):
        if msg.slot in self.committed_slots:
            qc = self.commit_qc.get(msg.slot)
            if qc:
                proposal = self.props.get((msg.slot, qc.view))
                if proposal:
                    self.net.send(self.id, msg.sender,
                                StatusReply(sender=self.id, slot=msg.slot, proposal=proposal, commit_qc=qc))
            return
        key = (msg.slot, msg.view)
        d = self.timeouts_seen.setdefault(key, {})
        d[msg.sender] = msg
        if len(d) >= self.f + 1 and key not in self.mutinied:
            self.mutinied.add(key)
            self.log(f"[VIEW-CHANGE] Pridruzujem se pobuni za slot={msg.slot} view={msg.view}.")
            mine = Timeout(sender=self.id, slot=msg.slot, view=msg.view,
                            high_qc=self.high_qc.get(msg.slot), high_prop=self.props.get(key))
            self.net.broadcast(self.id, mine)
            d[self.id] = mine
        if len(d) >= self.n - self.f:
            self._form_tc(key, list(d.values()))

    def _form_tc(self, key, timeouts):
        slot, view = key
        if slot in self.committed_slots:
            return
        new_view = view + 1
        if self.view.get(slot, 0) >= new_view:
            return
        self.view[slot] = new_view
        self.log(f"[VIEW-CHANGE] Formiran TC za slot={slot}, prelazim na view={new_view}.")
        winning = self._pick_winning_proposal(timeouts)
        self.start_timer(slot, new_view)
        if self.leader_for(slot, new_view) == self.id:
            ticket = Ticket(kind="tc", slot=slot, view=view, payload=timeouts)
            cut = winning.cut if winning is not None else self.build_cut()
            proposal = ConsensusProposal(slot=slot, view=new_view, cut=cut)
            self.props[(slot, new_view)] = proposal
            self.log(f"[LIDER] (posle view-change-a) Predlazem slot={slot} view={new_view}.")
            self.net.broadcast(self.id, Prepare(sender=self.id, proposal=proposal, ticket=ticket))

    def _pick_winning_proposal(self, timeouts) -> Optional[ConsensusProposal]:
        with_qc = [t for t in timeouts if t.high_qc is not None]
        if with_qc:
            best = max(with_qc, key=lambda t: t.high_qc.view)
            return best.high_prop
        counts: Dict[str, list] = {}
        for t in timeouts:
            if t.high_prop is not None:
                counts.setdefault(t.high_prop.dig, []).append(t.high_prop)
        for props in counts.values():
            if len(props) >= self.f + 1:
                return props[0]
        return None