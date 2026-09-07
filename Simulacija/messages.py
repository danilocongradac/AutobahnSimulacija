"""
messages.py
Definicije svih poruka i pomocnih struktura podataka koje cvorovi razmenjuju
u simulaciji Autobahn protokola (Giridharan et al., SOSP'24).
"""
from dataclasses import dataclass
from typing import Optional, List, Set
import hashlib
import itertools

_tx_counter = itertools.count(1)


def new_batch(size: int) -> List[int]:
    """Kreira 'batch' transakcija - u simulaciji su to samo redni brojevi."""
    return [next(_tx_counter) for _ in range(size)]


def digest(*parts) -> str:
    return hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()[:10]


@dataclass
class Msg:
    sender: int


# ---------------------- Data-dissemination sloj (lanes/cars) ----------------------

@dataclass
class Car:
    proposer: int
    pos: int
    batch: List[int]
    parent: Optional[str]
    cert: Optional["PoA"]

    @property
    def dig(self) -> str:
        return digest(self.proposer, self.pos, tuple(self.batch), self.parent)


@dataclass
class Vote:
    voter: int
    lane: int
    pos: int
    dig: str


@dataclass
class PoA:
    """Proof of Availability - najmanje f+1 glasova za dati car."""
    lane: int
    pos: int
    dig: str
    voters: Set[int]


@dataclass
class TipRef:
    lane: int
    pos: int
    dig: str
    certified: bool


@dataclass
class ConsensusProposal:
    """Predlog konsenzusa - 'cut' preko svih lane-ova"""
    slot: int
    view: int
    cut: List[TipRef]

    @property
    def dig(self) -> str:
        parts = tuple((t.lane, t.pos, t.dig, t.certified) for t in self.cut)
        return digest(self.slot, self.view, parts)


@dataclass
class QC:
    """Quorum Certificate - koristi se i za PrepareQC i za CommitQC."""
    slot: int
    view: int
    dig: str
    voters: Set[int]
    kind: str  # "prepare" | "fast_commit" | "commit"


@dataclass
class Ticket:
    kind: str  # "genesis" | "commitqc" | "tc"
    slot: int
    view: int
    payload: object


# ---------------------- Mrezne poruke ----------------------

@dataclass
class ProposeCar(Msg):
    car: Car


@dataclass
class VoteCar(Msg):
    vote: Vote


@dataclass
class Prepare(Msg):
    proposal: ConsensusProposal
    ticket: Ticket


@dataclass
class PrepVote(Msg):
    slot: int
    view: int
    dig: str


@dataclass
class Confirm(Msg):
    slot: int
    view: int
    prepare_qc: QC


@dataclass
class ConfirmAck(Msg):
    slot: int
    view: int
    dig: str


@dataclass
class CommitMsg(Msg):
    slot: int
    view: int
    commit_qc: QC
    fast: bool


@dataclass
class Timeout(Msg):
    slot: int
    view: int
    high_qc: Optional[QC]
    high_prop: Optional[ConsensusProposal]


@dataclass
class SyncRequest(Msg):
    lane: int
    frm: int
    to: int


@dataclass
class SyncReply(Msg):
    lane: int
    cars: List[Car]


@dataclass
class StatusRequest(Msg):
    """Koristi se za 'catch-up': cvor pita ostale da li je neki slot vec komitovan."""
    slot: int


@dataclass
class StatusReply(Msg):
    slot: int
    proposal: ConsensusProposal
    commit_qc: QC