AUTOBAHN BFT

FAJLOVI

    messages.py - definicije svih poruka i struktura podataka (Car, PoA, ConsensusProposal, QC, Prepare/Confirm/Commit, Timeout, Sync/Status poruke).

    network.py - simulacija mreze sa kasnjenjem, gubitkom paketa, particijama, padom cvorova (crash) i blip efektom. Ova komponenta je zaduzena za ubacivanje gresaka u sistem.

    data_layer.py - sloj za diseminaciju podataka - upravljanje lane-ovima i car-ovima, glasanje za automobile, formiranje Proof of Availability (PoA) i sinhronizacija propustenih podataka (SyncRequest/Reply).

    consensus.py - sloj konsenzusa - implementacija Prepare/Confirm/Commit faza, fast-path (brzi put), slow-path (spori put) i view-change mehanizma (Timeout/TC).

    node.py - klasa Node koja nasledjuje DataLayer i Consensus, predstavlja jednu repliku. Svaki cvor se izvrsava kao nezavisan asyncio task i komunicira iskljucivo razmenom poruka.

    main.py - interaktivni meni za podesavanje i pokretanje simulacije. Omogucava izbor broja cvorova, trajanja, scenarija greske i tajminga.

POKRETANJE I KONFIGURACIJA

    Zahtevi: Python 3.9+ (bez spoljnih biblioteka).

Simulator se pokrece komandom:

    "python main.py"

Nakon pokretanja, pojavljuje se interaktivni meni u kome se podesavaju parametri:

    1. Broj cvorova - n = 3f + 1 (podrazumevano 4, sto daje f = 1).
    2. Ukupno trajanje simulacije - u sekundama (podrazumevano 20.0).
    3. Scenarij greske - bira se jedan od pet ponudjenih (opisani u nastavku).
    4. Tajming greske (samo za scenarije sa greskom) - trenutak pocetka (fault_start) i trajanje greske.
    - Za blip scenario se dodatno podesava i intenzitet kasnjenja.

    Nakon unosa, prikazuje se rezime podesavanja i potvrda za pokretanje. Po zavrsetku simulacije, meni se automatski vraca - omogucavajuci pokretanje novih scenarija bez ponovnog startovanja programa.

DOSTUPNI SCENARIJI GRESAKA

    1. none - Normalan rad, bez gresaka.
    2. leader_crash - Pad lidera (cvor 0 prestaje da salje poruke na odredjeno vreme).
    3. byzantine_leader - Vizantijski lider (cvor 0 cuti - ne predlaze nove slotove, ali i dalje odgovara na neke poruke).
    4. partition - Mrezna particija (cvorovi se dele u dve grupe koje ne mogu da komuniciraju).
    5. blip - Naglo povecanje kasnjenja svih poruka na mrezi.

PRACENJE RADA I REZULTATI

    Tokom izvrsavanja, u konzolu se ispisuju vremenski logovi svih znacajnih dogadjaja:

    - Kreiranje i glasanje za car-ove
    - Slanje i prijem PREPARE / CONFIRM / COMMIT poruka
    - Formiranje QC (Quorum Certificate) i PoA (Proof of Availability)
    - Timeout i view-change dogadjaji
    - Trenuci komitovanja slotova

    Primer ispisa:

    [t=  0.21s][Cvor 0] [LIDER] Predlazem slot=0 view=0 cut=[(0,0), (1,0), (2,0), (3,0)]
    [t=  0.30s][Cvor 0] *** KOMITOVAN slot=0 (view=0) - ukupno transakcija: 0 ***

    Na kraju svake simulacije prikazuje se rezime po cvoru:

    Cvor 0: komitovano slotova=19, komitovano transakcija=580, poslednji-commit={0:29, 1:29, 2:29, 3:29}
    Cvor 1: komitovano slotova=20, komitovano transakcija=605, poslednji-commit={0:30, 1:30, 2:30, 3:31}

    Ovi podaci omogucavaju proveru konzistentnosti - u ispravnom radu, svi posteni cvorovi treba da imaju identican poslednji-commit i isti ukupan broj transakcija.

IMPLEMENTACIONA POJEDNOSTAVLJENJA

    - Kriptografija - umesto stvarnih digitalnih potpisa, koriste se skupovi ID-jeva glasaca (set), sto je dovoljno za simulaciju kvoruma.
    - Perzistencija - ne postoji trajno cuvanje stanja na disku; sve se odvija u memoriji tokom simulacije.
    - Batch velicina - fiksirana je na 5 transakcija po car-u (podrazumevano).
