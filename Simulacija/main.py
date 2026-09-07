"""
main.py
Ulazna tacka simulacije - interaktivni meni u konzoli.
Ovde korisnik bira broj cvorova, trajanje simulacije, scenario greske i
tajming te greske, nakon cega se pokrece simulacija.
"""
import asyncio
import logging
import time
from network import Network
from node import Node

SCENARIOS = {
    "1": "none",
    "2": "leader_crash",
    "3": "byzantine_leader",
    "4": "partition",
    "5": "blip",
}

SCENARIO_OPIS = {
    "none": "Bez greske - normalan rad",
    "leader_crash": "Pad lidera (crash cvora 0)",
    "byzantine_leader": "Vizantijski/neodgovarajuci lider (cvor 0 cuti)",
    "partition": "Mrezna particija (cvorovi podeljeni u dve grupe)",
    "blip": "Blip - naglo povecanje kasnjenja poruka na mrezi",
}


def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    return logging.getLogger("autobahn-sim")


def unesi_broj(poruka: str, default, tip=float):
    """Trazi unos broja od korisnika; ako se ostavi prazno, koristi default."""
    unos = input(f"{poruka} [default={default}]: ").strip()
    if unos == "":
        return default
    try:
        return tip(unos)
    except ValueError:
        print("Neispravan unos, koristim default vrednost.")
        return default


def prikazi_meni_scenarija():
    print("\nDostupni scenariji greske:")
    for kljuc, naziv in SCENARIOS.items():
        print(f"  {kljuc}. {naziv:<18} - {SCENARIO_OPIS[naziv]}")


def podesi_parametre():
    print("=" * 60)
    print(" AUTOBAHN BFT KONSENZUS - PODESAVANJE SIMULACIJE")
    print("=" * 60)

    n = int(unesi_broj("Broj cvorova (n = 3f+1, npr. 4, 7, 10)", 4, int))
    duration = unesi_broj("Ukupno trajanje simulacije (u sekundama)", 20.0, float)

    prikazi_meni_scenarija()
    izbor = input(f"Izaberi scenario [1-{len(SCENARIOS)}, default=1]: ").strip()
    scenario = SCENARIOS.get(izbor, "none")

    fault_start = 0.0
    fault_duration = 0.0
    blip_delay = 1.0

    if scenario != "none":
        print(f"\nPodesavanje tajminga za scenario '{scenario}':")
        fault_start = unesi_broj("  Posle koliko sekundi greska pocinje", 4.0, float)
        fault_duration = unesi_broj("  Koliko sekundi greska traje", 6.0, float)
        if scenario == "blip":
            blip_delay = unesi_broj("  Dodatno kasnjenje poruka tokom blip-a (u sekundama)", 1.0, float)

    print("\n--- Rezime podesavanja ---")
    print(f"  Broj cvorova     : {n}")
    print(f"  Trajanje sim.    : {duration}s")
    print(f"  Scenario         : {scenario}")
    if scenario != "none":
        print(f"  Greska pocinje   : t={fault_start}s")
        print(f"  Greska traje     : {fault_duration}s")
        if scenario == "blip":
            print(f"  Dodatno kasnjenje: {blip_delay}s")
    print("--------------------------\n")

    return {
        "n": n,
        "duration": duration,
        "scenario": scenario,
        "fault_start": fault_start,
        "fault_duration": fault_duration,
        "blip_delay": blip_delay,
    }


async def run_scenario(params: dict):
    logger = setup_logging()
    start = time.monotonic()
    n = params["n"]
    net = Network(n, base_delay=0.03, jitter=0.03, loss_rate=0.0, log=logger.info)
    nodes = [Node(i, n, net, start, logger) for i in range(n)]

    tasks = [asyncio.ensure_future(node.run()) for node in nodes]
    fault_task = asyncio.ensure_future(inject_faults(params, net, n, logger))

    await asyncio.sleep(params["duration"])

    for node in nodes:
        node.stop()
    fault_task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    print_summary(nodes, logger)


async def inject_faults(params: dict, net: Network, n: int, logger):
    log = logger.info
    scenario = params["scenario"]
    if scenario == "none":
        return

    await asyncio.sleep(params["fault_start"])

    if scenario == "leader_crash":
        log("=== SCENARIO: pad lidera (crash cvora 0) ===")
        net.crash(0)
        await asyncio.sleep(params["fault_duration"])
        net.recover(0)
        log("=== Cvor 0 se oporavio ===")

    elif scenario == "byzantine_leader":
        log("=== SCENARIO: vizantijski/neodgovarajuci lider (cvor 0 cuti) ===")
        net.byzantine_silent.add(0)
        await asyncio.sleep(params["fault_duration"])
        net.byzantine_silent.discard(0)
        log("=== Cvor 0 je ponovo aktivan ===")

    elif scenario == "partition":
        log("=== SCENARIO: mrezna particija ===")
        half = n // 2
        net.set_partition(set(range(half)), set(range(half, n)))
        await asyncio.sleep(params["fault_duration"])
        net.heal_partition()
        log("=== Particija zalecena ===")

    elif scenario == "blip":
        log(f"=== SCENARIO: 'blip' - dodatno kasnjenje {params['blip_delay']}s ===")
        net.set_blip(params["blip_delay"])
        await asyncio.sleep(params["fault_duration"])
        net.set_blip(0.0)
        log("=== Blip zavrsen, kasnjenje vraceno na normalu ===")


def print_summary(nodes, logger):
    logger.info("\n================ REZULTATI SIMULACIJE ================")
    for node in nodes:
        total_tx = sum(x[3] for x in node.log_out)
        logger.info(f"Cvor {node.id}: komitovano slotova={len(node.committed_slots)}, "
                     f"komitovano transakcija={total_tx}, poslednji-commit={node.last_commit}")
    logger.info("========================================================")


def main():
    while True:
        params = podesi_parametre()
        potvrda = input("Pokreni simulaciju sa ovim podesavanjima? (d/n): ").strip().lower()
        if potvrda in ("d", "da", "y", "yes", ""):
            break
        print("Podesavanje otkazano, krecemo ispocetka.\n")

    asyncio.run(run_scenario(params))

    ponovo = input("\nZelis li da pokrenes jos jednu simulaciju? (d/n): ").strip().lower()
    if ponovo in ("d", "da", "y", "yes"):
        main()


if __name__ == "__main__":
    main()