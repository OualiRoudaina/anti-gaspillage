#!/usr/bin/env python3
"""Générateur de ventes simulées : publie dans le topic Kafka `ventes`.

Il lit d'abord les lots dans le topic `lots` (publiés par export_lots.py), puis simule des
ventes en vendant en priorité le lot qui périme le plus tôt (règle FEFO). Il suit lui-même le
stock restant de chaque lot, donc il ne vend jamais plus que ce qui existe.

Message (JSON, clé Kafka = magasin, donc une partition par magasin en pratique) :
    {"vente_id": "V-1a2b3c4d5e6f7a8b", "lot_id": "LOT-2026-0042", "produit_ref": "LAI001",
     "magasin": "M2", "quantite": 2, "horodatage": "2026-10-19T16:50:03Z"}

`vente_id` est unique : c'est ce qui permet à Spark d'ignorer une vente reçue deux fois.
Avec --dup-rate, une partie des ventes est volontairement envoyée deux fois (même vente_id)
pour tester cette déduplication.

Utilisation :
    pip install confluent-kafka
    python generator/generateur_ventes.py --count 300 --rate 10
    python generator/generateur_ventes.py --dry-run --count 5      # affiche, n'envoie rien
"""
import argparse
import json
import os
import random
import sys
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone

TOPIC_LOTS = "lots"
TOPIC_VENTES = "ventes"


def charger_lots(bootstrap, delai_max=30):
    """Lit tout le topic `lots` et garde le dernier état de chaque lot."""
    from confluent_kafka import Consumer, KafkaError

    c = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "generateur-" + uuid.uuid4().hex[:8],  # groupe jetable : on relit tout
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    })
    nb_partitions = len(c.list_topics(TOPIC_LOTS, timeout=10).topics[TOPIC_LOTS].partitions)
    c.subscribe([TOPIC_LOTS])
    lots, termines, debut = {}, set(), time.time()
    while len(termines) < nb_partitions and time.time() - debut < delai_max:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                termines.add(msg.partition())
                continue
            sys.exit(f"Erreur Kafka : {msg.error()}")
        m = json.loads(msg.value())
        lots[m["lot_id"]] = m  # un lot republié remplace l'ancien état
    c.close()
    return list(lots.values())


def multiplicateur(maintenant):
    """Les ventes sont plus fortes le week-end et aux heures de pointe."""
    m = 1.0
    if maintenant.weekday() >= 5:
        m *= 1.4
    if 11 <= maintenant.hour <= 13 or 17 <= maintenant.hour <= 19:
        m *= 1.5
    return m


class Simulateur:
    def __init__(self, lots, seed=None):
        self.rnd = random.Random(seed)  # la graine ne sert qu'aux choix, pas aux vente_id
        self.reste = {l["lot_id"]: l["stock_restant"] for l in lots}
        # (magasin, produit) -> lots triés du plus proche de la péremption au plus lointain
        self.lots_par_cle = defaultdict(list)
        for l in sorted(lots, key=lambda l: l.get("date_limite") or "9999"):
            self.lots_par_cle[(l["magasin"], l["produit_ref"])].append(l)
        refs = {l["produit_ref"] for l in lots}
        magasins = {l["magasin"] for l in lots}
        popularite = {r: self.rnd.uniform(0.3, 3.0) for r in refs}   # certains produits se vendent mieux
        poids_magasin = {m: self.rnd.uniform(0.8, 1.4) for m in magasins}
        self.cles = list(self.lots_par_cle)
        self.poids = [popularite[r] * poids_magasin[m] for m, r in self.cles]

    def prochaine_vente(self, maintenant):
        maintenant_iso = maintenant.strftime("%Y-%m-%dT%H:%M:%SZ")
        for _ in range(30):  # on réessaie si le produit choisi n'a plus de lot vendable
            magasin, ref = self.rnd.choices(self.cles, weights=self.poids)[0]
            lot = next((l for l in self.lots_par_cle[(magasin, ref)]
                        if self.reste[l["lot_id"]] > 0
                        and (not l.get("date_limite") or l["date_limite"] > maintenant_iso)), None)
            if lot is None:
                continue
            quantite = min(self.rnd.choice([1, 1, 1, 2, 2, 3]), self.reste[lot["lot_id"]])
            self.reste[lot["lot_id"]] -= quantite
            return {
                "vente_id": "V-" + uuid.uuid4().hex[:16],
                "lot_id": lot["lot_id"],
                "produit_ref": ref,
                "magasin": magasin,
                "quantite": quantite,
                "horodatage": maintenant_iso,
            }
        return None


def executer(sim, emettre, count, rate, duree=None, dup_rate=0.0, dormir=time.sleep):
    stats = Counter()
    debut = time.time()
    while stats["ventes"] < count and (not duree or time.time() - debut < duree):
        maintenant = datetime.now(timezone.utc)
        vente = sim.prochaine_vente(maintenant)
        if vente is None:
            print("Plus aucun lot vendable : arrêt.")
            break
        emettre(vente)
        stats["ventes"] += 1
        stats["magasin " + vente["magasin"]] += 1
        if sim.rnd.random() < dup_rate:  # même vente_id, envoyée une 2e fois
            emettre(vente)
            stats["doublons"] += 1
        dormir(sim.rnd.expovariate(rate * multiplicateur(maintenant)))
    return stats


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--count", type=int, default=300, help="nombre de ventes à générer (défaut 300)")
    ap.add_argument("--rate", type=float, default=5.0, help="ventes par seconde en moyenne (défaut 5)")
    ap.add_argument("--duration", type=float, help="durée maximale en secondes")
    ap.add_argument("--dup-rate", type=float, default=0.0, help="part de ventes envoyées en double, ex. 0.05")
    ap.add_argument("--seed", type=int, help="graine pour des choix reproductibles")
    ap.add_argument("--dry-run", action="store_true", help="affiche les ventes sans les envoyer")
    args = ap.parse_args()

    try:
        from confluent_kafka import Producer
    except ImportError:
        sys.exit("Module manquant : pip install confluent-kafka")

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP") or "localhost:29094"
    lots = charger_lots(bootstrap)
    if not lots:
        sys.exit("Aucun lot dans le topic 'lots' : lance d'abord scripts/export_lots.py")
    print(f"{len(lots)} lots chargés depuis Kafka")
    sim = Simulateur(lots, args.seed)

    if args.dry_run:
        stats = executer(sim, lambda v: print(json.dumps(v, ensure_ascii=False)), args.count,
                         args.rate, args.duration, args.dup_rate, dormir=lambda s: None)
    else:
        producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})

        def emettre(v):
            producer.produce(TOPIC_VENTES, key=v["magasin"], value=json.dumps(v, ensure_ascii=False))
            producer.poll(0)

        stats = executer(sim, emettre, args.count, args.rate, args.duration, args.dup_rate)
        if producer.flush(30):
            sys.exit("Certains messages n'ont pas pu être envoyés")
    envoyes = stats["ventes"] + stats["doublons"]
    print(f"{stats['ventes']} ventes ({stats['doublons']} doublons volontaires) = {envoyes} messages "
          f"{'générés' if args.dry_run else 'publiés'} dans '{TOPIC_VENTES}'")
    for cle in sorted(k for k in stats if k.startswith("magasin")):
        print(f"  {cle} : {stats[cle]}")


if __name__ == "__main__":
    main()