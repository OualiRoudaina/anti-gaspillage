#!/usr/bin/env python3
"""Exporte les lots d'Odoo vers le topic Kafka `lots`.

Un message par lot (clé = numéro de lot), au format JSON :
    {"lot_id": "LOT-2026-0042", "produit_ref": "LAI001", "produit": "Yaourt nature x4",
     "magasin": "M2", "stock_restant": 120, "date_limite": "2026-10-25T12:00:00Z",
     "exporte_le": "2026-10-18T16:30:00Z"}

Ces champs correspondent à ceux du hash Redis `lot:{lot_id}` du cahier des charges.
Le script peut être relancé : republier un lot remplace simplement son état.

Utilisation :
    pip install confluent-kafka
    python scripts/export_lots.py             # exporte vers Kafka
    python scripts/export_lots.py --dry-run   # affiche les messages sans toucher à Kafka
"""
import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

from seed_odoo import Odoo, load_config  # réutilise la connexion Odoo du script de seed

TOPIC = "lots"


def build_messages(quants, lots, produits, magasins):
    """Transforme les données Odoo en messages Kafka (fonction pure, facile à tester).

    quants   : lignes de stock.quant  {lot_id: [id, nom], product_id: [...], warehouse_id: [id, nom], quantity}
    lots     : {id du lot: {name, expiration_date}}
    produits : {id du produit: {name, default_code}}
    magasins : {id de l'entrepôt: code}
    """
    # Un lot peut avoir plusieurs lignes de stock : on additionne par (lot, magasin)
    stock = defaultdict(float)
    for q in quants:
        if not q["lot_id"] or not q["warehouse_id"]:
            continue
        stock[(q["lot_id"][0], q["product_id"][0], q["warehouse_id"][0])] += q["quantity"]

    maintenant = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    messages = []
    for (lot_id, produit_id, wh_id), quantite in sorted(stock.items()):
        lot, produit = lots[lot_id], produits[produit_id]
        date_limite = lot.get("expiration_date")
        if date_limite:  # Odoo renvoie "2026-10-25 12:00:00" (UTC)
            date_limite = date_limite.replace(" ", "T") + "Z"
        messages.append({
            "lot_id": lot["name"],
            "produit_ref": produit.get("default_code") or "",
            "produit": produit["name"],
            "magasin": magasins[wh_id],
            "stock_restant": int(quantite),
            "date_limite": date_limite or None,
            "exporte_le": maintenant,
        })
    return messages


def lire_odoo(o):
    quants = o.call("stock.quant", "search_read",
                    [("lot_id", "!=", False), ("location_id.usage", "=", "internal"), ("quantity", ">", 0)],
                    fields=["lot_id", "product_id", "warehouse_id", "quantity"])
    lot_ids = list({q["lot_id"][0] for q in quants})
    prod_ids = list({q["product_id"][0] for q in quants})
    lots = {l["id"]: l for l in o.call("stock.lot", "read", lot_ids, fields=["name", "expiration_date"])}
    produits = {p["id"]: p for p in o.call("product.product", "read", prod_ids, fields=["name", "default_code"])}
    magasins = {w["id"]: w["code"] for w in o.call("stock.warehouse", "search_read", [], fields=["code"])}
    return quants, lots, produits, magasins


def publier(messages, bootstrap):
    try:
        from confluent_kafka import Producer
    except ImportError:
        sys.exit("Module manquant : pip install confluent-kafka")

    erreurs = []
    envoyes = 0

    def rapport(err, msg):
        nonlocal envoyes
        if err:
            erreurs.append(str(err))
        else:
            envoyes += 1

    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})
    for m in messages:
        producer.produce(TOPIC, key=m["lot_id"], value=json.dumps(m, ensure_ascii=False), on_delivery=rapport)
        producer.poll(0)
    reste = producer.flush(30)
    if erreurs or reste:
        sys.exit(f"Échec : {len(erreurs)} erreurs, {reste} messages non envoyés. Premier problème : {erreurs[:1]}")
    print(f"{envoyes} messages publiés dans le topic '{TOPIC}' ({bootstrap})")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="affiche les messages sans les envoyer à Kafka")
    args = ap.parse_args()

    cfg = load_config()
    for cle in ("ODOO_LOGIN", "ODOO_PASSWORD"):
        if not cfg.get(cle):
            sys.exit(f"{cle} manquant : ajoute-le dans .env")
    o = Odoo(cfg["ODOO_URL"], cfg["ODOO_DB"], cfg["ODOO_LOGIN"], cfg["ODOO_PASSWORD"])

    messages = build_messages(*lire_odoo(o))
    if not messages:
        sys.exit("Aucun lot en stock trouvé dans Odoo : le script seed_odoo.py a-t-il été exécuté ?")
    print(f"{len(messages)} lots lus dans Odoo")

    if args.dry_run:
        for m in messages[:3]:
            print(json.dumps(m, ensure_ascii=False))
        print("(dry-run : rien n'a été envoyé)")
        return

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP") or f"localhost:{cfg.get('KAFKA_EXTERNAL_PORT', '29092')}"
    publier(messages, bootstrap)


if __name__ == "__main__":
    main()