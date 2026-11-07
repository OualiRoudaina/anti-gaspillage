#!/usr/bin/env python3
"""Synchronise le risque d'invendu calculé par Spark (Redis) vers les lots Odoo.

Lit les hashes Redis `risque:{lot_id}` et écrit dans les champs `waste_*` de `stock.lot`
(module Odoo `lot_waste_risk`). Les lots sont mis à jour par groupes ayant les mêmes
valeurs, pour limiter le nombre d'appels XML-RPC. Le script est rejouable.

Utilisation :
    pip install redis
    python scripts/sync_risque_odoo.py             # écrit dans Odoo
    python scripts/sync_risque_odoo.py --dry-run   # affiche ce qui serait écrit
"""
import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

from seed_odoo import Odoo, load_config

TAILLE_LOT = 200


def convertir_date(texte):
    """'2026-11-07T10:30:00+00:00' ou '...Z' -> '2026-11-07 10:30:00' (UTC, format Odoo)."""
    try:
        d = datetime.fromisoformat(texte.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        return False
    if d.tzinfo:
        d = d.astimezone(timezone.utc)
    return d.strftime("%Y-%m-%d %H:%M:%S")


def construire_valeurs(risque):
    """Convertit un hash Redis `risque:*` en valeurs d'écriture Odoo (fonction pure)."""
    niveau = risque.get("niveau_risque")
    if niveau not in ("faible", "a_surveiller", "eleve"):
        return None
    try:
        return {
            "waste_risk_level": niveau,
            "waste_risk_rate": float(risque.get("taux_invendus", 0)),
            "waste_projected_unsold": float(risque.get("invendus_projetes", 0)),
            "waste_sales_speed": float(risque.get("vitesse_vente", 0)),
            "waste_days_left": float(risque.get("jours_restants", 0)),
            "waste_discount": int(float(risque.get("remise_recommandee", 0))),
            "waste_risk_updated": convertir_date(risque.get("date_calcul", "")),
        }
    except (TypeError, ValueError):
        return None


def lire_redis(client):
    """Retourne {lot_id: valeurs Odoo} pour tous les `risque:*`."""
    resultat, ignores = {}, 0
    for cle in client.scan_iter(match="risque:*", count=500):
        valeurs = construire_valeurs(client.hgetall(cle))
        if valeurs is None:
            ignores += 1
        else:
            resultat[cle.split(":", 1)[1]] = valeurs
    return resultat, ignores


def synchroniser(odoo, risques):
    """Écrit les valeurs dans Odoo. Retourne (lots mis à jour, lots introuvables)."""
    noms = list(risques)
    ids = {}
    for i in range(0, len(noms), TAILLE_LOT):
        lot = noms[i:i + TAILLE_LOT]
        for rec in odoo.call("stock.lot", "search_read", [("name", "in", lot)], fields=["name"]):
            ids[rec["name"]] = rec["id"]

    groupes = defaultdict(list)  # valeurs identiques -> ids, pour un seul write
    for nom, valeurs in risques.items():
        if nom in ids:
            groupes[tuple(sorted(valeurs.items()))].append(ids[nom])
    for valeurs, liste in groupes.items():
        for i in range(0, len(liste), TAILLE_LOT):
            odoo.call("stock.lot", "write", liste[i:i + TAILLE_LOT], dict(valeurs))

    introuvables = [n for n in noms if n not in ids]
    return len(ids), introuvables


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="affiche sans écrire dans Odoo")
    args = ap.parse_args()

    try:
        import redis
    except ImportError:
        sys.exit("Module manquant : pip install redis")

    cfg = load_config()
    client = redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT") or cfg.get("REDIS_PORT", 6379)),
        password=os.environ.get("REDIS_PASSWORD") or cfg.get("REDIS_PASSWORD") or None,
        decode_responses=True,
    )
    risques, ignores = lire_redis(client)
    print(f"{len(risques)} lots avec un risque dans Redis ({ignores} ignorés car invalides)")
    if not risques:
        sys.exit("Aucun hash risque:* trouvé : les jobs Spark ont-ils tourné ?")

    if args.dry_run:
        for nom, v in list(risques.items())[:3]:
            print(nom, v)
        print("(dry-run : rien n'a été écrit dans Odoo)")
        return

    for cle in ("ODOO_LOGIN", "ODOO_PASSWORD"):
        if not cfg.get(cle):
            sys.exit(f"{cle} manquant : ajoute-le dans .env")
    odoo = Odoo(cfg["ODOO_URL"], cfg["ODOO_DB"], cfg["ODOO_LOGIN"], cfg["ODOO_PASSWORD"])
    mis_a_jour, introuvables = synchroniser(odoo, risques)
    print(f"{mis_a_jour} lots mis à jour dans Odoo")
    if introuvables:
        print(f"Attention : {len(introuvables)} lots absents d'Odoo (ex. {introuvables[:3]}). "
              "Le module lot_waste_risk est-il installé et le seed exécuté ?")


if __name__ == "__main__":
    main()
