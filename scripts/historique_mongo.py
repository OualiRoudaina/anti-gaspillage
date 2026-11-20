#!/usr/bin/env python3
"""Archive dans MongoDB l'historique du risque d'invendu et des vitesses de vente.

Redis ne garde que l'état courant de chaque lot. Ce script en prend une photo et la
conserve dans MongoDB, ce qui permet de suivre l'évolution du risque dans le temps.

Collections (base `antigaspi`) :
  - historique_risque             un document par lot et par calcul (clé : lot_id + date_calcul)
  - vitesses_lot                  vitesse de vente moyenne de chaque lot (dernière valeur)
  - vitesses_produit_magasin      vitesse de vente moyenne par produit et par magasin

Le script peut être relancé : un calcul déjà archivé n'est pas enregistré deux fois.

Utilisation :
    pip install redis pymongo
    python scripts/historique_mongo.py             # archive l'état actuel
    python scripts/historique_mongo.py --dry-run   # affiche sans écrire
    python scripts/historique_mongo.py --stats     # résume l'historique déjà archivé
"""
import argparse
import os
import sys
from datetime import datetime, timezone
from urllib.parse import quote_plus

from seed_odoo import load_config

BASE = "antigaspi"


def lire_date(texte):
    """Convertit une date ISO (avec ou sans fuseau) en datetime UTC, ou None."""
    try:
        d = datetime.fromisoformat(str(texte).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)


def _nombre(valeur, defaut=0.0):
    try:
        return float(valeur)
    except (TypeError, ValueError):
        return defaut


def construire_historique(lot_id, risque, maintenant):
    """Transforme un hash Redis `risque:{lot_id}` en document MongoDB (fonction pure)."""
    date_calcul = lire_date(risque.get("date_calcul"))
    niveau = risque.get("niveau_risque")
    if date_calcul is None or niveau not in ("faible", "a_surveiller", "eleve"):
        return None
    return {
        "lot_id": lot_id,
        "produit": risque.get("produit", ""),
        "magasin": risque.get("magasin", ""),
        "niveau_risque": niveau,
        "taux_invendus": _nombre(risque.get("taux_invendus")),
        "invendus_projetes": _nombre(risque.get("invendus_projetes")),
        "stock_restant": _nombre(risque.get("stock_restant")),
        "jours_restants": _nombre(risque.get("jours_restants")),
        "vitesse_vente": _nombre(risque.get("vitesse_vente")),
        "remise_recommandee": int(_nombre(risque.get("remise_recommandee"))),
        "date_calcul": date_calcul,
        "enregistre_le": maintenant,
    }


def construire_vitesse(donnees, maintenant):
    """Transforme un hash Redis `vitesse:*` ou `vitesse_pm:*` en document (fonction pure)."""
    if _nombre(donnees.get("vitesse_moyenne_jour")) <= 0:
        return None
    return {
        "vitesse_moyenne_jour": _nombre(donnees.get("vitesse_moyenne_jour")),
        "quantite_vendue": int(_nombre(donnees.get("quantite_vendue"))),
        "jours_actifs": int(_nombre(donnees.get("jours_actifs"))),
        "date_calcul": lire_date(donnees.get("date_calcul")) or maintenant,
        "enregistre_le": maintenant,
    }


def lire_redis(client, maintenant):
    """Retourne (historique, vitesses de lot, vitesses par produit et magasin)."""
    historique, par_lot, par_produit = [], [], []

    for cle in client.scan_iter(match="risque:*", count=500):
        doc = construire_historique(cle.split(":", 1)[1], client.hgetall(cle), maintenant)
        if doc:
            historique.append(doc)

    for cle in client.scan_iter(match="vitesse:*", count=500):
        doc = construire_vitesse(client.hgetall(cle), maintenant)
        if doc:
            doc["lot_id"] = cle.split(":", 1)[1]
            doc["produit_ref"] = client.hget(cle, "produit_ref") or ""
            doc["magasin"] = client.hget(cle, "magasin") or ""
            par_lot.append(doc)

    for cle in client.scan_iter(match="vitesse_pm:*", count=500):
        _, magasin, produit_ref = cle.split(":", 2)
        doc = construire_vitesse(client.hgetall(cle), maintenant)
        if doc:
            doc["magasin"], doc["produit_ref"] = magasin, produit_ref
            par_produit.append(doc)

    return historique, par_lot, par_produit


def connexion_mongo(cfg):
    from pymongo import MongoClient

    utilisateur = os.environ.get("MONGO_USER") or cfg.get("MONGO_INITDB_ROOT_USERNAME", "")
    mot_de_passe = os.environ.get("MONGO_PASSWORD") or cfg.get("MONGO_INITDB_ROOT_PASSWORD", "")
    port = os.environ.get("MONGO_PORT") or cfg.get("MONGO_PORT", "27017")
    hote = os.environ.get("MONGO_HOST", "localhost")
    if utilisateur:
        uri = f"mongodb://{quote_plus(utilisateur)}:{quote_plus(mot_de_passe)}@{hote}:{port}/?authSource=admin"
    else:
        uri = f"mongodb://{hote}:{port}/"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client[BASE]


def ecrire_mongo(db, historique, par_lot, par_produit):
    """Écrit les documents. Retourne le nombre de nouveaux calculs archivés."""
    from pymongo import ASCENDING, DESCENDING, ReplaceOne
    from pymongo.errors import BulkWriteError

    db.historique_risque.create_index(
        [("lot_id", ASCENDING), ("date_calcul", DESCENDING)], unique=True
    )
    db.historique_risque.create_index([("magasin", ASCENDING), ("niveau_risque", ASCENDING)])

    nouveaux = 0
    if historique:
        try:
            nouveaux = len(db.historique_risque.insert_many(historique, ordered=False).inserted_ids)
        except BulkWriteError as e:
            erreurs = e.details.get("writeErrors", [])
            if any(err.get("code") != 11000 for err in erreurs):  # 11000 = déjà archivé
                raise
            nouveaux = e.details.get("nInserted", 0)

    if par_lot:
        db.vitesses_lot.bulk_write(
            [ReplaceOne({"lot_id": d["lot_id"]}, d, upsert=True) for d in par_lot]
        )
    if par_produit:
        db.vitesses_produit_magasin.bulk_write([
            ReplaceOne({"magasin": d["magasin"], "produit_ref": d["produit_ref"]}, d, upsert=True)
            for d in par_produit
        ])
    return nouveaux


def afficher_stats(db):
    total = db.historique_risque.count_documents({})
    print(f"{total} calculs archivés dans historique_risque")
    for ligne in db.historique_risque.aggregate([
        {"$group": {"_id": "$niveau_risque", "n": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]):
        print(f"  {ligne['_id']:<14} {ligne['n']}")
    print(f"{db.vitesses_lot.count_documents({})} vitesses de lot, "
          f"{db.vitesses_produit_magasin.count_documents({})} vitesses par produit et magasin")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="affiche sans écrire dans MongoDB")
    ap.add_argument("--stats", action="store_true", help="résume l'historique déjà archivé")
    args = ap.parse_args()

    try:
        import redis
        import pymongo  # noqa: F401
    except ImportError:
        sys.exit("Modules manquants : pip install redis pymongo")

    cfg = load_config()

    if args.stats:
        afficher_stats(connexion_mongo(cfg))
        return

    r = redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT") or cfg.get("REDIS_PORT", 6379)),
        password=os.environ.get("REDIS_PASSWORD") or cfg.get("REDIS_PASSWORD") or None,
        decode_responses=True,
    )
    maintenant = datetime.now(timezone.utc)
    historique, par_lot, par_produit = lire_redis(r, maintenant)
    print(f"Redis : {len(historique)} calculs de risque, {len(par_lot)} vitesses de lot, "
          f"{len(par_produit)} vitesses par produit et magasin")
    if not historique:
        sys.exit("Aucun hash risque:* trouvé : les jobs Spark ont-ils tourné ?")

    if args.dry_run:
        print(historique[0])
        print("(dry-run : rien n'a été écrit dans MongoDB)")
        return

    nouveaux = ecrire_mongo(connexion_mongo(cfg), historique, par_lot, par_produit)
    print(f"{nouveaux} nouveaux calculs archivés dans MongoDB "
          f"({len(historique) - nouveaux} déjà présents)")


if __name__ == "__main__":
    main()
