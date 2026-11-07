#!/usr/bin/env python3
"""Calcul du risque d'invendus par lot à partir des vitesses Redis."""

import os
import argparse
from datetime import datetime, timezone

import redis

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD") or None

SEUIL_SURVEILLER = float(os.environ.get("SEUIL_SURVEILLER", 0.20))
SEUIL_ELEVE = float(os.environ.get("SEUIL_ELEVE", 0.50))
VITESSE_DEFAUT = float(os.environ.get("VITESSE_DEFAUT", 5))

CLE_ALERTES = "alertes:risque"


def connexion_redis():
    client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True
    )
    client.ping()
    return client


def jours_restants(date_limite, maintenant):
    if not date_limite:
        return None

    try:
        limite = datetime.fromisoformat(
            date_limite.replace("Z", "+00:00")
        )

        if limite.tzinfo is None:
            limite = limite.replace(tzinfo=timezone.utc)

        return max(
            0.0,
            (limite - maintenant).total_seconds() / 86400
        )
    except (ValueError, TypeError):
        return None


def obtenir_vitesse_vente(client, lot_id):
    donnees = client.hgetall(f"vitesse:{lot_id}")

    try:
        vitesse = float(donnees.get("vitesse_moyenne_jour", 0))
        if vitesse > 0:
            return vitesse
    except (TypeError, ValueError):
        pass

    return VITESSE_DEFAUT


def calculer_risque(stock, jours, vitesse):
    invendus = max(0.0, stock - vitesse * jours)
    taux = invendus / stock if stock > 0 else 0.0

    if taux >= SEUIL_ELEVE:
        niveau = "eleve"
        remise = 50 if jours <= 1 else 30 if jours <= 3 else 20
    elif taux >= SEUIL_SURVEILLER:
        niveau = "a_surveiller"
        remise = 0
    else:
        niveau = "faible"
        remise = 0

    return {
        "invendus_projetes": round(invendus, 2),
        "taux_invendus": round(taux * 100, 2),
        "niveau_risque": niveau,
        "remise_recommandee": remise
    }


def analyser_lot(lot, maintenant, vitesse):
    try:
        stock = float(lot.get("stock_restant", 0))
    except (TypeError, ValueError):
        return None

    jours = jours_restants(lot.get("date_limite", ""), maintenant)

    if jours is None:
        return None

    resultat = calculer_risque(stock, jours, vitesse)

    resultat.update({
        "stock_restant": stock,
        "jours_restants": round(jours, 2),
        "vitesse_vente": round(vitesse, 2),
        "produit": lot.get("produit", ""),
        "magasin": lot.get("magasin", ""),
        "date_limite": lot.get("date_limite", ""),
        "date_calcul": maintenant.isoformat()
    })

    return resultat


def traiter_lot(client, lot_id, maintenant, dry_run=False, afficher=True):
    cle_lot = f"lot:{lot_id}"
    lot = client.hgetall(cle_lot)

    if not lot:
        return False

    vitesse = obtenir_vitesse_vente(client, lot_id)
    resultat = analyser_lot(lot, maintenant, vitesse)

    if resultat is None:
        if afficher:
            print(f"[IGNORÉ] Date limite ou stock invalide : {lot_id}")
        return False

    if afficher:
        print(
            f"[{resultat['niveau_risque'].upper()}] "
            f"{lot_id} | Stock : {resultat['stock_restant']} | "
            f"Vitesse/jour : {resultat['vitesse_vente']} | "
            f"Invendus : {resultat['invendus_projetes']} | "
            f"Taux : {resultat['taux_invendus']}% | "
            f"Remise : {resultat['remise_recommandee']}%"
        )

    if dry_run:
        return True

    # Résultats détaillés : compatibilité avec la branche feature
    client.hset(
        f"risque:{lot_id}",
        mapping={k: str(v) for k, v in resultat.items()}
    )

    # Indicateurs directement accessibles depuis le lot : compatibilité main
    client.hset(
        cle_lot,
        mapping={
            "jours_restants": resultat["jours_restants"],
            "vitesse": resultat["vitesse_vente"],
            "vitesse_source": (
                "batch"
                if client.hexists(f"vitesse:{lot_id}", "vitesse_moyenne_jour")
                else "defaut"
            ),
            "invendus_projetes": resultat["invendus_projetes"],
            "taux_invendus": resultat["taux_invendus"],
            "niveau": resultat["niveau_risque"],
            "remise": resultat["remise_recommandee"],
            "risque_maj": maintenant.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
    )

    # Alertes globales de la branche feature
    if resultat["niveau_risque"] in ("a_surveiller", "eleve"):
        client.sadd(CLE_ALERTES, lot_id)
    else:
        client.srem(CLE_ALERTES, lot_id)

    # Alertes par magasin de la branche main
    magasin = lot.get("magasin", "")
    if magasin:
        cle_alertes_magasin = f"alertes:{magasin}"

        if resultat["niveau_risque"] == "faible":
            client.zrem(cle_alertes_magasin, lot_id)
        else:
            client.zadd(
                cle_alertes_magasin,
                {lot_id: resultat["taux_invendus"]}
            )

    return True


def recalculer_risque(client, lot_ids, maintenant=None):
    """Fonction appelée par le streaming après une vente ou l'initialisation."""
    maintenant = maintenant or datetime.now(timezone.utc)
    nombre = 0

    for lot_id in sorted(set(lot_ids)):
        if traiter_lot(
            client,
            lot_id,
            maintenant,
            dry_run=False,
            afficher=False
        ):
            nombre += 1

    return nombre


def main():
    parser = argparse.ArgumentParser(
        description="Calcul du risque d'invendus par lot"
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--lot", help="Analyser un seul lot")

    args = parser.parse_args()

    client = connexion_redis()
    maintenant = datetime.now(timezone.utc)

    print("=== ANALYSE DES RISQUES D'INVENDUS ===")
    print("Source : vitesses de vente calculées dans Redis")
    print(f"Vitesse de secours : {VITESSE_DEFAUT} unités/jour")
    print(f"Mode simulation : {args.dry_run}")
    print("--------------------------------------")

    if args.lot:
        nombre = int(
            traiter_lot(client, args.lot, maintenant, args.dry_run)
        )
    else:
        nombre = 0

        for cle in client.scan_iter(match="lot:*", count=200):
            lot_id = cle.split(":", 1)[1]
            if traiter_lot(client, lot_id, maintenant, args.dry_run):
                nombre += 1

    print("--------------------------------------")
    print(f"Nombre de lots analysés : {nombre}")
    print("=== ANALYSE TERMINÉE ===")


if __name__ == "__main__":
    main()