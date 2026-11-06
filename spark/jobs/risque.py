
#!/usr/bin/env python3
"""
Calcul du risque d'invendus par lot.

Sources :
- Redis : stock_restant, date_limite, produit, magasin
- Redis : vitesse_moyenne_jour calculée par vitesses_vente.py
- VITESSE_DEFAUT : vitesse de secours si aucune vitesse n'est disponible

Résultats :
- risque:{lot_id} : résultats du calcul
- alertes:risque : ensemble des lots nécessitant une attention
"""

import os
import argparse
from datetime import datetime, timezone

import redis


# Configuration Redis
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD") or None

# Paramètres métier configurables
SEUIL_SURVEILLER = float(os.environ.get("SEUIL_SURVEILLER", 0.20))
SEUIL_ELEVE = float(os.environ.get("SEUIL_ELEVE", 0.50))
VITESSE_DEFAUT = float(os.environ.get("VITESSE_DEFAUT", 5))

CLE_ALERTES = "alertes:risque"


def connexion_redis():
    """Établit la connexion à Redis."""
    client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True
    )
    client.ping()
    return client


def jours_restants(date_limite, maintenant):
    """Calcule le nombre de jours avant expiration."""
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
    """
    Récupère la vitesse moyenne de vente calculée par le batch.
    Utilise la vitesse par défaut si aucune valeur valide n'existe.
    """
    donnees = client.hgetall(f"vitesse:{lot_id}")

    try:
        vitesse = float(donnees.get("vitesse_moyenne_jour", 0))

        if vitesse > 0:
            return vitesse

    except (TypeError, ValueError):
        pass

    return VITESSE_DEFAUT


def calculer_risque(stock, jours, vitesse):
    """
    Calcule les invendus projetés, le taux de risque,
    le niveau de risque et la remise recommandée.
    """

    invendus = max(0.0, stock - vitesse * jours)

    taux = invendus / stock if stock > 0 else 0.0

    if taux >= SEUIL_ELEVE:
        niveau = "eleve"

        if jours <= 1:
            remise = 50
        elif jours <= 3:
            remise = 30
        else:
            remise = 20

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
    """Analyse un lot avec sa vitesse réelle de vente."""

    stock = float(lot.get("stock_restant", 0))

    jours = jours_restants(
        lot.get("date_limite", ""),
        maintenant
    )

    if jours is None:
        return None

    resultat = calculer_risque(
        stock=stock,
        jours=jours,
        vitesse=vitesse
    )

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


def traiter_lot(client, lot_id, maintenant, dry_run=False):
    """Calcule et enregistre le risque d'un lot."""

    cle_lot = f"lot:{lot_id}"
    lot = client.hgetall(cle_lot)

    if not lot:
        print(f"[IGNORÉ] Lot introuvable : {lot_id}")
        return

    # Récupération de la vitesse calculée dans Redis
    vitesse = obtenir_vitesse_vente(client, lot_id)

    resultat = analyser_lot(
        lot,
        maintenant,
        vitesse
    )

    if resultat is None:
        print(f"[IGNORÉ] Date limite invalide : {lot_id}")
        return

    print(
        f"[{resultat['niveau_risque'].upper()}] "
        f"{lot_id} | "
        f"Stock : {resultat['stock_restant']} | "
        f"Vitesse/jour : {resultat['vitesse_vente']} | "
        f"Invendus : {resultat['invendus_projetes']} | "
        f"Taux : {resultat['taux_invendus']}% | "
        f"Remise : {resultat['remise_recommandee']}%"
    )

    if dry_run:
        return

    # Enregistrement des indicateurs du lot
    client.hset(
        f"risque:{lot_id}",
        mapping={
            cle: str(valeur)
            for cle, valeur in resultat.items()
        }
    )

    # Mise à jour de l'ensemble des alertes
    if resultat["niveau_risque"] in ("a_surveiller", "eleve"):
        client.sadd(CLE_ALERTES, lot_id)
    else:
        client.srem(CLE_ALERTES, lot_id)


def main():
    parser = argparse.ArgumentParser(
        description="Calcul du risque d'invendus par lot"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Afficher les résultats sans les enregistrer dans Redis"
    )

    parser.add_argument(
        "--lot",
        help="Analyser un seul lot, par exemple LOT-2026-0001"
    )

    args = parser.parse_args()

    client = connexion_redis()
    maintenant = datetime.now(timezone.utc)

    print("=== ANALYSE DES RISQUES D'INVENDUS ===")
    print("Source : vitesses de vente calculées dans Redis")
    print(f"Vitesse de secours : {VITESSE_DEFAUT} unités/jour")
    print(f"Mode simulation : {args.dry_run}")
    print("--------------------------------------")

    if args.lot:
        traiter_lot(
            client,
            args.lot,
            maintenant,
            args.dry_run
        )
        nombre = 1

    else:
        nombre = 0

        for cle in client.scan_iter(match="lot:*", count=200):
            lot_id = cle.split(":", 1)[1]

            traiter_lot(
                client,
                lot_id,
                maintenant,
                args.dry_run
            )

            nombre += 1

    print("--------------------------------------")
    print(f"Nombre de lots analysés : {nombre}")
    print("=== ANALYSE TERMINÉE ===")


if __name__ == "__main__":
    main()
