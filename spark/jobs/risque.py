"""Calcul du risque d'invendu d'un lot (règle du cahier des charges, section 5).

S = stock restant, D = jours restants, v = vitesse de vente (unités/jour)
Invendus projetés = max(0, S - v*D)      Taux d'invendus = invendus projetés / S
  Faible       : taux < 20 %      -> aucune action
  À surveiller : 20 % <= taux < 50 %  -> alerte au responsable
  Élevé        : taux >= 50 %     -> remise de 20 %, 30 % ou 50 % selon les jours restants
Les seuils et paliers sont des valeurs de départ, modifiables ici ou par variables d'environnement.
"""
import os
from datetime import datetime, timezone

SEUIL_SURVEILLER = float(os.environ.get("SEUIL_SURVEILLER", 0.20))
SEUIL_ELEVE = float(os.environ.get("SEUIL_ELEVE", 0.50))
# Vitesse utilisée tant que le batch n'a pas calculé la vraie (unités par jour)
VITESSE_DEFAUT = float(os.environ.get("VITESSE_DEFAUT", 5))


def jours_restants(date_limite, maintenant):
    limite = datetime.strptime(date_limite, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return max(0.0, (limite - maintenant).total_seconds() / 86400)


def calculer_risque(stock, jours, vitesse):
    invendus = max(0.0, stock - vitesse * jours)
    taux = invendus / stock if stock > 0 else 0.0
    if taux >= SEUIL_ELEVE:
        niveau = "eleve"
    elif taux >= SEUIL_SURVEILLER:
        niveau = "a_surveiller"
    else:
        niveau = "faible"
    remise = 0
    if niveau == "eleve":
        remise = 50 if jours <= 1 else 30 if jours <= 3 else 20
    return {"invendus_projetes": round(invendus, 1), "taux_invendus": round(taux, 3),
            "niveau": niveau, "remise": remise}


def recalculer_risque(r, lot_ids, maintenant=None):
    """Recalcule le risque des lots donnés et met à jour Redis (hash du lot + alertes:{magasin})."""
    maintenant = maintenant or datetime.now(timezone.utc)
    lot_ids = sorted(set(lot_ids))
    if not lot_ids:
        return 0
    p = r.pipeline(transaction=False)
    for lid in lot_ids:
        p.hmget(f"lot:{lid}", "stock_restant", "date_limite", "magasin", "produit_ref")
    lots = p.execute()

    jour = maintenant.weekday()  # 0 = lundi ... 6 = dimanche
    p = r.pipeline(transaction=False)
    for _, _, magasin, ref in lots:
        p.get(f"vitesse:{magasin}:{ref}:{jour}")
    vitesses = p.execute()

    horodatage = maintenant.strftime("%Y-%m-%dT%H:%M:%SZ")
    p = r.pipeline(transaction=False)
    for lid, (stock, limite, magasin, ref), v in zip(lot_ids, lots, vitesses):
        if stock is None or not limite:
            continue
        vitesse = float(v) if v is not None else VITESSE_DEFAUT
        jours = jours_restants(limite, maintenant)
        res = calculer_risque(int(stock), jours, vitesse)
        p.hset(f"lot:{lid}", mapping={
            "jours_restants": round(jours, 2), "vitesse": vitesse,
            "vitesse_source": "batch" if v is not None else "defaut",
            "invendus_projetes": res["invendus_projetes"], "taux_invendus": res["taux_invendus"],
            "niveau": res["niveau"], "remise": res["remise"], "risque_maj": horodatage,
        })
        if res["niveau"] == "faible":
            p.zrem(f"alertes:{magasin}", lid)
        else:
            p.zadd(f"alertes:{magasin}", {lid: res["taux_invendus"]})
    p.execute()
    return len(lot_ids)