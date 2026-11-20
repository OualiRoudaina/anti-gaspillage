"""Tests des fonctions pures du batch de vitesses et de l'historique MongoDB.

Lancer : python -m unittest discover -s tests -v
Redis est simulé : aucun service n'est nécessaire.
"""
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "spark" / "jobs"))
sys.path.insert(0, str(ROOT / "scripts"))

try:
    import redis  # noqa: F401
except ImportError:  # le module n'est pas nécessaire pour ces tests
    sys.modules["redis"] = types.ModuleType("redis")

import historique_mongo as hm  # noqa: E402
import risque  # noqa: E402


class FauxRedis:
    def __init__(self, donnees=None):
        self.h = {k: dict(v) for k, v in (donnees or {}).items()}

    def hgetall(self, cle):
        return dict(self.h.get(cle, {}))

    def hset(self, cle, mapping=None, **kw):
        self.h.setdefault(cle, {}).update({k: str(v) for k, v in (mapping or {}).items()})

    def hexists(self, cle, champ):
        return champ in self.h.get(cle, {})

    def sadd(self, *a):
        pass

    def srem(self, *a):
        pass

    def zadd(self, *a, **k):
        pass

    def zrem(self, *a):
        pass


LOT = {"produit_ref": "LAI001", "magasin": "M1", "produit": "Yaourt",
       "stock_restant": "100", "date_limite": "2030-01-01T00:00:00Z"}


class TestVitesses(unittest.TestCase):
    def test_priorite_lot_puis_produit_puis_defaut(self):
        r = FauxRedis({"vitesse:L1": {"vitesse_moyenne_jour": "7"},
                       "vitesse_pm:M1:LAI001": {"vitesse_moyenne_jour": "3"}})
        self.assertEqual(risque.obtenir_vitesse_detail(r, "L1", LOT), (7.0, "batch"))
        self.assertEqual(risque.obtenir_vitesse_detail(r, "L2", LOT), (3.0, "batch_produit"))
        self.assertEqual(risque.obtenir_vitesse_detail(r, "L2", {"magasin": "M9", "produit_ref": "X"}),
                         (risque.VITESSE_DEFAUT, "defaut"))
        self.assertEqual(risque.obtenir_vitesse_detail(r, "L2"), (risque.VITESSE_DEFAUT, "defaut"))

    def test_vitesse_nulle_ignoree(self):
        r = FauxRedis({"vitesse:L1": {"vitesse_moyenne_jour": "0"}})
        self.assertEqual(risque.obtenir_vitesse_detail(r, "L1", LOT)[1], "defaut")

    def test_ecriture_produit_magasin(self):
        r = FauxRedis()
        lignes = [
            {"produit_ref": "LAI001", "magasin": "M1", "vitesse_moyenne_jour": 4.567,
             "quantite_vendue": 40, "jours_actifs": 9},
            {"produit_ref": None, "magasin": "M1", "vitesse_moyenne_jour": 1,
             "quantite_vendue": 1, "jours_actifs": 1},
        ]
        self.assertEqual(risque.ecrire_vitesses_produit_magasin(r, lignes), 1)
        self.assertEqual(r.h["vitesse_pm:M1:LAI001"]["vitesse_moyenne_jour"], "4.57")

    def test_traiter_lot_utilise_la_vitesse_produit(self):
        r = FauxRedis({"lot:L1": LOT, "vitesse_pm:M1:LAI001": {"vitesse_moyenne_jour": "3"}})
        maintenant = datetime(2029, 12, 30, tzinfo=timezone.utc)  # 2 jours avant la date limite
        self.assertTrue(risque.traiter_lot(r, "L1", maintenant, afficher=False))
        self.assertEqual(r.h["lot:L1"]["vitesse_source"], "batch_produit")
        self.assertEqual(r.h["risque:L1"]["vitesse_vente"], "3.0")
        # 100 - 3 x 2 = 94 invendus projetés, soit 94 % : risque élevé
        self.assertEqual(r.h["lot:L1"]["niveau"], "eleve")


class TestHistorique(unittest.TestCase):
    MAINTENANT = datetime(2026, 11, 20, tzinfo=timezone.utc)

    def test_document_valide(self):
        doc = hm.construire_historique("L1", {
            "niveau_risque": "eleve", "taux_invendus": "62.5", "remise_recommandee": "30",
            "date_calcul": "2026-11-20T08:00:00+00:00", "magasin": "M2"}, self.MAINTENANT)
        self.assertEqual(doc["taux_invendus"], 62.5)
        self.assertEqual(doc["remise_recommandee"], 30)
        self.assertEqual(doc["date_calcul"], datetime(2026, 11, 20, 8, tzinfo=timezone.utc))

    def test_documents_invalides(self):
        self.assertIsNone(hm.construire_historique("L1", {"niveau_risque": "x",
                          "date_calcul": "2026-11-20T08:00:00"}, self.MAINTENANT))
        self.assertIsNone(hm.construire_historique("L1", {"niveau_risque": "faible"}, self.MAINTENANT))

    def test_vitesse(self):
        doc = hm.construire_vitesse({"vitesse_moyenne_jour": "2.5", "quantite_vendue": "20",
                                     "jours_actifs": "8"}, self.MAINTENANT)
        self.assertEqual((doc["vitesse_moyenne_jour"], doc["quantite_vendue"], doc["jours_actifs"]),
                         (2.5, 20, 8))
        self.assertIsNone(hm.construire_vitesse({"vitesse_moyenne_jour": "0"}, self.MAINTENANT))


if __name__ == "__main__":
    unittest.main()
