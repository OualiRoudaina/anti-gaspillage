#!/usr/bin/env python3
"""Peuple Odoo avec les données de test du projet anti-gaspillage.

Crée : 3 magasins (entrepôts), 30 produits périssables suivis par lot,
150 lots avec date de péremption et stock initial.
Génère aussi scripts/seed/produits.csv et lots.csv (pour l'import CSV du cours).

Le script peut être relancé sans danger : ce qui existe déjà n'est pas recréé.

Utilisation :
    python scripts/seed_odoo.py            # tout créer dans Odoo
    python scripts/seed_odoo.py --dry-run  # écrire seulement les CSV
"""
import argparse
import csv
import os
import random
import sys
import time
import xmlrpc.client
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SEED_DIR = Path(__file__).resolve().parent / "seed"

# Modules du cours : Inventaire, Ventes, Dates d'expiration, Contacts, Facturation
MODULES = ["contacts", "stock", "sale_management", "account", "product_expiry"]

# (code, nom)
STORES = [("M1", "Magasin 1 - Centre"), ("M2", "Magasin 2 - Nord"), ("M3", "Magasin 3 - Sud")]

LOTS_PAR_PRODUIT = 5  # 30 produits x 5 = 150 lots

# (référence, nom, catégorie, durée de vie max en jours, prix)
PRODUCTS = [
    ("LAI001", "Yaourt nature x4", "Produits laitiers", 21, 2.5),
    ("LAI002", "Lait demi-écrémé 1L", "Produits laitiers", 14, 1.8),
    ("LAI003", "Fromage frais 200g", "Produits laitiers", 15, 3.2),
    ("LAI004", "Beurre 250g", "Produits laitiers", 30, 5.5),
    ("LAI005", "Crème fraîche 20cl", "Produits laitiers", 18, 2.9),
    ("LAI006", "Fromage râpé 200g", "Produits laitiers", 25, 4.2),
    ("LAI007", "Crème caramel x4", "Produits laitiers", 20, 3.6),
    ("BOU001", "Baguette", "Boulangerie", 2, 0.25),
    ("BOU002", "Pain de mie", "Boulangerie", 6, 2.2),
    ("BOU003", "Croissants x6", "Boulangerie", 3, 4.8),
    ("BOU004", "Brioche", "Boulangerie", 5, 3.4),
    ("BOU005", "Pain complet", "Boulangerie", 4, 1.5),
    ("VIA001", "Poulet entier", "Viandes et poissons", 6, 14.0),
    ("VIA002", "Steak haché 500g", "Viandes et poissons", 5, 16.5),
    ("VIA003", "Escalope de dinde", "Viandes et poissons", 6, 12.0),
    ("VIA004", "Saucisses 400g", "Viandes et poissons", 9, 9.5),
    ("VIA005", "Filet de poisson blanc", "Viandes et poissons", 4, 18.0),
    ("VIA006", "Jambon de dinde 4 tranches", "Viandes et poissons", 12, 4.9),
    ("FRU001", "Salade verte", "Fruits et légumes", 6, 1.2),
    ("FRU002", "Tomates 1kg", "Fruits et légumes", 8, 2.4),
    ("FRU003", "Fraises 500g", "Fruits et légumes", 5, 6.5),
    ("FRU004", "Bananes 1kg", "Fruits et légumes", 7, 3.1),
    ("FRU005", "Carottes 1kg", "Fruits et légumes", 14, 1.9),
    ("FRU006", "Courgettes 1kg", "Fruits et légumes", 9, 2.6),
    ("FRA001", "Houmous 200g", "Plats frais", 14, 3.9),
    ("FRA002", "Salade composée", "Plats frais", 4, 5.2),
    ("FRA003", "Pizza fraîche", "Plats frais", 7, 8.5),
    ("FRA004", "Jus d'orange frais 1L", "Plats frais", 6, 5.8),
    ("FRA005", "Oeufs x12", "Plats frais", 28, 6.0),
    ("FRA006", "Pâtes fraîches 250g", "Plats frais", 12, 4.4),
]


def build_dataset(seed=42):
    """Jeu de données reproductible (même graine = mêmes données)."""
    rnd = random.Random(seed)
    today = date.today()
    produits, lots, n = [], [], 0
    for ref, nom, categorie, duree_max, prix in PRODUCTS:
        produits.append({"ref": ref, "nom": nom, "categorie": categorie,
                         "duree_max": duree_max, "prix": prix})
        for _ in range(LOTS_PAR_PRODUIT):
            n += 1
            lots.append({
                "lot": f"LOT-{today.year}-{n:04d}",
                "ref": ref,
                "magasin": STORES[n % len(STORES)][0],
                "stock": rnd.randint(20, 250),
                "peremption": (today + timedelta(days=rnd.randint(1, duree_max))).isoformat(),
            })
    return produits, lots


def write_csv(produits, lots):
    SEED_DIR.mkdir(exist_ok=True)
    with open(SEED_DIR / "produits.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Référence interne", "Nom", "Catégorie", "Prix de vente"])
        for p in produits:
            w.writerow([p["ref"], p["nom"], p["categorie"], p["prix"]])
    noms = {p["ref"]: p["nom"] for p in produits}
    with open(SEED_DIR / "lots.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Numéro de lot", "Produit", "Magasin", "Quantité", "Date de péremption"])
        for l in lots:
            w.writerow([l["lot"], noms[l["ref"]], l["magasin"], l["stock"], l["peremption"]])
    print(f"CSV écrits dans {SEED_DIR}")


def load_config():
    """Lit .env puis les variables d'environnement (qui ont la priorité)."""
    cfg = {"ODOO_URL": "http://localhost:8069", "ODOO_DB": "antigaspi"}
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                cfg[k.strip()] = v.strip()
    cfg.update({k: v for k, v in os.environ.items() if k.startswith("ODOO_")})
    return cfg


class Odoo:
    def __init__(self, url, db, login, password):
        self.db, self.password = db, password
        common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
        self.uid = common.authenticate(db, login, password, {})
        if not self.uid:
            sys.exit("Connexion refusée : vérifie ODOO_DB, ODOO_LOGIN et ODOO_PASSWORD dans .env")
        self.models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object", allow_none=True)

    def call(self, model, method, *args, **kwargs):
        """Appel XML-RPC. On réessaie si Odoo redémarre (après une installation de module)."""
        for essai in range(6):
            try:
                return self.models.execute_kw(self.db, self.uid, self.password,
                                              model, method, list(args), kwargs)
            except (ConnectionError, xmlrpc.client.ProtocolError, OSError):
                if essai == 5:
                    raise
                print("  Odoo n'est pas joignable, nouvelle tentative dans 10 s...")
                time.sleep(10)

    def get_or_create(self, model, domain, vals):
        ids = self.call(model, "search", domain, limit=1)
        return ids[0] if ids else self.call(model, "create", vals)


def install_modules(o):
    print("Modules :")
    mods = o.call("ir.module.module", "search_read", [("name", "in", MODULES)],
                  fields=["name", "state"])
    trouves = {m["name"] for m in mods}
    for manquant in set(MODULES) - trouves:
        print(f"  ! module introuvable : {manquant}")
    for m in mods:
        if m["state"] == "installed":
            print(f"  {m['name']} : déjà installé")
        else:
            print(f"  {m['name']} : installation (peut durer quelques minutes)...")
            o.call("ir.module.module", "button_immediate_install", [m["id"]])
    # Active "Lots et numéros de série" dans les réglages d'Inventaire
    try:
        sid = o.call("res.config.settings", "create", {"group_stock_production_lot": True})
        o.call("res.config.settings", "execute", [sid])
        print("  suivi par lots activé")
    except Exception as e:  # pas bloquant : on peut l'activer à la main
        print(f"  ! activation des lots à faire à la main (Inventaire > Configuration) : {e}")


def ensure_warehouses(o):
    print("Magasins :")
    existants = o.call("stock.warehouse", "search_read", [], fields=["name", "code"])
    codes = {w["code"] for w in existants}
    libres = [w for w in existants if w["code"] not in dict(STORES)]
    for code, nom in STORES:
        if code in codes:
            continue
        if libres:  # on réutilise l'entrepôt créé par défaut par Odoo
            w = libres.pop(0)
            o.call("stock.warehouse", "write", [w["id"]], {"name": nom, "code": code})
            print(f"  {code} : entrepôt par défaut renommé")
        else:
            o.call("stock.warehouse", "create", {"name": nom, "code": code})
            print(f"  {code} : créé")
    whs = o.call("stock.warehouse", "search_read", [("code", "in", [c for c, _ in STORES])],
                 fields=["code", "lot_stock_id"])
    return {w["code"]: w["lot_stock_id"][0] for w in whs}


def seed(o, produits, lots):
    install_modules(o)
    emplacements = ensure_warehouses(o)
    company_id = o.call("res.users", "read", [o.uid], fields=["company_id"])[0]["company_id"][0]

    print("Produits :")
    variantes = {}
    for p in produits:
        categ = o.get_or_create("product.category", [("name", "=", p["categorie"])],
                                {"name": p["categorie"]})
        tmpl = o.call("product.template", "search", [("default_code", "=", p["ref"])], limit=1)
        if not tmpl:
            tmpl = [o.call("product.template", "create", {
                "name": p["nom"], "default_code": p["ref"], "categ_id": categ,
                "list_price": p["prix"],
                "type": "consu",            # bien physique
                "is_storable": True,        # suivre le stock
                "tracking": "lot",          # suivi par lot
                "use_expiration_date": True,
                "expiration_time": p["duree_max"], "alert_time": 2,
            })]
        variantes[p["ref"]] = o.call("product.product", "search",
                                     [("product_tmpl_id", "=", tmpl[0])], limit=1)[0]
    print(f"  {len(variantes)} produits prêts")

    print("Lots et stock :")
    quants, crees = [], 0
    for l in lots:
        pid = variantes[l["ref"]]
        if o.call("stock.lot", "search", [("name", "=", l["lot"]), ("product_id", "=", pid)], limit=1):
            continue
        lot_id = o.call("stock.lot", "create", {
            "name": l["lot"], "product_id": pid, "company_id": company_id,
            "expiration_date": l["peremption"] + " 12:00:00",
        })
        quants.append(o.call("stock.quant", "create", {
            "product_id": pid, "location_id": emplacements[l["magasin"]],
            "lot_id": lot_id, "inventory_quantity": l["stock"],
        }, context={"inventory_mode": True}))
        crees += 1
        if len(quants) >= 50:  # on valide le stock par paquets de 50
            o.call("stock.quant", "action_apply_inventory", quants, context={"inventory_mode": True})
            quants = []
            print(f"  {crees} lots...")
    if quants:
        o.call("stock.quant", "action_apply_inventory", quants, context={"inventory_mode": True})
    print(f"  {crees} nouveaux lots créés ({len(lots) - crees} existaient déjà)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="écrit seulement les CSV, sans toucher à Odoo")
    args = ap.parse_args()

    produits, lots = build_dataset()
    write_csv(produits, lots)
    if args.dry_run:
        print(f"{len(produits)} produits, {len(lots)} lots (rien envoyé à Odoo)")
        return

    cfg = load_config()
    for cle in ("ODOO_LOGIN", "ODOO_PASSWORD"):
        if not cfg.get(cle):
            sys.exit(f"{cle} manquant : ajoute-le dans .env")
    o = Odoo(cfg["ODOO_URL"], cfg["ODOO_DB"], cfg["ODOO_LOGIN"], cfg["ODOO_PASSWORD"])
    seed(o, produits, lots)
    print("Terminé.")


if __name__ == "__main__":
    main()
