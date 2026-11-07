{
    "name": "Risque d'invendu par lot",
    "version": "18.0.1.0.0",
    "summary": "Affiche sur chaque lot le risque d'invendu calculé par la plateforme Big Data",
    "description": """
Étend stock.lot avec le niveau de risque, le taux d'invendus projeté et la remise
recommandée. Les valeurs sont écrites par le script scripts/sync_risque_odoo.py
(Redis -> Odoo, par XML-RPC).
    """,
    "category": "Inventory",
    "depends": ["stock", "product_expiry"],
    "data": ["views/stock_lot_views.xml"],
    "license": "LGPL-3",
    "installable": True,
    "application": False,
}
