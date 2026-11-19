from odoo import api, fields, models

NIVEAUX = {"eleve": "Élevé", "a_surveiller": "À surveiller", "faible": "Faible"}


class ReportLotsRisk(models.AbstractModel):
    _name = "report.lot_waste_risk.report_lots_risk"
    _description = "Rapport des lots à risque d'invendu"

    @api.model
    def _get_report_values(self, docids, data=None):
        """Prépare les données : lots à risque regroupés par magasin.

        Sans sélection, le rapport couvre tous les lots à risque élevé ou à surveiller.
        """
        Lot = self.env["stock.lot"]
        if docids:
            lots = Lot.browse(docids)
        else:
            lots = Lot.search([("waste_risk_level", "in", ["eleve", "a_surveiller"])])

        magasins = {}
        for lot in lots:
            nom = lot.waste_store or "Non renseigné"
            magasins.setdefault(nom, []).append(lot)

        resultat = []
        for nom in sorted(magasins):
            triees = sorted(magasins[nom], key=lambda l: -l.waste_risk_rate)
            resultat.append({
                "nom": nom,
                "nb_eleve": sum(1 for l in triees if l.waste_risk_level == "eleve"),
                "nb_surveiller": sum(1 for l in triees if l.waste_risk_level == "a_surveiller"),
                "lignes": [{
                    "lot": l.name,
                    "produit": l.product_id.display_name,
                    "niveau": l.waste_risk_level or "",
                    "niveau_label": NIVEAUX.get(l.waste_risk_level, "-"),
                    "taux": l.waste_risk_rate,
                    "invendus": l.waste_projected_unsold,
                    "jours": l.waste_days_left,
                    "remise": l.waste_discount,
                } for l in triees],
            })

        return {
            "doc_ids": lots.ids,
            "doc_model": "stock.lot",
            "docs": lots,
            "magasins": resultat,
            "total": len(lots),
            "date_edition": fields.Date.context_today(self).strftime("%d/%m/%Y"),
        }
