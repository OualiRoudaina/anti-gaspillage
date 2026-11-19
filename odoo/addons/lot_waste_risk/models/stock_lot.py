from odoo import api, fields, models


class StockLot(models.Model):
    _inherit = "stock.lot"

    waste_store = fields.Char(
        string="Magasin", readonly=True, index=True, copy=False
    )
    waste_risk_level = fields.Selection(
        [("faible", "Faible"), ("a_surveiller", "À surveiller"), ("eleve", "Élevé")],
        string="Niveau de risque",
        readonly=True,
        index=True,
        copy=False,
    )
    waste_risk_rate = fields.Float(
        string="Taux d'invendus projeté (%)", readonly=True, copy=False, digits=(16, 2)
    )
    waste_projected_unsold = fields.Float(
        string="Invendus projetés", readonly=True, copy=False, digits=(16, 2)
    )
    waste_sales_speed = fields.Float(
        string="Vitesse de vente (unités/jour)", readonly=True, copy=False, digits=(16, 2)
    )
    waste_days_left = fields.Float(
        string="Jours restants", readonly=True, copy=False, digits=(16, 2)
    )
    waste_discount = fields.Integer(
        string="Remise recommandée (%)", readonly=True, copy=False
    )
    waste_risk_updated = fields.Datetime(
        string="Risque mis à jour le", readonly=True, copy=False
    )

    def _create_discount_activities(self):
        """Crée une activité « Appliquer la remise » pour chaque lot à risque élevé.

        Une activité identique déjà ouverte n'est pas recréée : la méthode peut donc
        être relancée sans produire de doublons. Retourne le nombre d'activités créées.
        """
        admin = self.env.ref("base.user_admin", raise_if_not_found=False) or self.env.user
        creees = 0
        for lot in self.filtered(
            lambda l: l.waste_risk_level == "eleve" and l.waste_discount > 0
        ):
            resume = f"Appliquer la remise de {lot.waste_discount} %"
            if lot.activity_ids.filtered(lambda a: a.summary == resume):
                continue
            responsable = lot.product_id.responsible_id or admin
            lot.activity_schedule(
                "mail.mail_activity_data_todo",
                summary=resume,
                note=(
                    f"Lot {lot.name} ({lot.product_id.display_name}"
                    f"{', magasin ' + lot.waste_store if lot.waste_store else ''}) : "
                    f"{lot.waste_risk_rate:.0f} % d'invendus projetés, "
                    f"{lot.waste_days_left:.1f} jour(s) restant(s)."
                ),
                user_id=responsable.id,
            )
            creees += 1
        return creees

    @api.model
    def _cron_apply_discount_activities(self):
        """Tâche planifiée : traite tous les lots à risque élevé avec une remise."""
        lots = self.search(
            [("waste_risk_level", "=", "eleve"), ("waste_discount", ">", 0)]
        )
        return lots._create_discount_activities()
