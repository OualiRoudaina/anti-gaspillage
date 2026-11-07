from odoo import fields, models


class StockLot(models.Model):
    _inherit = "stock.lot"

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
