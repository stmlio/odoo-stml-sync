"""Idempotency ledger: one row per completed ``batch_upsert`` call.

The row is inserted in the same transaction as the data it describes, so the
replay guarantee is exactly as durable as the commit itself. The ledger stores
*results only* (indices, ids, actions, error text) — never the submitted
values — so it stays small and secret-free.
"""

from datetime import timedelta

from odoo import api, fields, models

LEDGER_RETENTION_DAYS = 7

_UNIQUE_MSG = "An operation with this id has already been recorded."


class StmlSyncOperation(models.Model):
    _name = "stml.sync.operation"
    _description = "STML sync operation ledger"
    _order = "id desc"
    _rec_name = "name"

    name = fields.Char(string="Operation id", required=True, index=True, readonly=True)
    user_id = fields.Many2one(
        "res.users",
        string="User",
        required=True,
        readonly=True,
        index=True,
        ondelete="cascade",
        default=lambda self: self.env.uid,
    )
    target_model = fields.Char(string="Target model", required=True, readonly=True)
    record_count = fields.Integer(string="Records", readonly=True)
    result_json = fields.Text(string="Results (JSON)", required=True, readonly=True)

    # Odoo 19 dropped `_sql_constraints` in favour of table objects; 16/18
    # only know the list form. Same constraint name on every series.
    if hasattr(models, "Constraint"):
        _name_uniq = models.Constraint("UNIQUE(name)", _UNIQUE_MSG)
    else:
        _sql_constraints = [("name_uniq", "UNIQUE(name)", _UNIQUE_MSG)]

    @api.model
    def _gc_operations(self, days=LEDGER_RETENTION_DAYS):
        """Delete ledger rows older than ``days``. Called by the daily cron
        (as the system user, so every user's rows are in scope)."""
        cutoff = fields.Datetime.now() - timedelta(days=days)
        stale = self.search([("create_date", "<", cutoff)])
        count = len(stale)
        stale.unlink()
        return count
