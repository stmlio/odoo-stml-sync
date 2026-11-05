"""Install hooks. Signature differs across the supported series: Odoo 16
calls ``post_init_hook(cr, registry)``, Odoo 17+ calls ``post_init_hook(env)``.
"""

from odoo import SUPERUSER_ID, api


def post_init_hook(*args):
    if len(args) == 1:
        env = args[0]
    else:
        cr, _registry = args
        env = api.Environment(cr, SUPERUSER_ID, {})
    # Odoo 16's ir.cron.numbercall defaults to 1 (run once, then deactivate);
    # the field is gone in 17+, so it cannot be set from the XML data file.
    cron = env.ref("stml_sync.ir_cron_gc_operations", raise_if_not_found=False)
    if cron and "numbercall" in cron._fields:
        cron.write({"numbercall": -1})
