# Keep "version" in sync with MODULE_VERSION in models/stml_sync.py (a unit
# test asserts they match). Odoo prefixes the series itself ("19.0.1.0.0"), so
# one manifest serves 16, 18 and 19.
{
    "name": "STML Sync",
    "summary": "Transactional batch upsert surface for the STML sync engine",
    "description": """
STML Sync — companion module for the STML sync engine.

Exposes two RPC methods on the abstract model ``stml.sync``:

* ``ping()`` — module version, Odoo version and capability flags, used by the
  engine to detect the module and fall back to stock RPC when it is absent.
* ``batch_upsert(model=, records=, options=)`` — up to 1000 heterogeneous
  creates/updates in one round-trip, each record in its own savepoint so one
  bad row costs one row. Optional ``xml_id`` per record, ``skip_unchanged``
  no-op detection, and an idempotency ledger (``stml.sync.operation``) that
  replays a retried ``operation_id`` instead of re-executing it.

Everything runs as the calling user: access rights and record rules apply
unchanged, there is no ``sudo()`` anywhere, and the ledger never stores the
submitted values.
""",
    "version": "1.0.1",
    "category": "Technical",
    "license": "LGPL-3",
    "author": "STML",
    "website": "https://stml.io",
    "support": "info@stml.io",
    "depends": ["base"],
    "images": ["static/description/banner.png"],
    "data": [
        "security/ir.model.access.csv",
        "security/stml_sync_security.xml",
        "data/ir_cron_data.xml",
    ],
    "post_init_hook": "post_init_hook",
    "installable": True,
    "application": False,
    "auto_install": False,
}
