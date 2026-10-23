# STML Sync

Transactional batch upsert surface for Odoo, used by the STML sync engine.
One RPC call creates or updates up to 1,000 records of one model, each record
in its own savepoint, so one bad row costs one row instead of the batch.

Works on Odoo 16, 18 and 19 (Community and Enterprise), on-premise and Odoo.sh.
It cannot be installed on Odoo Online (odoo.com SaaS), which does not accept
third-party modules.

## What it adds

Two methods on the abstract model `stml.sync`, callable over JSON-RPC
(`/jsonrpc`, `/web/dataset/call_kw`) or Odoo 19's `/json/2/` transport:

- `ping()` returns the module version, Odoo version and capability flags.
- `batch_upsert(model=, records=, options=)` creates or updates records.
  Optional per-record `xml_id`, `skip_unchanged` no-op detection, and an
  idempotency ledger (`stml.sync.operation`) that replays a retried
  `operation_id` instead of running it again.

The module has no UI, no menus and no settings. It sends nothing anywhere:
every call runs as the authenticated user through the ORM, with access rights
and record rules unchanged, and the ledger stores results only, never the
submitted values. Ledger rows older than seven days are purged by a daily cron.

## Install

Copy `stml_sync` into an addons folder on your Odoo server (or add this
repository to your Odoo.sh project), update the apps list and install
"STML Sync". No configuration is needed.

## Tests

```
odoo -d stml_sync_test -i base,stml_sync --stop-after-init --without-demo=all
odoo -d stml_sync_test -u stml_sync --test-tags /stml_sync --stop-after-init
```

## License

LGPL-3. See `LICENSE`.
