"""``stml.sync`` — the RPC surface the STML sync engine talks to.

Contract:

    ping() -> {module_version, odoo_version, capabilities}
    batch_upsert(*, model, records, options=None) -> {module_version, replayed, results}
    batch_reconcile(*, groups, options=None) -> {module_version, results}

Design rules, in order of importance:

* Runs strictly as the calling user. Every inner ``create`` / ``write`` goes
  through the ORM with ACLs and record rules intact. No ``sudo()`` anywhere.
* One transaction per call, one savepoint per record. A failing record is
  rolled back to its savepoint and reported; the others commit together with
  the request.
* No server-side matching and no mapping logic: the engine sends a resolved
  ``id`` (update) or ``None`` (create) and ORM-ready ``values``.
* Keyword-only parameters: the json2 transport (Odoo 19+) binds kwargs only.
"""

import json
from datetime import date, datetime

import psycopg2

from odoo import api, fields, models, release
from odoo.exceptions import (
    AccessDenied,
    AccessError,
    MissingError,
    UserError,
    ValidationError,
)

MODULE_VERSION = "1.1.0"
CAPABILITIES = ["batch_upsert", "skip_unchanged", "xml_id", "idempotency", "batch_reconcile"]
MAX_RECORDS = 1000
MAX_GROUPS = 1000
# Groups per ``_reconcile_plan`` savepoint (Odoo 17+). One prefetch and one
# balance check per chunk; a failing chunk is bisected, so a bad group costs
# about log2(chunk) extra plan calls, not the whole batch.
PLAN_CHUNK = 50
MAX_ERROR_CHARS = 2000
MAX_OPERATION_ID_CHARS = 128


# --------------------------------------------------------------------------
# Value comparison — a verbatim mirror of the STML sync engine's
# ``_values_equal``. Keep the two in step.
# --------------------------------------------------------------------------


def _is_x2m_commands(v):
    """Whether ``v`` is an x2many command list — ``[(op, id, vals?), ...]``."""
    return bool(
        isinstance(v, (list, tuple))
        and bool(v)
        and all(
            isinstance(c, (list, tuple))
            and len(c) in (2, 3)
            and isinstance(c[0], int)
            and 0 <= c[0] <= 6
            for c in v
        )
    )


def _as_number(x):
    """``x`` as a float if it is a number or a numeric string, else None.
    Booleans are not numbers here (Odoo booleans compare as-is)."""
    if isinstance(x, bool):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip()
        if s:
            try:
                return float(s)
            except ValueError:
                return None
    return None


def _values_equal(existing, new):
    """Compare an incoming write value to what Odoo currently has, with the
    engine's normalisation: m2o ``[id, name]`` → id; ``False``/``None``/``""``
    are all empty; numeric strings bridge to numbers when the types differ."""
    if _is_x2m_commands(new):
        if not isinstance(existing, (list, tuple)) or not all(isinstance(i, int) for i in existing):
            return False
        existing_ids = set(existing)
        if all(c[0] == 4 for c in new):
            return all(c[1] in existing_ids for c in new)
        if len(new) == 1 and new[0][0] == 6:
            return set(new[0][2] or []) == existing_ids
        return False
    if isinstance(existing, (list, tuple)) and len(existing) == 2 and isinstance(existing[0], int):
        existing = existing[0]
    if existing in (False, None, "") and new in (False, None, ""):
        return True
    ex_num = isinstance(existing, (int, float)) and not isinstance(existing, bool)
    nw_num = isinstance(new, (int, float)) and not isinstance(new, bool)
    if ex_num != nw_num:
        en, nn = _as_number(existing), _as_number(new)
        if en is not None and nn is not None:
            return en == nn
    return existing == new


def _wire_form(value):
    """Bring an ORM write-form value into the shape the engine sees over RPC
    (dates/datetimes as Odoo strings, binary as text)."""
    if isinstance(value, datetime):
        return fields.Datetime.to_string(value)
    if isinstance(value, date):
        return fields.Date.to_string(value)
    if isinstance(value, bytes):
        try:
            return value.decode("ascii")
        except UnicodeDecodeError:
            return value
    return value


def _error_class(exc):
    """The contract's error taxonomy. Order matters: ValidationError,
    AccessError and MissingError all subclass UserError."""
    if isinstance(exc, ValidationError):
        return "ValidationError"
    if isinstance(exc, (AccessError, AccessDenied)):
        return "AccessError"
    if isinstance(exc, MissingError):
        return "MissingError"
    if isinstance(exc, UserError):
        return "UserError"
    if isinstance(exc, psycopg2.IntegrityError):
        return "IntegrityError"
    return "Other"


def _error_text(exc):
    text = str(exc) or exc.__class__.__name__
    return text[:MAX_ERROR_CHARS]


class StmlSync(models.AbstractModel):
    _name = "stml.sync"
    _description = "STML sync batch surface"

    # ------------------------------------------------------------- probe

    @api.model
    def ping(self):
        """Capability probe. Cheap, side-effect free; the engine calls it once
        per run and treats any failure as "module absent"."""
        return {
            "module_version": MODULE_VERSION,
            "odoo_version": release.version,
            "odoo_series": release.series,
            "capabilities": list(CAPABILITIES),
        }

    # ------------------------------------------------------ batch_upsert

    @api.model
    def batch_upsert(self, *, model, records, options=None):
        """Create/update up to MAX_RECORDS records of ``model`` in one call,
        each in its own savepoint. See the module docstring for the contract."""
        options = options or {}
        if not isinstance(options, dict):
            raise UserError("options must be a mapping")
        operation_id = options.get("operation_id")
        if not isinstance(operation_id, str) or not operation_id.strip():
            raise UserError("options.operation_id is required (a client-minted uuid per call)")
        if len(operation_id) > MAX_OPERATION_ID_CHARS:
            raise UserError("options.operation_id is too long (max %d chars)" % MAX_OPERATION_ID_CHARS)
        skip_unchanged = bool(options.get("skip_unchanged", False))

        if not isinstance(model, str) or model not in self.env:
            raise UserError("Unknown model %r" % (model,))
        Model = self.env[model]
        if Model._abstract or Model._transient:
            raise UserError("Model %r is abstract or transient; batch_upsert needs a stored model" % model)
        if not isinstance(records, (list, tuple)):
            raise UserError("records must be a list")
        if len(records) > MAX_RECORDS:
            raise UserError(
                "Too many records: %d (max %d per call). Split the batch." % (len(records), MAX_RECORDS)
            )

        # Idempotent replay: a retried operation_id returns the stored result
        # instead of re-executing. The record rule limits the lookup to the
        # caller's own operations.
        Ledger = self.env["stml.sync.operation"]
        prior = Ledger.search([("name", "=", operation_id)], limit=1)
        if prior:
            return {
                "module_version": MODULE_VERSION,
                "replayed": True,
                "results": json.loads(prior.result_json),
            }

        # Fast path first: every well-formed create record goes through ONE
        # batched Model.create, bisected on failure; whatever is left over
        # (updates, malformed records, the leaves of a failed bisection) runs
        # through the per-record savepoint loop exactly as before.
        results = [None] * len(records)
        self._create_batched(Model, records, results)
        for position, rec in enumerate(records):
            if results[position] is None:
                results[position] = self._apply_one(Model, rec, position, skip_unchanged)

        # Same transaction as the data: the replay guarantee is exactly as
        # durable as the commit. A concurrent duplicate trips the unique
        # constraint here, the request aborts, and the retry reads the winner.
        Ledger.create({
            "name": operation_id,
            "target_model": model,
            "record_count": len(records),
            "result_json": json.dumps(results),
        })
        Ledger.flush_model()

        return {"module_version": MODULE_VERSION, "replayed": False, "results": results}

    # --------------------------------------------------- batch_reconcile

    @api.model
    def batch_reconcile(self, *, groups, options=None):
        """Reconcile up to MAX_GROUPS groups of ``account.move.line`` ids in
        one call, each group isolated: a group that fails is rolled back to
        its savepoint and reported, the others commit with the request.

        ``groups``: ``[{"index": i, "line_ids": [..]}, ...]`` — each group the
        lines of ONE settlement (a document's receivable or payable line plus
        the lines of the entries that settle it): same account and partner,
        posted, unreconciled — exactly what ``account.move.line.reconcile``
        takes. Returns ``{module_version, results}`` with one
        ``{index, action: "reconciled" | "error", error?, error_class?}`` per
        group, in input order.

        Odoo 17+ reconciles a chunk of groups through ``_reconcile_plan``
        (one prefetch and one balance check per chunk instead of per group);
        a failing chunk is bisected down to single groups, which run through
        ``reconcile()`` in their own savepoint. Odoo 16 has no plan API and
        takes the per-group path directly.

        No idempotency ledger: a retried group fails on Odoo's own "already
        reconciled" check, and the caller reads the lines' ``reconciled``
        flag before sending, so a replay is cheap and harmless."""
        options = options or {}
        if not isinstance(options, dict):
            raise UserError("options must be a mapping")
        if "account.move.line" not in self.env:
            raise UserError("batch_reconcile needs the Accounting app (account) installed")
        if not isinstance(groups, (list, tuple)):
            raise UserError("groups must be a list")
        if len(groups) > MAX_GROUPS:
            raise UserError(
                "Too many groups: %d (max %d per call). Split the batch." % (len(groups), MAX_GROUPS)
            )
        Line = self.env["account.move.line"]
        results = [None] * len(groups)
        positions = []
        for i, group in enumerate(groups):
            if self._group_line_ids(group) is None:
                results[i] = {
                    "index": self._group_index(group, i),
                    "action": "error",
                    "error": "a group is {index, line_ids: [int, int, ...]} with at least two line ids",
                    "error_class": "UserError",
                }
            else:
                positions.append(i)
        if hasattr(Line, "_reconcile_plan"):
            for start in range(0, len(positions), PLAN_CHUNK):
                self._reconcile_plan_group(Line, groups, positions[start:start + PLAN_CHUNK], results)
        for i in positions:
            if results[i] is None:
                results[i] = self._reconcile_one(Line, groups[i], i)
        return {"module_version": MODULE_VERSION, "results": results}

    @staticmethod
    def _group_index(group, position):
        return group.get("index", position) if isinstance(group, dict) else position

    @staticmethod
    def _group_line_ids(group):
        """The group's line ids as a list of ints, or None when malformed."""
        if not isinstance(group, dict):
            return None
        ids = group.get("line_ids")
        if not isinstance(ids, (list, tuple)) or len(ids) < 2:
            return None
        out = []
        for v in ids:
            if isinstance(v, bool) or not isinstance(v, int):
                return None
            out.append(v)
        return out

    def _reconcile_plan_group(self, Line, groups, positions, results):
        """Reconcile ``positions`` as one ``_reconcile_plan`` in one savepoint;
        on failure bisect, down to single groups left for ``_reconcile_one``."""
        if len(positions) < 2:
            return
        try:
            with self.env.cr.savepoint():
                Line._reconcile_plan([Line.browse(self._group_line_ids(groups[i])) for i in positions])
                for i in positions:
                    results[i] = {"index": self._group_index(groups[i], i), "action": "reconciled"}
        except Exception:  # noqa: BLE001 — bisect; the leaves report per group
            self.env.invalidate_all(flush=False)
            for i in positions:
                results[i] = None
            half = len(positions) // 2
            self._reconcile_plan_group(Line, groups, positions[:half], results)
            self._reconcile_plan_group(Line, groups, positions[half:], results)

    def _reconcile_one(self, Line, group, position):
        """One group, one savepoint. Never raises."""
        index = self._group_index(group, position)
        try:
            with self.env.cr.savepoint():
                Line.browse(self._group_line_ids(group)).reconcile()
        except Exception as exc:  # noqa: BLE001 — per-group isolation
            self.env.invalidate_all(flush=False)
            return {
                "index": index,
                "action": "error",
                "error": _error_text(exc),
                "error_class": _error_class(exc),
            }
        return {"index": index, "action": "reconciled"}

    # --------------------------------------------------- batched creates

    def _create_batched(self, Model, records, results):
        """Create records in bulk. Odoo's batched ``create(vals_list)`` is an
        order of magnitude cheaper per record than N single creates
        (computed fields, variants and inverse writes run once per batch):
        2,000 product.template rows measured 28 ms/row one by one against
        2.7 ms/row batched. Per-record isolation is kept by bisection: a
        batch that fails rolls back to its savepoint and is retried as two
        halves, down to single records, which the per-record loop handles
        with their own error report. k bad rows cost about k * log2(batch)
        extra creates, all server-side, instead of one create per row."""
        positions = []
        for i, rec in enumerate(records):
            if not isinstance(rec, dict) or rec.get("id") not in (None, False):
                continue
            values = rec.get("values")
            if values is not None and not isinstance(values, dict):
                continue
            xml_id = rec.get("xml_id") or None
            if xml_id is not None and not isinstance(xml_id, str):
                continue
            positions.append(i)
        self._create_group(Model, records, positions, results)

    def _create_group(self, Model, records, positions, results):
        if len(positions) < 2:
            return  # a single record is the per-record loop's job
        try:
            with self.env.cr.savepoint():
                vals_list = [dict(records[i].get("values") or {}) for i in positions]
                created = Model.create(vals_list)
                if len(created) != len(positions):
                    raise UserError(
                        "batched create returned %d records for %d values" % (len(created), len(positions))
                    )
                for i, record in zip(positions, created):
                    xml_id = records[i].get("xml_id") or None
                    if xml_id:
                        self._ensure_xml_id(record, xml_id)
                    results[i] = {
                        "index": records[i].get("index", i),
                        "id": record.id,
                        "action": "created",
                    }
        except Exception:  # noqa: BLE001 — bisect; the leaves report per record
            self.env.invalidate_all(flush=False)
            for i in positions:
                results[i] = None
            half = len(positions) // 2
            self._create_group(Model, records, positions[:half], results)
            self._create_group(Model, records, positions[half:], results)

    # ------------------------------------------------------- per record

    def _apply_one(self, Model, rec, position, skip_unchanged):
        """One record, one savepoint. Never raises: every failure becomes an
        ``action: "error"`` result and the transaction is left clean."""
        index = rec.get("index", position) if isinstance(rec, dict) else position
        rec_id = rec.get("id") if isinstance(rec, dict) else None
        if rec_id is False:
            rec_id = None
        try:
            with self.env.cr.savepoint():
                outcome = self._apply_in_savepoint(Model, rec, rec_id, skip_unchanged)
        except Exception as exc:  # noqa: BLE001 — the whole point is per-row isolation
            # The savepoint rolled back; drop any ORM cache that may still
            # hold the rolled-back state before touching the next record.
            self.env.invalidate_all(flush=False)
            return {
                "index": index,
                "id": rec_id if isinstance(rec_id, int) and not isinstance(rec_id, bool) else None,
                "action": "error",
                "error": _error_text(exc),
                "error_class": _error_class(exc),
            }
        return {"index": index, "id": outcome[0], "action": outcome[1]}

    def _apply_in_savepoint(self, Model, rec, rec_id, skip_unchanged):
        if not isinstance(rec, dict):
            raise UserError("each record must be a mapping with values/id/xml_id")
        values = rec.get("values")
        if values is None:
            values = {}
        if not isinstance(values, dict):
            raise UserError("values must be a mapping")
        xml_id = rec.get("xml_id") or None
        if xml_id is not None and not isinstance(xml_id, str):
            raise UserError("xml_id must be a string of the form module.name")

        if rec_id is None:
            record = Model.create(values)
            action = "created"
        else:
            if not isinstance(rec_id, int) or isinstance(rec_id, bool):
                raise UserError("id must be an integer or null")
            record = Model.browse(rec_id)
            if not record.exists():
                raise MissingError("%s record %d does not exist" % (Model._name, rec_id))
            if skip_unchanged and self._unchanged(record, values):
                action = "skipped"
            else:
                record.write(values)
                action = "updated"

        if xml_id:
            self._ensure_xml_id(record, xml_id)
        return record.id, action

    # ---------------------------------------------------- skip_unchanged

    def _unchanged(self, record, values):
        """True when writing ``values`` would change nothing. x2many keys are
        never skippable: command lists are instructions, not state."""
        for name, new in values.items():
            field = record._fields.get(name)
            if field is None:
                return False
            if field.type in ("one2many", "many2many") or _is_x2m_commands(new):
                return False
            current = _wire_form(field.convert_to_write(record[name], record))
            if not _values_equal(current, new):
                return False
        return True

    # ------------------------------------------------------------ xml_id

    def _ensure_xml_id(self, record, xml_id):
        """Create or repair the ``ir.model.data`` row for ``record`` inside the
        record's own savepoint (an xml_id failure fails that record only)."""
        module, _dot, name = xml_id.partition(".")
        if not module or not name:
            raise UserError("xml_id %r must have the form module.name" % xml_id)
        Data = self.env["ir.model.data"]
        existing = Data.search([("module", "=", module), ("name", "=", name)], limit=1)
        if existing:
            if existing.model != record._name or existing.res_id != record.id:
                existing.write({"model": record._name, "res_id": record.id})
        else:
            Data.create({
                "module": module,
                "name": name,
                "model": record._name,
                "res_id": record.id,
                "noupdate": True,
            })
