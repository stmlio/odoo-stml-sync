"""Addon unit tests for ``stml.sync``.

Runs on Odoo 16, 18 and 19:

    odoo -d stml_sync_test -i base,stml_sync --stop-after-init --without-demo=all
    odoo -d stml_sync_test -u stml_sync --test-tags /stml_sync --stop-after-init

``TransactionCase`` runs as the superuser by default, which bypasses ACLs and
record rules; every call here goes through ``with_user`` so the tests exercise
the module the way an RPC caller hits it.
"""

import ast
import json
import os
import uuid
from datetime import timedelta

import psycopg2
from unittest.mock import patch

from odoo import SUPERUSER_ID, fields
from odoo.exceptions import UserError
from odoo.tests.common import TransactionCase, tagged
from odoo.tools import mute_logger

from ..models.stml_sync import MODULE_VERSION


def _op():
    return str(uuid.uuid4())


@tagged("post_install", "-at_install", "stml_sync")
class TestStmlSync(TransactionCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Users = cls.env["res.users"]
        groups_field = "group_ids" if "group_ids" in Users._fields else "groups_id"
        cls.admin = cls.env.ref("base.user_admin")
        cls.bob = Users.create({
            "name": "Sync Bob",
            "login": "stml_sync_bob",
            groups_field: [(6, 0, [cls.env.ref("base.group_user").id])],
        })
        cls.sync_admin = cls.env["stml.sync"].with_user(cls.admin)
        cls.sync_bob = cls.env["stml.sync"].with_user(cls.bob)
        cls.country = cls.env.ref("base.nl")

    # ------------------------------------------------------------ helpers

    def _upsert(self, sync, model, records, **options):
        options.setdefault("operation_id", _op())
        return sync.batch_upsert(model=model, records=records, options=options)

    def _backdate(self, table, rec_id, column, days):
        self.env.flush_all()  # pending ORM writes would re-stamp the row after our UPDATE
        self.env.cr.execute(
            "UPDATE %s SET %s = %%s WHERE id = %%s" % (table, column),
            (fields.Datetime.now() - timedelta(days=days), rec_id),
        )
        self.env.invalidate_all()

    # --------------------------------------------------------------- M0

    def test_ping(self):
        res = self.sync_bob.ping()
        self.assertEqual(res["module_version"], MODULE_VERSION)
        self.assertTrue(res["odoo_version"])
        self.assertEqual(
            set(res["capabilities"]), {"batch_upsert", "skip_unchanged", "xml_id", "idempotency"}
        )

    def test_manifest_version_matches_constant(self):
        path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "__manifest__.py")
        with open(path, encoding="utf-8") as fh:
            manifest = ast.literal_eval(fh.read())
        self.assertEqual(manifest["version"], MODULE_VERSION)

    # -------------------------------------------------- caps / validation

    def test_cap_1000_records_rejected(self):
        p = self.env["res.partner"].create({"name": "cap"})
        records = [{"index": i, "id": p.id, "values": {"name": "cap"}} for i in range(1001)]
        with self.assertRaises(UserError):
            self._upsert(self.sync_admin, "res.partner", records)
        self.assertFalse(self.env["stml.sync.operation"].search([("target_model", "=", "res.partner")]))
        # Exactly 1000 is fine.
        res = self._upsert(self.sync_admin, "res.partner", records[:1000], skip_unchanged=True)
        self.assertEqual(len(res["results"]), 1000)
        self.assertEqual({r["action"] for r in res["results"]}, {"skipped"})

    def test_operation_id_required(self):
        with self.assertRaises(UserError):
            self.sync_admin.batch_upsert(model="res.partner", records=[], options={})
        with self.assertRaises(UserError):
            self.sync_admin.batch_upsert(model="res.partner", records=[])

    def test_unknown_or_abstract_model_rejected(self):
        with self.assertRaises(UserError):
            self._upsert(self.sync_admin, "no.such.model", [])
        with self.assertRaises(UserError):
            self._upsert(self.sync_admin, "stml.sync", [])

    # ------------------------------------------------- M1: isolation

    @mute_logger("odoo.sql_db")
    def test_poisoned_record_isolation(self):
        tag = "stml_sync poison %s" % _op()
        records = [{"index": i, "id": None, "values": {"name": "%s %d" % (tag, i)}} for i in range(6)]
        # 2: dangling many2one → FK violation at flush (IntegrityError)
        records[2]["values"]["industry_id"] = 2**30
        # 4: contact without a name → CHECK constraint (IntegrityError)
        records[4]["values"] = {"type": "contact", "comment": tag}
        res = self._upsert(self.sync_admin, "res.partner", records)
        self.assertFalse(res["replayed"])
        by_index = {r["index"]: r for r in res["results"]}
        self.assertEqual(sorted(by_index), [0, 1, 2, 3, 4, 5])
        for i in (0, 1, 3, 5):
            self.assertEqual(by_index[i]["action"], "created", by_index[i])
            self.assertTrue(self.env["res.partner"].browse(by_index[i]["id"]).exists())
        for i in (2, 4):
            self.assertEqual(by_index[i]["action"], "error")
            self.assertEqual(by_index[i]["error_class"], "IntegrityError", by_index[i])
            self.assertIsNone(by_index[i]["id"])
            self.assertTrue(by_index[i]["error"])
        landed = self.env["res.partner"].search_count([("name", "ilike", tag)])
        self.assertEqual(landed, 4)
        # The transaction is still usable after the rolled-back rows.
        self.env["res.partner"].create({"name": tag + " after"})

    def test_clean_creates_go_through_one_batched_create(self):
        Partner = type(self.env["res.partner"])
        calls = []
        real = Partner.create

        def spy(model, vals_list):
            calls.append(vals_list)
            return real(model, vals_list)

        with patch.object(Partner, "create", spy):
            res = self._upsert(self.sync_admin, "res.partner", [
                {"index": i, "id": None, "values": {"name": "stml_sync batched %d" % i},
                 "xml_id": "stml_t.batched_%d" % i} for i in range(5)])
        self.assertEqual([r["action"] for r in res["results"]], ["created"] * 5)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(calls[0]), 5)
        self.assertEqual(self.env.ref("stml_t.batched_3").id, res["results"][3]["id"])

    @mute_logger("odoo.sql_db")
    def test_bad_rows_cost_a_bisection_not_a_row_each(self):
        tag = "stml_sync bisect %s" % _op()
        n = 64
        records = [{"index": i, "id": None, "values": {"name": "%s %d" % (tag, i)}} for i in range(n)]
        for bad in (10, 50):
            records[bad]["values"]["industry_id"] = 2**30  # FK violation at flush
        Partner = type(self.env["res.partner"])
        calls = []
        real = Partner.create

        def spy(model, vals_list):
            calls.append(len(vals_list))
            return real(model, vals_list)

        with patch.object(Partner, "create", spy):
            res = self._upsert(self.sync_admin, "res.partner", records)
        by_index = {r["index"]: r for r in res["results"]}
        self.assertEqual(sum(1 for r in by_index.values() if r["action"] == "created"), n - 2)
        for bad in (10, 50):
            self.assertEqual(by_index[bad]["action"], "error")
            self.assertEqual(by_index[bad]["error_class"], "IntegrityError")
        self.assertEqual(self.env["res.partner"].search_count([("name", "ilike", tag)]), n - 2)
        # 2 bad rows in 64: about 2 * log2(64) batched attempts plus the
        # single-record leaves, far fewer than one create per row.
        self.assertLess(len(calls), n // 2, calls)

    @mute_logger("odoo.sql_db")
    def test_error_taxonomy(self):
        p = self.env["res.partner"].create({"name": "stml_sync taxonomy"})
        records = [
            # ValidationError: recursive hierarchy (python constraint)
            {"index": "val", "id": p.id, "values": {"parent_id": p.id}},
            # MissingError: unknown id
            {"index": "missing", "id": 2**30, "values": {"name": "ghost"}},
            # Other: invalid field name
            {"index": "other", "id": None, "values": {"name": "x", "no_such_field_xyz": 1}},
            # IntegrityError: CHECK constraint
            {"index": "integrity", "id": None, "values": {"type": "contact"}},
            # UserError: malformed record payload
            {"index": "user", "id": "not-an-int", "values": {"name": "x"}},
            # a good one, after all the bad ones
            {"index": "ok", "id": p.id, "values": {"name": "stml_sync taxonomy 2"}},
        ]
        res = self._upsert(self.sync_admin, "res.partner", records)
        by_index = {r["index"]: r for r in res["results"]}
        self.assertEqual(by_index["val"]["error_class"], "ValidationError", by_index["val"])
        self.assertEqual(by_index["missing"]["error_class"], "MissingError", by_index["missing"])
        self.assertEqual(by_index["missing"]["id"], 2**30)
        self.assertEqual(by_index["other"]["error_class"], "Other", by_index["other"])
        self.assertEqual(by_index["integrity"]["error_class"], "IntegrityError", by_index["integrity"])
        self.assertEqual(by_index["user"]["error_class"], "UserError", by_index["user"])
        self.assertEqual(by_index["ok"]["action"], "updated")
        self.assertEqual(p.name, "stml_sync taxonomy 2")
        self.assertFalse(p.parent_id)

    def test_error_text_truncated_to_2000(self):
        # A 5000-char invalid value for a selection field echoes the value
        # back in Odoo's error text; the result must be capped.
        res = self._upsert(
            self.sync_admin, "res.partner",
            [{"index": 0, "id": None, "values": {"name": "x", "type": "z" * 5000}}],
        )
        r = res["results"][0]
        self.assertEqual(r["action"], "error")
        self.assertLessEqual(len(r["error"]), 2000)

    # ------------------------------------------------- M1: ACL as non-admin

    def test_acl_enforced_per_row_as_non_admin(self):
        records = [
            # own user, self-writeable field → allowed
            {"index": 0, "id": self.bob.id, "values": {"tz": "Europe/Amsterdam"}},
            # someone else's user → AccessError
            {"index": 1, "id": self.admin.id, "values": {"tz": "Europe/Amsterdam"}},
            # creating a user → AccessError
            {"index": 2, "id": None, "values": {"name": "Eve", "login": "stml_sync_eve"}},
        ]
        res = self._upsert(self.sync_bob, "res.users", records)
        by_index = {r["index"]: r for r in res["results"]}
        self.assertEqual(by_index[0]["action"], "updated", by_index[0])
        self.assertEqual(by_index[1]["action"], "error")
        self.assertEqual(by_index[1]["error_class"], "AccessError", by_index[1])
        self.assertEqual(by_index[2]["action"], "error")
        self.assertEqual(by_index[2]["error_class"], "AccessError", by_index[2])
        self.assertEqual(self.bob.tz, "Europe/Amsterdam")
        self.assertFalse(self.env["res.users"].search([("login", "=", "stml_sync_eve")]))

    def test_model_without_write_access_errors_per_row_and_survives(self):
        eur = self.env.ref("base.EUR")
        res = self._upsert(
            self.sync_bob, "res.currency",
            [{"index": 0, "id": eur.id, "values": {"symbol": "€€"}},
             {"index": 1, "id": None, "values": {"name": "ZZZ", "symbol": "Z"}}],
        )
        self.assertEqual([r["action"] for r in res["results"]], ["error", "error"])
        self.assertEqual({r["error_class"] for r in res["results"]}, {"AccessError"})
        self.assertNotEqual(eur.symbol, "€€")

    # ------------------------------------------------- M1: xml_id

    def test_xml_id_create_and_repair(self):
        Data = self.env["ir.model.data"]
        res = self._upsert(
            self.sync_admin, "res.partner",
            [{"index": 0, "id": None, "values": {"name": "xmlid one"}, "xml_id": "stml_t.partner_1"}],
        )
        first_id = res["results"][0]["id"]
        self.assertEqual(res["results"][0]["action"], "created")
        row = Data.search([("module", "=", "stml_t"), ("name", "=", "partner_1")])
        self.assertEqual(len(row), 1)
        self.assertEqual((row.model, row.res_id, row.noupdate), ("res.partner", first_id, True))

        # Repair: the same xml_id now claims a different record on update.
        other = self.env["res.partner"].create({"name": "xmlid two"})
        res = self._upsert(
            self.sync_admin, "res.partner",
            [{"index": 0, "id": other.id, "values": {"name": "xmlid two b"}, "xml_id": "stml_t.partner_1"}],
        )
        self.assertEqual(res["results"][0]["action"], "updated")
        row = Data.search([("module", "=", "stml_t"), ("name", "=", "partner_1")])
        self.assertEqual(len(row), 1)
        self.assertEqual(row.res_id, other.id)
        self.assertEqual(self.env.ref("stml_t.partner_1").id, other.id)

    def test_xml_id_failure_fails_only_that_record(self):
        tag = "xmlid fail %s" % _op()
        res = self._upsert(
            self.sync_admin, "res.partner",
            [{"index": 0, "id": None, "values": {"name": tag + " a"}, "xml_id": "no_dot_here"},
             {"index": 1, "id": None, "values": {"name": tag + " b"}, "xml_id": "stml_t.ok"}],
        )
        self.assertEqual(res["results"][0]["action"], "error")
        self.assertEqual(res["results"][0]["error_class"], "UserError")
        self.assertEqual(res["results"][1]["action"], "created")
        # The record that failed on its xml_id was rolled back with it.
        self.assertEqual(self.env["res.partner"].search_count([("name", "=", tag + " a")]), 0)
        self.assertEqual(self.env["res.partner"].search_count([("name", "=", tag + " b")]), 1)

    # ------------------------------------------------- M1: ledger / replay

    def test_replay_returns_stored_result_without_re_executing(self):
        op = _op()
        tag = "replay %s" % op
        records = [{"index": 7, "id": None, "values": {"name": tag}}]
        first = self.sync_admin.batch_upsert(model="res.partner", records=records, options={"operation_id": op})
        self.assertFalse(first["replayed"])
        second = self.sync_admin.batch_upsert(model="res.partner", records=records, options={"operation_id": op})
        self.assertTrue(second["replayed"])
        self.assertEqual(second["results"], first["results"])
        self.assertEqual(self.env["res.partner"].search_count([("name", "=", tag)]), 1)
        # Ledger row belongs to the caller and holds results only — no values.
        ledger = self.env["stml.sync.operation"].search([("name", "=", op)])
        self.assertEqual(len(ledger), 1)
        self.assertEqual(ledger.user_id, self.admin)
        self.assertEqual(ledger.target_model, "res.partner")
        self.assertEqual(ledger.record_count, 1)
        self.assertNotIn(tag, ledger.result_json)
        self.assertNotIn("values", json.dumps(json.loads(ledger.result_json)))

    def test_ledger_record_rule_hides_other_users_operations(self):
        op = _op()
        self._upsert(self.sync_admin, "res.partner", [{"index": 0, "id": None, "values": {"name": "rr"}}],
                     operation_id=op)
        Ledger = self.env["stml.sync.operation"]
        self.assertTrue(Ledger.with_user(self.admin).search([("name", "=", op)]))
        self.assertFalse(Ledger.with_user(self.bob).search([("name", "=", op)]))

    @mute_logger("odoo.sql_db")
    def test_unique_constraint_aborts_duplicate_operation(self):
        """Another user reusing an operation id cannot see the ledger row
        (record rule) so they execute — and the unique constraint then aborts
        their request instead of leaving two ledger rows."""
        op = _op()
        self._upsert(self.sync_admin, "res.partner", [{"index": 0, "id": None, "values": {"name": "uq a"}}],
                     operation_id=op)
        with self.assertRaises(psycopg2.IntegrityError), self.env.cr.savepoint():
            self._upsert(self.sync_bob, "res.partner",
                         [{"index": 0, "id": None, "values": {"name": "uq b"}}], operation_id=op)
        self.assertEqual(self.env["stml.sync.operation"].search_count([("name", "=", op)]), 1)
        self.assertEqual(self.env["res.partner"].search_count([("name", "=", "uq b")]), 0)

    def test_gc_cron_purges_rows_older_than_7_days(self):
        old_op, new_op = _op(), _op()
        self._upsert(self.sync_admin, "res.partner", [], operation_id=old_op)
        self._upsert(self.sync_bob, "res.partner", [], operation_id=new_op)
        Ledger = self.env["stml.sync.operation"]
        old_row = Ledger.search([("name", "=", old_op)])
        self._backdate("stml_sync_operation", old_row.id, "create_date", 8)
        # The cron runs as the system user (bypasses the per-user rule).
        cron = self.env.ref("stml_sync.ir_cron_gc_operations")
        self.assertEqual(cron.user_id.id, SUPERUSER_ID)
        self.assertEqual(cron.interval_type, "days")
        self.assertEqual(cron.interval_number, 1)
        if "numbercall" in cron._fields:  # Odoo 16: must not deactivate after one run
            self.assertEqual(cron.numbercall, -1)
        purged = Ledger.with_user(SUPERUSER_ID)._gc_operations()
        self.assertEqual(purged, 1)
        self.assertFalse(Ledger.search([("name", "=", old_op)]))
        self.assertTrue(Ledger.with_user(SUPERUSER_ID).search([("name", "=", new_op)]))

    # ------------------------------------------------- M2: skip_unchanged

    def _partner(self):
        return self.env["res.partner"].create({
            "name": "skip me",
            "email": "skip@example.com",
            "country_id": self.country.id,
            "partner_latitude": 52.5,
            "comment": False,
            "is_company": False,
        })

    def test_skip_unchanged_matrix(self):
        p = self._partner()
        cat = self.env["res.partner.category"].create({"name": "skip cat"})
        p.write({"category_id": [(6, 0, [cat.id])]})
        cases = {
            "m2o_id": {"country_id": self.country.id},
            "empty_equivalence_none": {"comment": None},
            "empty_equivalence_empty_string": {"comment": ""},
            "empty_equivalence_false": {"comment": False},
            "numeric_string": {"partner_latitude": "52.5"},
            "numeric_int_for_float": {"partner_latitude": 52.5},
            "bool": {"is_company": False},
            "same_strings": {"name": "skip me", "email": "skip@example.com"},
        }
        records = [{"index": k, "id": p.id, "values": v} for k, v in cases.items()]
        res = self._upsert(self.sync_admin, "res.partner", records, skip_unchanged=True)
        self.assertEqual({r["index"]: r["action"] for r in res["results"]},
                         {k: "skipped" for k in cases})

        changed = {
            "m2o_other": {"country_id": self.env.ref("base.be").id},
            "numeric_differs": {"partner_latitude": "52.6"},
            "string_differs": {"name": "skip me 2"},
            "command_list_same_set_never_skipped": {"category_id": [(6, 0, [cat.id])]},
            "command_list_additive_never_skipped": {"category_id": [(4, cat.id)]},
            "bool_flip": {"is_company": True},
        }
        records = [{"index": k, "id": p.id, "values": v} for k, v in changed.items()]
        res = self._upsert(self.sync_admin, "res.partner", records, skip_unchanged=True)
        self.assertEqual({r["index"]: r["action"] for r in res["results"]},
                         {k: "updated" for k in changed})

    def test_skip_unchanged_date_field(self):
        eur = self.env.ref("base.EUR")
        rate = self.env["res.currency.rate"].create({
            "currency_id": eur.id, "name": "2024-01-15", "rate": 1.0,
        })
        res = self._upsert(
            self.sync_admin, "res.currency.rate",
            [{"index": 0, "id": rate.id, "values": {"name": "2024-01-15", "rate": "1.0"}}],
            skip_unchanged=True,
        )
        self.assertEqual(res["results"][0]["action"], "skipped")

    def test_skip_unchanged_ignores_creates(self):
        res = self._upsert(
            self.sync_admin, "res.partner",
            [{"index": 0, "id": None, "values": {"name": "create under skip"}}],
            skip_unchanged=True,
        )
        self.assertEqual(res["results"][0]["action"], "created")

    def test_zero_change_batch_leaves_write_date_alone(self):
        partners = [self._partner() for _ in range(5)]
        for p in partners:
            self._backdate("res_partner", p.id, "write_date", 30)
        before = {p.id: p.write_date for p in partners}
        self.assertTrue(all(before.values()))

        records = [
            {"index": i, "id": p.id,
             "values": {"name": "skip me", "email": "skip@example.com", "country_id": self.country.id,
                        "partner_latitude": "52.5", "comment": ""}}
            for i, p in enumerate(partners)
        ]
        res = self._upsert(self.sync_admin, "res.partner", records, skip_unchanged=True)
        self.assertEqual({r["action"] for r in res["results"]}, {"skipped"})
        self.env.invalidate_all()
        self.assertEqual({p.id: p.write_date for p in partners}, before)

        # Without the option the same batch writes every row and write_date moves.
        res = self._upsert(self.sync_admin, "res.partner", records)
        self.assertEqual({r["action"] for r in res["results"]}, {"updated"})
        self.env.invalidate_all()
        for p in partners:
            self.assertGreater(p.write_date, before[p.id])
