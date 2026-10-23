from . import models  # noqa: F401 — Odoo loads models by import side effect
from .hooks import post_init_hook  # noqa: F401 — referenced by name from __manifest__
