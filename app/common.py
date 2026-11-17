"""Utilitários compartilhados: lê .env e .state/*.env sem depender de python-dotenv."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load_env() -> None:
    for name in (".env", ".state/ids.env", ".state/secrets.env"):
        path = ROOT / name
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def require(*keys: str) -> dict:
    load_env()
    missing = [k for k in keys if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"Variáveis faltando: {', '.join(missing)} (confira .env e rode os scripts na ordem)")
    return {k: os.environ[k] for k in keys}


def pg_connect():
    import psycopg

    env = require("PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASSWORD")
    return psycopg.connect(
        host=env["PG_HOST"],
        port=int(env["PG_PORT"]),
        dbname=env["PG_DB"],
        user=env["PG_USER"],
        password=env["PG_PASSWORD"],
        sslmode="require",
        autocommit=True,
    )
