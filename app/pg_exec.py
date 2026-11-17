"""Executa arquivos .sql ou consultas avulsas no Postgres (dispensa ter o psql instalado).

  python app/pg_exec.py sql/postgres/01_schema.sql sql/postgres/02_seed.sql
  python app/pg_exec.py --query "SHOW wal_level" --query "SELECT count(*) FROM accounts"
"""
import argparse
from pathlib import Path

from common import pg_connect


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="*")
    parser.add_argument("--query", action="append", default=[])
    args = parser.parse_args()

    with pg_connect() as conn:
        for file in args.files:
            print(f"-- executando {file}")
            conn.execute(Path(file).read_text(encoding="utf-8"))
        for sql in args.query:
            print(f"\n> {sql}")
            cur = conn.execute(sql)
            if cur.description:
                cols = [d.name for d in cur.description]
                print(" | ".join(cols))
                for row in cur.fetchall():
                    print(" | ".join(str(v) for v in row))


if __name__ == "__main__":
    main()
