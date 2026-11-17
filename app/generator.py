"""Gerador de dados sintéticos de pagamento, escrevendo direto no Postgres.

Modos:
  steady     fluxo normal e aleatório (pouca chance de disparar regra sem querer)
  burst      rajada no mesmo cartão -> dispara a REGRA 1 (3+ em 60 s)
  big-spend  poucas compras grandes no mesmo cartão -> dispara a REGRA 2 (> R$ 5.000 em 5 min)
  crud-demo  insert, update e delete de uma conta -> evidência da camada de ingestão
"""
import argparse
import random
import time
from datetime import datetime, timezone

from common import pg_connect

MERCHANTS = [
    ("Mercado Bom Preço", "5411"), ("Posto Ipiranga", "5541"), ("Farmácia Pague Menos", "5912"),
    ("iFood", "5814"), ("Uber", "4121"), ("Amazon BR", "5999"), ("Renner", "5651"),
    ("Cinema São Luiz", "7832"), ("Padaria Santa Clara", "5462"), ("Magazine Luiza", "5732"),
]
CITIES = ["Recife", "São Paulo", "Rio de Janeiro", "Belo Horizonte", "Salvador",
          "Fortaleza", "Curitiba", "Porto Alegre"]


def now() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def load_accounts(conn) -> list[tuple[int, str]]:
    rows = conn.execute("SELECT account_id, home_city FROM accounts ORDER BY account_id").fetchall()
    if not rows:
        raise SystemExit("Nenhuma conta. Rode sql/postgres/02_seed.sql antes.")
    return rows


def card_of(account_id: int, n: int = 1) -> str:
    return f"card-{account_id:04d}-{n}"


def insert_txn(conn, card_id, account_id, amount, city, channel=None, merchant=None):
    merchant_name, mcc = merchant or random.choice(MERCHANTS)
    channel = channel or random.choice(["pos", "ecommerce", "contactless"])
    txn_id = conn.execute(
        """INSERT INTO transactions (card_id, account_id, amount, merchant, mcc, city, channel)
           VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING txn_id""",
        (card_id, account_id, round(amount, 2), merchant_name, mcc, city, channel),
    ).fetchone()[0]
    print(f"[{now()} UTC] txn {txn_id:>6}  {card_id}  R$ {amount:>9.2f}  {city:<15} {merchant_name}")
    return txn_id


def steady(conn, rate: float, duration: int) -> None:
    accounts = load_accounts(conn)
    end = time.time() + duration
    while time.time() < end:
        account_id, home = random.choice(accounts)
        city = home if random.random() < 0.85 else random.choice(CITIES)
        insert_txn(conn, card_of(account_id, random.choice([1, 2])), account_id,
                   random.uniform(8, 400), city)
        time.sleep(random.expovariate(rate))


def pick_account(conn, card: str | None):
    if card:
        return int(card.split("-")[1]), card
    account_id, _ = random.choice(load_accounts(conn))
    return account_id, card_of(account_id, 1)


def burst(conn, card: str | None, count: int, interval: float) -> None:
    account_id, card = pick_account(conn, card)
    city = random.choice(CITIES)
    print(f"Rajada: {count} transações em {card}, uma a cada {interval}s (regra 1 espera alerta a partir da 3ª)")
    for _ in range(count):
        insert_txn(conn, card, account_id, random.uniform(50, 300), city, channel="ecommerce")
        time.sleep(interval)


def big_spend(conn, card: str | None) -> None:
    account_id, card = pick_account(conn, card)
    # Evita começar perto da virada de uma janela de 5 min, para as 3 compras caírem na mesma janela.
    secs_into_window = time.time() % 300
    if secs_into_window > 240:
        wait = 300 - secs_into_window + 2
        print(f"Aguardando {wait:.0f}s para começar numa janela de 5 min nova...")
        time.sleep(wait)
    print(f"Compras grandes em {card} (regra 2 espera soma > R$ 5.000 na janela)")
    for amount in (2150.0, 1890.0, 2300.0):
        insert_txn(conn, card, account_id, amount, "São Paulo",
                   merchant=("Magazine Luiza", "5732"), channel="ecommerce")
        time.sleep(10)


def crud_demo(conn) -> None:
    acc = conn.execute(
        """INSERT INTO accounts (holder_name, segment, home_city, risk_tier, credit_limit)
           VALUES ('Conta Demo CDC', 'standard', 'Recife', 'low', 2500) RETURNING account_id"""
    ).fetchone()[0]
    print(f"[{now()} UTC] INSERT conta {acc} (standard, Recife)")
    time.sleep(3)
    conn.execute("UPDATE accounts SET segment = 'premium', home_city = 'Olinda' WHERE account_id = %s", (acc,))
    print(f"[{now()} UTC] UPDATE conta {acc} -> premium, Olinda")
    time.sleep(3)
    conn.execute("DELETE FROM accounts WHERE account_id = %s", (acc,))
    print(f"[{now()} UTC] DELETE conta {acc}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="mode", required=True)
    p = sub.add_parser("steady"); p.add_argument("--rate", type=float, default=0.3, help="transações/s"); p.add_argument("--duration", type=int, default=300)
    p = sub.add_parser("burst"); p.add_argument("--card"); p.add_argument("--count", type=int, default=4); p.add_argument("--interval", type=float, default=8)
    p = sub.add_parser("big-spend"); p.add_argument("--card")
    sub.add_parser("crud-demo")
    args = parser.parse_args()

    with pg_connect() as conn:
        if args.mode == "steady":
            steady(conn, args.rate, args.duration)
        elif args.mode == "burst":
            burst(conn, args.card, args.count, args.interval)
        elif args.mode == "big-spend":
            big_spend(conn, args.card)
        else:
            crud_demo(conn)


if __name__ == "__main__":
    main()
