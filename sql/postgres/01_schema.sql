-- Camada 0: o banco de origem. Duas tabelas: a dimensão (contas) e o fato (transações).

CREATE TABLE IF NOT EXISTS accounts (
    account_id    SERIAL PRIMARY KEY,
    holder_name   TEXT           NOT NULL,
    segment       TEXT           NOT NULL CHECK (segment IN ('standard', 'premium', 'business')),
    home_city     TEXT           NOT NULL,
    risk_tier     TEXT           NOT NULL DEFAULT 'low' CHECK (risk_tier IN ('low', 'medium', 'high')),
    credit_limit  NUMERIC(12, 2) NOT NULL,
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
    txn_id      BIGSERIAL PRIMARY KEY,
    card_id     TEXT           NOT NULL,
    account_id  INT            NOT NULL REFERENCES accounts (account_id),
    amount      NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    merchant    TEXT           NOT NULL,
    mcc         TEXT           NOT NULL,
    city        TEXT           NOT NULL,
    channel     TEXT           NOT NULL CHECK (channel IN ('pos', 'ecommerce', 'contactless')),
    status      TEXT           NOT NULL DEFAULT 'approved',
    created_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_transactions_card_time ON transactions (card_id, created_at);

-- REPLICA IDENTITY FULL: no UPDATE/DELETE o WAL carrega a linha inteira de "antes".
-- Sem isso o evento de update chega com "before" vazio e o de delete só com a PK.
ALTER TABLE accounts     REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;

CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_accounts_updated_at ON accounts;
CREATE TRIGGER trg_accounts_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
