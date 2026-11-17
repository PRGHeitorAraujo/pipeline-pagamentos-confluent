-- Saída do enriquecimento: transação + dados da conta.
-- upsert por txn_id: se a conta mudar, a linha da transação é corrigida no lugar.
CREATE TABLE transactions_enriched (
  txn_id            BIGINT NOT NULL,
  card_id           STRING,
  account_id        INT,
  amount            DOUBLE,
  merchant          STRING,
  city              STRING,
  channel           STRING,
  event_time        TIMESTAMP_LTZ(3),
  holder_name       STRING,
  segment           STRING,
  home_city         STRING,
  risk_tier         STRING,
  out_of_home_city  BOOLEAN,
  PRIMARY KEY (txn_id) NOT ENFORCED
)
DISTRIBUTED BY HASH(txn_id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert');
