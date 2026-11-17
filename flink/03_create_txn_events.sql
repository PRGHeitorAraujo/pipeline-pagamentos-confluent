-- Stream limpo de transações, com tempo de evento e watermark próprios.
-- event_time = instante do COMMIT no Postgres (source.ts_ms), não a hora em que o Kafka recebeu.
-- Watermark com 5 s de tolerância para eventos que chegam um pouco fora de ordem.
-- 1 bucket = 1 partição: com pouco volume, partição ociosa trava a watermark.
-- No Confluent, a chave de distribuição (card_id) tem que ser a PRIMEIRA coluna do schema.
CREATE TABLE txn_events (
  card_id     STRING,
  txn_id      BIGINT,
  account_id  INT,
  amount      DOUBLE,
  merchant    STRING,
  city        STRING,
  channel     STRING,
  event_time  TIMESTAMP_LTZ(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
)
DISTRIBUTED BY HASH(card_id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'append');
