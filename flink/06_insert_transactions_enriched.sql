-- Join regular (não temporal) com a dimensão de contas.
-- Por que não temporal join? Ele espera a watermark da tabela de contas avançar, e contas
-- quase não mudam: o resultado ficaria travado. Com 50 contas, o estado do join regular é pequeno.
-- O CAST tira o atributo de tempo de event_time (Flink não aceita rowtime em join regular).
INSERT INTO transactions_enriched
SELECT
  t.txn_id,
  t.card_id,
  t.account_id,
  t.amount,
  t.merchant,
  t.city,
  t.channel,
  t.event_time,
  a.holder_name,
  a.segment,
  a.home_city,
  a.risk_tier,
  CASE WHEN a.home_city IS NULL THEN CAST(NULL AS BOOLEAN) ELSE t.city <> a.home_city END
FROM (
  SELECT txn_id, card_id, account_id, amount, merchant, city, channel,
         CAST(event_time AS TIMESTAMP_LTZ(3)) AS event_time
  FROM txn_events
) AS t
LEFT JOIN `pg.public.accounts` AS a
  ON t.account_id = a.account_id;
