-- REGRA 1: 3 ou mais transações no MESMO cartão em 60 segundos = alerta.
-- OVER com RANGE de 60 s é uma janela deslizante "olhando para trás" a partir de cada transação:
-- toda transação nova conta quantas o mesmo cartão teve no último minuto.
-- (Uma janela fixa de 1 min perderia a rajada que cruza a virada do minuto.)
INSERT INTO fraud_alerts
  (alert_id, rule_id, card_id, account_id, trigger_txn_id, txn_count, total_amount, window_start, alert_time, severity)
SELECT
  CONCAT('VEL-', CAST(txn_id AS STRING)),
  'VELOCITY_3_IN_60S',
  card_id,
  account_id,
  txn_id,
  txn_count,
  total_amount,
  window_start,
  event_time,
  'HIGH'
FROM (
  SELECT
    txn_id, card_id, account_id, event_time,
    COUNT(*)        OVER w60 AS txn_count,
    SUM(amount)     OVER w60 AS total_amount,
    MIN(event_time) OVER w60 AS window_start
  FROM txn_events
  WINDOW w60 AS (
    PARTITION BY card_id
    ORDER BY event_time
    RANGE BETWEEN INTERVAL '60' SECOND PRECEDING AND CURRENT ROW
  )
)
WHERE txn_count >= 3;
