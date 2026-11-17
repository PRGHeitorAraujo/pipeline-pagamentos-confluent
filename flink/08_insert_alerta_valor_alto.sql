-- REGRA 2 (evolução): outra janela, outro critério.
-- Soma acima de R$ 5.000 no mesmo cartão dentro de uma janela fixa de 5 minutos.
-- Pega o golpe "poucas compras grandes", que a regra de velocidade não vê.
INSERT INTO fraud_alerts
  (alert_id, rule_id, card_id, account_id, trigger_txn_id, txn_count, total_amount, window_start, alert_time, severity)
SELECT
  CONCAT('AMT-', card_id, '-', DATE_FORMAT(window_start, 'yyyyMMddHHmm')),
  'HIGH_AMOUNT_5MIN',
  card_id,
  account_id,
  CAST(NULL AS BIGINT),
  COUNT(*),
  SUM(amount),
  CAST(window_start AS TIMESTAMP_LTZ(3)),
  window_time,
  'MEDIUM'
FROM TABLE(
  TUMBLE(TABLE txn_events, DESCRIPTOR(event_time), INTERVAL '5' MINUTES)
)
GROUP BY window_start, window_end, window_time, card_id, account_id
HAVING SUM(amount) > 5000;
