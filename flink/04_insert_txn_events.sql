INSERT INTO txn_events (txn_id, card_id, account_id, amount, merchant, city, channel, event_time)
SELECT
  `after`.txn_id,
  `after`.card_id,
  `after`.account_id,
  `after`.amount,
  `after`.merchant,
  `after`.city,
  `after`.channel,
  TO_TIMESTAMP_LTZ(`source`.ts_ms, 3) AS event_time
FROM `pg.public.transactions`
WHERE `op` = 'c';
