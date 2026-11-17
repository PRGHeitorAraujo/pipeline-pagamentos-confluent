-- Consultas para rodar no Flink SQL Workspace (ou `confluent flink shell`) e tirar print.
-- Rode UMA por vez.

-- 1) Envelope cru do CDC de transações: op = 'c' (insert), 'u' (update), 'd' (delete), 'r' (snapshot)
SELECT `op`, `after`.txn_id, `after`.card_id, `after`.amount, `source`.ts_ms
FROM `pg.public.transactions`;

-- 2) Contas como changelog: rode o crud-demo do gerador e veja +I, -U/+U e -D aparecendo
SELECT * FROM `pg.public.accounts`;

-- 3) Stream limpo com tempo de evento
SELECT * FROM txn_events;

-- 4) Transação enriquecida com dados da conta
SELECT txn_id, card_id, amount, city, holder_name, segment, home_city, out_of_home_city
FROM transactions_enriched;

-- 5) Alertas
SELECT * FROM fraud_alerts;

-- 6) Alertas por regra
SELECT rule_id, severity, COUNT(*) AS alertas
FROM fraud_alerts
GROUP BY rule_id, severity;
