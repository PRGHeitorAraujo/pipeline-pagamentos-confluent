-- Contas são DIMENSÃO: o que importa é o estado atual de cada conta.
-- Lendo o envelope Debezium como changelog, um UPDATE no Postgres vira -U/+U no Flink
-- e um DELETE vira -D. O join de enriquecimento sempre enxerga a versão mais recente.
ALTER TABLE `pg.public.accounts` SET (
  'value.format'   = 'avro-debezium-registry',
  'changelog.mode' = 'retract'
);
