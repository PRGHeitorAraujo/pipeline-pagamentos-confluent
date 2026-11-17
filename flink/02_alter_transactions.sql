-- Transações são FATO imutável: uma compra aconteceu e pronto.
-- Lemos o envelope cru (before/after/op/source) como append-only, que é o que
-- janelas de tempo (OVER, TUMBLE) exigem. A próxima etapa filtra só op = 'c' (insert).
ALTER TABLE `pg.public.transactions` SET (
  'value.format'   = 'avro-registry',
  'changelog.mode' = 'append'
);
