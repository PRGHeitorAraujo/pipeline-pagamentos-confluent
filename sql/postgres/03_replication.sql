-- Pré-requisito: replicação lógica habilitada no Neon
-- (Console > Settings > Logical replication > Enable). Isso muda wal_level para "logical".
--
-- A publicação diz QUAIS tabelas entram no fluxo de replicação.
-- O slot (payments_slot) é criado pelo próprio conector na primeira subida.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'payments_pub') THEN
        CREATE PUBLICATION payments_pub FOR TABLE public.accounts, public.transactions;
    END IF;
END
$$;
