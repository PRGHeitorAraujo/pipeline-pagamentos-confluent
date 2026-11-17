-- Rode DEPOIS de apagar o conector (slot ativo não pode ser removido).
-- Um slot esquecido segura WAL para sempre: o disco cresce e o Neon não deixa a compute dormir.
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE slot_name = 'payments_slot' AND NOT active;

DROP PUBLICATION IF EXISTS payments_pub;
