#!/usr/bin/env bash
# CAMADA 3 — Ingestão: prepara o Postgres, sobe o conector CDC e prova insert/update/delete virando evento.
source "$(dirname "$0")/lib.sh"
need ENV_ID; need CLUSTER_ID; need SA_WRITER; need SR_URL
E=03_ingestao.txt; : > "$EVID_DIR/$E"
confluent environment use "$ENV_ID" >/dev/null; confluent kafka cluster use "$CLUSTER_ID" >/dev/null

log "Postgres: schema, seed e publicação"
"$PY" app/pg_exec.py sql/postgres/01_schema.sql sql/postgres/02_seed.sql sql/postgres/03_replication.sql

evid_header "$E" "Pré-requisitos do banco para CDC"
evidence "$E" "$PY" app/pg_exec.py \
  --query "SHOW wal_level" \
  --query "SELECT rolname, rolreplication FROM pg_roles WHERE rolname = current_user" \
  --query "SELECT pubname, schemaname, tablename FROM pg_publication_tables WHERE pubname = 'payments_pub'" \
  --query "SELECT relname, CASE relreplident WHEN 'f' THEN 'FULL' WHEN 'd' THEN 'DEFAULT' ELSE relreplident::text END AS replica_identity FROM pg_class WHERE relname IN ('accounts','transactions')" \
  --query "SELECT count(*) AS contas FROM accounts"

log "Conector PostgresCdcSourceV2"
"$PY" app/render.py connectors/postgres-cdc.template.json > "$STATE_DIR/postgres-cdc.json"
if [ -z "${CONNECTOR_ID:-}" ]; then
  save_id CONNECTOR_ID "$(confluent connect cluster create --config-file "$STATE_DIR/postgres-cdc.json" -o json | jq -r '.id')"
fi
wait_for "conector" "confluent connect cluster describe $CONNECTOR_ID -o json | jq -r '.connector.status // .status'" "RUNNING" 90

evid_header "$E" "Conector rodando"
evidence "$E" confluent connect cluster describe "$CONNECTOR_ID"
evid_header "$E" "Tópicos criados pelo conector"
evidence "$E" confluent kafka topic list

log "Demo: INSERT, UPDATE e DELETE de uma conta"
evid_header "$E" "Operações no Postgres"
evidence "$E" "$PY" app/generator.py crud-demo
sleep 15

evid_header "$E" "Eventos no tópico pg.public.accounts (op: c=insert, u=update, d=delete)"
echo "\$ confluent kafka topic consume pg.public.accounts --from-beginning --value-format avro | grep op c/u/d" | tee -a "$EVID_DIR/$E"
timeout 45 confluent kafka topic consume pg.public.accounts --from-beginning \
  --value-format avro --api-key "$CLI_API_KEY" --api-secret "$CLI_API_SECRET" \
  --schema-registry-endpoint "$SR_URL" --schema-registry-api-key "$CLI_SR_API_KEY" \
  --schema-registry-api-secret "$CLI_SR_API_SECRET" 2>&1 | mask > "$STATE_DIR/accounts_raw.txt" || true
grep -E '"op": ?"(c|u|d)"' "$STATE_DIR/accounts_raw.txt" | tee -a "$EVID_DIR/$E" || true
warn "Se não apareceu nada, a API key pode estar propagando: rode este script de novo em 1 min."
ok "Camada 3 pronta."
