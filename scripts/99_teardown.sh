#!/usr/bin/env bash
# Derruba TUDO, na ordem certa. Ordem importa: Flink -> conector -> slot no Postgres -> cluster -> ambiente.
source "$(dirname "$0")/lib.sh"
read -r -p "Isso apaga o pipeline inteiro no Confluent Cloud e o slot no Postgres. Digite 'derrubar': " c
[ "$c" = "derrubar" ] || { echo "cancelado"; exit 1; }

if [ -n "${POOL_ID:-}" ]; then
  log "Statements do Flink"
  for s in $(confluent flink statement list --compute-pool "$POOL_ID" --environment "$ENV_ID" -o json | jq -r '.[].name'); do
    confluent flink statement delete "$s" --environment "$ENV_ID" --force || true
  done
  log "Compute pool"; confluent flink compute-pool delete "$POOL_ID" --environment "$ENV_ID" --force || true
fi

if [ -n "${CONNECTOR_ID:-}" ]; then
  log "Conector CDC"; confluent connect cluster delete "$CONNECTOR_ID" --cluster "$CLUSTER_ID" --environment "$ENV_ID" --force || true
  sleep 20
fi

log "Postgres: remove slot de replicação e publicação"
"$PY" app/pg_exec.py sql/postgres/99_teardown.sql \
  --query "SELECT slot_name, active FROM pg_replication_slots" || warn "rode sql/postgres/99_teardown.sql manualmente"

log "API keys"
for k in "${READER_API_KEY:-}" "${READER_SR_API_KEY:-}" "${CLI_API_KEY:-}" "${CLI_SR_API_KEY:-}" "${CLOUD_API_KEY:-}"; do
  [ -n "$k" ] && confluent api-key delete "$k" --force || true
done

[ -n "${CLUSTER_ID:-}" ] && { log "Cluster"; confluent kafka cluster delete "$CLUSTER_ID" --environment "$ENV_ID" --force || true; }
for sa in "${SA_WRITER:-}" "${SA_READER:-}"; do
  [ -n "$sa" ] && confluent iam service-account delete "$sa" --force || true
done
[ -n "${ENV_ID:-}" ] && { log "Ambiente"; confluent environment delete "$ENV_ID" --force || true; }

: > "$STATE_DIR/ids.env"; : > "$STATE_DIR/secrets.env"
ok "Tudo derrubado. Confira em Console > Billing que nada continua cobrando."
warn "No Neon: se não for mais usar, apague o projeto (ou ao menos confirme que não sobrou slot)."
