#!/usr/bin/env bash
# CAMADA 4 — Processamento: Flink SQL enriquece e aplica as regras de fraude; depois gera tráfego e consome os alertas.
source "$(dirname "$0")/lib.sh"
need ENV_ID; need CLUSTER_ID
E=04_processamento.txt; : > "$EVID_DIR/$E"
confluent environment use "$ENV_ID" >/dev/null

log "Compute pool do Flink"
if [ -z "${POOL_ID:-}" ]; then
  save_id POOL_ID "$(confluent flink compute-pool create "$CC_POOL_NAME" --cloud "$CC_CLOUD" \
    --region "$CC_REGION" --max-cfu "$CC_POOL_MAX_CFU" -o json | jq -r .id)"
fi
wait_for "compute pool" "confluent flink compute-pool describe $POOL_ID -o json | jq -r .status" "PROVISIONED" 60

run_stmt() { # nome arquivo
  if confluent flink statement describe "$1" --environment "$ENV_ID" >/dev/null 2>&1; then
    warn "statement $1 já existe, pulando"; return 0
  fi
  local sql; sql="$(grep -v '^[[:space:]]*--' "$2")"
  log "Flink: $1  ($2)"
  confluent flink statement create "$1" --sql "$sql" --compute-pool "$POOL_ID" \
    --environment "$ENV_ID" --database "$CLUSTER_ID" --wait
}
run_stmt ddl-alter-accounts          flink/01_alter_accounts.sql
run_stmt ddl-alter-transactions      flink/02_alter_transactions.sql
run_stmt ddl-create-txn-events       flink/03_create_txn_events.sql
run_stmt job-txn-events              flink/04_insert_txn_events.sql
run_stmt ddl-create-enriched         flink/05_create_transactions_enriched.sql
run_stmt job-enriquecimento          flink/06_insert_transactions_enriched.sql
run_stmt job-regra-velocidade        flink/07_insert_alerta_velocidade.sql
run_stmt job-regra-valor-alto        flink/08_insert_alerta_valor_alto.sql

evid_header "$E" "Statements do Flink"
evidence "$E" confluent flink statement list --compute-pool "$POOL_ID" --environment "$ENV_ID"

log "Gerando tráfego: fluxo normal em segundo plano + rajada (regra 1) + compras grandes (regra 2)"
: > "$EVID_DIR/04_trafego.txt"
"$PY" app/generator.py steady --rate 0.3 --duration 420 >> "$EVID_DIR/04_trafego.txt" 2>&1 &
STEADY_PID=$!
sleep 20
evid_header "$E" "Rajada no mesmo cartão (4 transações, 8 s entre elas)"
evidence "$E" "$PY" app/generator.py burst --card card-0007-1 --count 4 --interval 8
evid_header "$E" "Compras grandes no mesmo cartão"
evidence "$E" "$PY" app/generator.py big-spend --card card-0012-1

log "Consumindo fraud_alerts com a identidade de LEITURA (espera a janela de 5 min fechar)"
evid_header "$E" "Alertas consumidos por sa-payments-reader"
evidence "$E" "$PY" app/alert_consumer.py --duration 360
wait "$STEADY_PID" 2>/dev/null || true
ok "Camada 4 pronta. Tire prints das consultas de flink/90_consultas_verificacao.sql no Workspace."
