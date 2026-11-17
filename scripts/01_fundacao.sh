#!/usr/bin/env bash
# CAMADA 1 — Fundação: ambiente, cluster, Schema Registry e duas identidades (escrita e leitura).
source "$(dirname "$0")/lib.sh"
E=01_fundacao.txt; : > "$EVID_DIR/$E"

log "Login no Confluent Cloud"
confluent environment list >/dev/null 2>&1 || confluent login --save

log "Ambiente (com Stream Governance Essentials = Schema Registry incluso)"
if [ -z "${ENV_ID:-}" ]; then
  save_id ENV_ID "$(confluent environment create "$CC_ENV_NAME" --governance-package essentials -o json | jq -r .id)"
fi
confluent environment use "$ENV_ID"

log "Cluster Kafka Basic"
if [ -z "${CLUSTER_ID:-}" ]; then
  save_id CLUSTER_ID "$(confluent kafka cluster create "$CC_CLUSTER_NAME" --cloud "$CC_CLOUD" \
    --region "$CC_REGION" --type basic -o json | jq -r .id)"
fi
wait_for "cluster" "confluent kafka cluster describe $CLUSTER_ID -o json | jq -r .status" "UP"
confluent kafka cluster use "$CLUSTER_ID"
save_id BOOTSTRAP "$(confluent kafka cluster describe "$CLUSTER_ID" -o json | jq -r .endpoint | sed 's#SASL_SSL://##')"

log "Schema Registry do ambiente"
for _ in $(seq 1 30); do
  SR_JSON="$(confluent schema-registry cluster describe -o json 2>/dev/null || true)"
  SR_ID="$(echo "$SR_JSON" | jq -r '.cluster // .cluster_id // .id // empty' 2>/dev/null || true)"
  [ -n "$SR_ID" ] && break; echo "  aguardando Schema Registry..."; sleep 10
done
save_id SR_ID "$SR_ID"
save_id SR_URL "$(echo "$SR_JSON" | jq -r '.endpoint_url // .url')"

log "Service accounts: uma escreve, outra lê"
sa_id() { confluent iam service-account list -o json | jq -r --arg n "$1" '.[] | select(.name==$n) | .id'; }
[ -n "$(sa_id sa-payments-writer)" ] || confluent iam service-account create sa-payments-writer \
  --description "Escreve: conector CDC Postgres -> pg.*" >/dev/null
[ -n "$(sa_id sa-payments-reader)" ] || confluent iam service-account create sa-payments-reader \
  --description "Le: apenas fraud_alerts" >/dev/null
save_id SA_WRITER "$(sa_id sa-payments-writer)"
save_id SA_READER "$(sa_id sa-payments-reader)"

log "ACLs do escritor (só tópicos pg.* e heartbeat do Debezium)"
confluent kafka acl create --allow --service-account "$SA_WRITER" --operations describe,idempotent-write --cluster-scope
confluent kafka acl create --allow --service-account "$SA_WRITER" --operations create,write,read,describe --topic pg. --prefix
confluent kafka acl create --allow --service-account "$SA_WRITER" --operations create,write,read,describe --topic __debezium-heartbeat --prefix

log "ACLs do leitor (só lê fraud_alerts, com grupos fraud-alerts-*)"
confluent kafka acl create --allow --service-account "$SA_READER" --operations read,describe --topic fraud_alerts
confluent kafka acl create --allow --service-account "$SA_READER" --operations read --consumer-group fraud-alerts- --prefix
confluent iam rbac role-binding create --principal "User:$SA_READER" --role DeveloperRead \
  --environment "$ENV_ID" --schema-registry-cluster "$SR_ID" --resource Subject:fraud_alerts-value || \
  warn "role-binding do SR falhou (já existe?)"

log "API keys"
newkey() { confluent api-key create "$@" -o json; }
if [ -z "${READER_API_KEY:-}" ]; then
  K="$(newkey --service-account "$SA_READER" --resource "$CLUSTER_ID" --description "reader kafka")"
  save_secret READER_API_KEY "$(echo "$K" | jq -r .api_key)"; save_secret READER_API_SECRET "$(echo "$K" | jq -r .api_secret)"
fi
if [ -z "${READER_SR_API_KEY:-}" ]; then
  K="$(newkey --service-account "$SA_READER" --resource "$SR_ID" --description "reader schema registry")"
  save_secret READER_SR_API_KEY "$(echo "$K" | jq -r .api_key)"; save_secret READER_SR_API_SECRET "$(echo "$K" | jq -r .api_secret)"
fi
# Chaves do SEU usuário, só para inspecionar tópicos pela CLI (consume)
if [ -z "${CLI_API_KEY:-}" ]; then
  K="$(newkey --resource "$CLUSTER_ID" --description "cli inspecao")"
  save_secret CLI_API_KEY "$(echo "$K" | jq -r .api_key)"; save_secret CLI_API_SECRET "$(echo "$K" | jq -r .api_secret)"
fi
if [ -z "${CLI_SR_API_KEY:-}" ]; then
  K="$(newkey --resource "$SR_ID" --description "cli inspecao sr")"
  save_secret CLI_SR_API_KEY "$(echo "$K" | jq -r .api_key)"; save_secret CLI_SR_API_SECRET "$(echo "$K" | jq -r .api_secret)"
fi
confluent api-key use "$CLI_API_KEY" >/dev/null 2>&1 || confluent api-key use "$CLI_API_KEY" --resource "$CLUSTER_ID"
warn "API keys novas levam 1–2 min para propagar."

log "Evidências"
evid_header "$E" "Ambiente";            evidence "$E" confluent environment describe "$ENV_ID"
evid_header "$E" "Cluster";             evidence "$E" confluent kafka cluster describe "$CLUSTER_ID"
evid_header "$E" "Schema Registry";     evidence "$E" confluent schema-registry cluster describe
evid_header "$E" "Service accounts";    evidence "$E" confluent iam service-account list
evid_header "$E" "ACLs (quem pode o quê)"; evidence "$E" confluent kafka acl list
ok "Camada 1 pronta. IDs em .state/ids.env, segredos em .state/secrets.env (fora do Git)."
