#!/usr/bin/env bash
# CAMADA 5 — Operação: métricas, acesso e custo do que subiu.
source "$(dirname "$0")/lib.sh"
need ENV_ID; need CLUSTER_ID
E=05_operacao.txt; : > "$EVID_DIR/$E"
C=05_custo.txt;    : > "$EVID_DIR/$C"
confluent environment use "$ENV_ID" >/dev/null; confluent kafka cluster use "$CLUSTER_ID" >/dev/null

log "Saúde dos componentes"
evid_header "$E" "Conector";        evidence "$E" confluent connect cluster describe "$CONNECTOR_ID"
evid_header "$E" "Flink statements"; evidence "$E" confluent flink statement list --compute-pool "$POOL_ID" --environment "$ENV_ID"
evid_header "$E" "Compute pool";    evidence "$E" confluent flink compute-pool describe "$POOL_ID"
evid_header "$E" "Tópicos";         evidence "$E" confluent kafka topic list
evid_header "$E" "Lag do consumidor de alertas"
evidence "$E" confluent kafka consumer group lag list fraud-alerts-consumer || true

log "Acesso: quem pode o quê"
evid_header "$E" "Service accounts"; evidence "$E" confluent iam service-account list
evid_header "$E" "ACLs";             evidence "$E" confluent kafka acl list
evid_header "$E" "API keys (sem segredos)"; evidence "$E" confluent api-key list
evid_header "$E" "Role bindings do leitor"
evidence "$E" confluent iam rbac role-binding list --principal "User:$SA_READER" --environment "$ENV_ID" || true

log "Métricas (Metrics API) — precisa de uma Cloud API key"
if [ -z "${CLOUD_API_KEY:-}" ]; then
  K="$(confluent api-key create --resource cloud --description "metrics api" -o json)"
  save_secret CLOUD_API_KEY "$(echo "$K" | jq -r .api_key)"; save_secret CLOUD_API_SECRET "$(echo "$K" | jq -r .api_secret)"
  warn "Cloud API key criada; esperando 60 s para propagar"; sleep 60
fi
START="$(date -u -d '-3 hours' '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -v-3H '+%Y-%m-%dT%H:%M:%SZ')"
END="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
for metric in received_records sent_records received_bytes retained_bytes; do
  evid_header "$E" "Métrica io.confluent.kafka.server/$metric por tópico (últimas 3 h)"
  curl -s -u "$CLOUD_API_KEY:$CLOUD_API_SECRET" -H 'Content-Type: application/json' \
    https://api.telemetry.confluent.cloud/v2/metrics/cloud/query -d @- <<JSON | jq . | mask | tee -a "$EVID_DIR/$E"
{"aggregations":[{"metric":"io.confluent.kafka.server/$metric"}],
 "filter":{"field":"resource.kafka.id","op":"EQ","value":"$CLUSTER_ID"},
 "group_by":["metric.topic"],"granularity":"PT1H","intervals":["$START/$END"],"limit":50}
JSON
done

log "Custo"
evid_header "$C" "Billing: custo diário dos últimos 7 dias (a API tem atraso de até ~1 dia)"
FROM="$(date -u -d '-7 days' '+%Y-%m-%d' 2>/dev/null || date -u -v-7d '+%Y-%m-%d')"
TO="$(date -u -d '+1 day' '+%Y-%m-%d' 2>/dev/null || date -u -v+1d '+%Y-%m-%d')"
evidence "$C" confluent billing cost list --start-date "$FROM" --end-date "$TO" || \
  warn "Sem permissão de billing ou dados ainda não consolidados: use Console > Billing & payment > Cost."
ok "Camada 5 pronta."
