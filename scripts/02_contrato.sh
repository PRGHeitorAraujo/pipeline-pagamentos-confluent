#!/usr/bin/env bash
# CAMADA 2 — Contrato: registra o formato do alerta ANTES de existir produtor e prova
# que o Schema Registry aceita uma mudança compatível e barra uma incompatível.
source "$(dirname "$0")/lib.sh"
need ENV_ID; need CLUSTER_ID
E=02_contrato.txt; : > "$EVID_DIR/$E"
SUBJ=fraud_alerts-value
confluent environment use "$ENV_ID" >/dev/null; confluent kafka cluster use "$CLUSTER_ID" >/dev/null

log "Tópico de saída dos alertas"
confluent kafka topic create fraud_alerts --partitions 1 --if-not-exists

evid_header "$E" "v1 registrada"
evidence "$E" confluent schema-registry schema create --subject "$SUBJ" --schema schemas/fraud_alert_v1.avsc --type avro
evid_header "$E" "Compatibilidade do subject = BACKWARD"
evidence "$E" confluent schema-registry subject update "$SUBJ" --compatibility backward

evid_header "$E" "Teste 1: v2 (campo novo COM default) -> esperado compatível"
evidence "$E" confluent schema-registry schema compatibility validate schemas/fraud_alert_v2.avsc \
  --type avro --subject "$SUBJ" --version latest

evid_header "$E" "Teste 2: v3 (muda tipo + campo sem default) -> esperado INCOMPATÍVEL"
evidence "$E" confluent schema-registry schema compatibility validate schemas/fraud_alert_v3_incompativel.avsc \
  --type avro --subject "$SUBJ" --version latest || true

evid_header "$E" "Tentativa de registrar a v3 -> o registry tem que recusar"
evidence "$E" confluent schema-registry schema create --subject "$SUBJ" \
  --schema schemas/fraud_alert_v3_incompativel.avsc --type avro || true

evid_header "$E" "v2 registrada (é a que o Flink vai usar)"
evidence "$E" confluent schema-registry schema create --subject "$SUBJ" --schema schemas/fraud_alert_v2.avsc --type avro
evid_header "$E" "Versões do subject"
evidence "$E" confluent schema-registry subject describe "$SUBJ"
ok "Camada 2 pronta."
