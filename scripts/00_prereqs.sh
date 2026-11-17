#!/usr/bin/env bash
# Confere ferramentas locais e instala as libs Python.
source "$(dirname "$0")/lib.sh"
log "Ferramentas"
for bin in confluent jq "$PY"; do
  command -v "$bin" >/dev/null && ok "$bin: $($bin --version 2>&1 | head -1)" || { echo "FALTA: $bin"; exit 1; }
done
log "Dependências Python"
"$PY" -m pip install -q -r requirements.txt && ok "requirements.txt instalado"
log "Conexão com o Postgres"
"$PY" app/pg_exec.py --query "SELECT version()" --query "SHOW wal_level"
warn "wal_level precisa ser 'logical'. Se não for, habilite Logical replication no Neon."
