#!/usr/bin/env bash
# Funções compartilhadas. Todo script começa com: source "$(dirname "$0")/lib.sh"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="$ROOT/.state"
EVID_DIR="$ROOT/evidence"
mkdir -p "$STATE_DIR" "$EVID_DIR"
touch "$STATE_DIR/ids.env" "$STATE_DIR/secrets.env"

if [ ! -f "$ROOT/.env" ]; then
  echo "ERRO: crie o .env a partir do modelo:  cp .env.example .env  (e preencha)"; exit 1
fi
set -a
# shellcheck disable=SC1091
source "$ROOT/.env"; source "$STATE_DIR/ids.env"; source "$STATE_DIR/secrets.env"
set +a
PY="${PY:-python3}"
cd "$ROOT"

log()  { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m✔ %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m! %s\033[0m\n' "$*"; }

_save() { # arquivo chave valor
  sed -i.bak "/^$2=/d" "$1" && rm -f "$1.bak"
  echo "$2=$3" >> "$1"; export "$2=$3"
}
save_id()     { _save "$STATE_DIR/ids.env" "$1" "$2"; }
save_secret() { _save "$STATE_DIR/secrets.env" "$1" "$2"; }

# Esconde qualquer segredo conhecido da saída antes de ir para a tela/arquivo.
mask() {
  local list
  list="$(printf '%s\n' "${PG_PASSWORD:-}" "${READER_API_SECRET:-}" "${READER_SR_API_SECRET:-}" \
          "${CLI_API_SECRET:-}" "${CLI_SR_API_SECRET:-}" "${CLOUD_API_SECRET:-}")"
  awk -v list="$list" 'BEGIN{n=split(list,S,"\n")}
    {for(i=1;i<=n;i++) if(length(S[i])>3){while((p=index($0,S[i]))>0){$0=substr($0,1,p-1) "****" substr($0,p+length(S[i]))}} print}'
}

# evidence <arquivo> <comando...>: roda, mostra e anexa em evidence/<arquivo> (já mascarado)
evidence() {
  local file="$1"; shift
  { echo "\$ $*"; "$@" 2>&1; echo; } | mask | tee -a "$EVID_DIR/$file"
}

# Cabeçalho de seção dentro de um arquivo de evidência
evid_header() { printf '### %s  (%s UTC)\n' "$2" "$(date -u '+%Y-%m-%d %H:%M:%S')" | tee -a "$EVID_DIR/$1"; }

wait_for() { # descrição  comando-que-imprime-status  status-esperado  [tentativas]
  local desc="$1" cmd="$2" want="$3" tries="${4:-60}" got=""
  for _ in $(seq 1 "$tries"); do
    got="$(eval "$cmd" 2>/dev/null || true)"
    [ "$got" = "$want" ] && { ok "$desc: $got"; return 0; }
    printf '  aguardando %s (atual: %s)...\n' "$desc" "${got:-?}"; sleep 10
  done
  echo "ERRO: $desc não chegou em $want (último: $got)"; return 1
}

need() { [ -n "${!1:-}" ] || { echo "ERRO: $1 vazio. Rode os scripts anteriores na ordem."; exit 1; }; }
