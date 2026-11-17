#!/usr/bin/env bash
# Sobe as 5 camadas em sequência e preenche o README com as evidências.
set -euo pipefail
D="$(dirname "$0")"
bash "$D/00_prereqs.sh"
bash "$D/01_fundacao.sh"
echo "Esperando 90 s para as API keys propagarem..."; sleep 90
bash "$D/02_contrato.sh"
bash "$D/03_ingestao.sh"
bash "$D/04_processamento.sh"
bash "$D/05_operacao.sh"
source "$D/lib.sh"; "$PY" app/fill_readme.py
