#!/usr/bin/env bash
# Checklist da entrega antes de submeter.
cd "$(dirname "$0")/.." || exit 1
fail=0
chk() { if eval "$2"; then printf '\033[1;32m✔\033[0m %s\n' "$1"; else printf '\033[1;31m✘\033[0m %s\n' "$1"; fail=1; fi; }

chk "README sem '<sua evidência aqui>'"          "! grep -q '<sua evidência aqui>' README.md"
chk "README sem EVIDENCIA_PENDENTE"              "! grep -q 'EVIDENCIA_PENDENTE' README.md"
chk ".env.example presente"                      "[ -f .env.example ]"
chk ".env NÃO versionado"                        "! git ls-files --error-unmatch .env >/dev/null 2>&1"
chk ".state/ NÃO versionado"                     "[ -z \"\$(git ls-files .state 2>/dev/null)\" ]"
chk "Nenhuma senha do .env em arquivo versionado" \
  "[ ! -f .env ] || ! (set -a; . ./.env; git grep -qF -- \"\$PG_PASSWORD\" 2>/dev/null)"
chk "Nenhum segredo de .state em arquivo versionado" \
  "[ ! -s .state/secrets.env ] || ! (cut -d= -f2 .state/secrets.env | grep -v '^\$' | git grep -qF -f - 2>/dev/null)"
chk "Nome do remoto em minúsculas e sem acento" \
  "git remote get-url origin 2>/dev/null | sed 's#.*/##; s#\\.git\$##' | grep -qE '^[a-z0-9._-]+\$'"
exit $fail
