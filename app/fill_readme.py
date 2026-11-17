"""Preenche os blocos de evidência do README com o conteúdo de evidence/.

No README, um bloco tem esta forma:
    <!-- evid:01_fundacao.txt:40 -->
    ...qualquer coisa (é substituído)...
    <!-- /evid -->
O número opcional é o máximo de linhas (as primeiras). Se o arquivo não existir, o bloco fica
marcado como pendente e o scripts/check_entrega.sh acusa.
"""
import re

from common import ROOT

README = ROOT / "README.md"
PATTERN = re.compile(r"(<!-- evid:([\w.\-]+)(?::(\d+))? -->)(.*?)(<!-- /evid -->)", re.S)


def render(match: re.Match) -> str:
    opener, name, limit, _, closer = match.groups()
    path = ROOT / "evidence" / name
    if not path.exists() or not path.read_text(encoding="utf-8").strip():
        body = f"\n> ⏳ EVIDENCIA_PENDENTE: rode o script que gera `evidence/{name}`\n"
    else:
        lines = path.read_text(encoding="utf-8").rstrip().splitlines()
        if limit and len(lines) > int(limit):
            lines = lines[: int(limit)] + [f"... ({len(lines) - int(limit)} linhas a mais em evidence/{name})"]
        body = "\n```text\n" + "\n".join(lines) + "\n```\n"
    return f"{opener}{body}{closer}"


text = README.read_text(encoding="utf-8")
new_text, n = PATTERN.subn(render, text)
README.write_text(new_text, encoding="utf-8")
print(f"{n} bloco(s) de evidência atualizados no README.md")
