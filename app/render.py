"""Preenche um template ${VAR} com as variáveis de ambiente. Usado para a config do conector,
para que a senha do banco nunca fique num arquivo versionado."""
import os
import sys
from string import Template

from common import load_env

load_env()
print(Template(open(sys.argv[1], encoding="utf-8").read()).substitute(os.environ))
