import os
import shutil
import re
from pathlib import Path

# Setup mock env
env_path = Path(".env")
env_path.write_text("DB_USER=olduser\nDB_PASSWORD=oldpass\nORACLE_HOST=oldhost", encoding="utf-8")

from backend.routers.oracle import _write_key

conteudo = env_path.read_text(encoding="utf-8")
new_conteudo = _write_key(conteudo, "DB_USER", "newuser")
new_conteudo = _write_key(new_conteudo, "DB_PASSWORD", "newpass\nMALICIOUS=true")
new_conteudo = _write_key(new_conteudo, "ORACLE_HOST", "domain\\user")

print(new_conteudo)
