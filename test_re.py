import re

def _write_key(conteudo: str, chave: str, valor: str) -> str:
    # Remove newlines to prevent CRLF/env injection
    safe_valor = valor.replace("\n", "").replace("\r", "")
    if re.search(rf"^{chave}=", conteudo, flags=re.MULTILINE):
        return re.sub(rf"^{chave}=.*$", lambda m: f"{chave}={safe_valor}", conteudo, flags=re.MULTILINE)
    return conteudo.rstrip() + f"\n{chave}={safe_valor}\n"

print(repr(_write_key("TEST=123", "TEST", "value\\with\\backslash")))
print(repr(_write_key("TEST=123", "TEST", "value\nMALICIOUS=true")))
