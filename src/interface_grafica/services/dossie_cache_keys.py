from __future__ import annotations

import hashlib
import json
from typing import Any


VERSAO_PADRAO_DOSSIE = "v1"


def normalizar_parametros_dossie(parametros: dict[str, Any] | None) -> dict[str, Any]:
    """Normaliza parâmetros para geração estável de cache do dossiê.

    Regras:
    - `None` vira dicionário vazio;
    - chaves são convertidas para string;
    - valores vazios (`None`, string vazia, listas vazias) são descartados;
    - listas e tuplas são ordenadas quando possível para evitar duplicação por ordem;
    - dicionários internos são normalizados recursivamente.
    """

    if not parametros:
        return {}

    def _normalizar_valor(valor: Any) -> Any:
        if isinstance(valor, dict):
            return normalizar_parametros_dossie(valor)
        if isinstance(valor, (list, tuple, set)):
            itens = [_normalizar_valor(item) for item in valor if item not in (None, "", [], (), {})]
            try:
                return sorted(itens)
            except TypeError:
                return itens
        return valor

    normalizado: dict[str, Any] = {}
    for chave, valor in sorted(parametros.items(), key=lambda item: str(item[0])):
        chave_txt = str(chave)
        valor_norm = _normalizar_valor(valor)
        if valor_norm in (None, "", [], (), {}, set()):
            continue
        normalizado[chave_txt] = valor_norm
    return normalizado


def serializar_parametros_dossie(parametros: dict[str, Any] | None) -> str:
    """Serializa parâmetros normalizados em JSON estável."""

    return json.dumps(
        normalizar_parametros_dossie(parametros),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def gerar_chave_cache_dossie(
    cnpj: str,
    secao: str,
    parametros: dict[str, Any] | None = None,
    versao_consulta: str | None = None,
) -> str:
    """Gera uma chave curta e estável para cache do dossiê."""

    payload = {
        "cnpj": str(cnpj).strip(),
        "secao": str(secao).strip().lower(),
        "parametros": normalizar_parametros_dossie(parametros),
        "versao_consulta": (versao_consulta or VERSAO_PADRAO_DOSSIE).strip(),
    }
    bruto = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(bruto.encode("utf-8")).hexdigest()[:24]


def gerar_nome_arquivo_cache_dossie(
    cnpj: str,
    secao: str,
    parametros: dict[str, Any] | None = None,
    versao_consulta: str | None = None,
    extensao: str = ".parquet",
) -> str:
    """Monta um nome de arquivo previsível para persistência do cache do dossiê."""

    chave = gerar_chave_cache_dossie(
        cnpj=cnpj,
        secao=secao,
        parametros=parametros,
        versao_consulta=versao_consulta,
    )
    secao_slug = "_".join(parte for parte in str(secao).strip().lower().replace("/", " ").replace("-", " ").split() if parte)
    sufixo = extensao if extensao.startswith(".") else f".{extensao}"
    return f"dossie_{str(cnpj).strip()}_{secao_slug}_{chave}{sufixo}"
