from __future__ import annotations

from pathlib import Path
from typing import Iterable
import re
import unicodedata

import polars as pl

from utilitarios.project_paths import CNPJ_ROOT


COLUNAS_CONTATO = [
    "tipo_vinculo",
    "cnpj_consultado",
    "cnpj_raiz",
    "cpf_cnpj_referencia",
    "nome_referencia",
    "crc_contador",
    "endereco",
    "telefone",
    "telefone_nfe_nfce",
    "email",
    "telefones_por_fonte",
    "emails_por_fonte",
    "fontes_contato",
    "situacao_cadastral",
    "indicador_matriz_filial",
    "origem_dado",
    "tabela_origem",
    "ordem_exibicao",
]

PRIORIDADE_FONTE_CONTADOR = {
    "dossie_historico_fac.sql": 0,
    "dossie_contador.sql": 1,
    "dossie_rascunho_fac_contador.sql": 2,
    "dossie_req_inscricao_contador.sql": 3,
}

ROTULO_FONTE_CONTADOR = {
    "dossie_historico_fac.sql": "FAC atual",
    "dossie_contador.sql": "SITAFE_PESSOA",
    "dossie_rascunho_fac_contador.sql": "Rascunho FAC",
    "dossie_req_inscricao_contador.sql": "Requerimento",
}


def compor_secao_dossie(cnpj: str, secao_id: str, datasets: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Compoe a secao final do Dossie a partir dos datasets base disponiveis."""

    if secao_id == "contato":
        return compor_secao_contato(cnpj, datasets)

    if not datasets:
        return pl.DataFrame()

    dataframes: list[pl.DataFrame] = []
    for sql_id, dataframe in datasets.items():
        if dataframe.is_empty():
            continue
        if "sql_id_origem" in dataframe.columns:
            dataframes.append(dataframe)
        else:
            dataframes.append(dataframe.with_columns(pl.lit(sql_id).alias("sql_id_origem")))

    if not dataframes:
        return pl.DataFrame()
    if len(dataframes) == 1:
        return dataframes[0]
    return pl.concat(dataframes, how="diagonal_relaxed")


def compor_secao_contato(cnpj: str, datasets: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Monta a secao `contato` com reaproveitamento de datasets ja existentes."""

    dataset_consolidado = datasets.get("dossie_contato.sql", pl.DataFrame())
    if not dataset_consolidado.is_empty():
        return _normalizar_secao_contato_consolidada(dataset_consolidado)

    cnpj_limpo = "".join(caractere for caractere in str(cnpj) if caractere.isdigit())
    cnpj_raiz = cnpj_limpo[:8]
    linhas: list[dict[str, object]] = []

    cadastro = _normalizar_dataframe(datasets.get("dados_cadastrais.sql", pl.DataFrame()))
    contadores = _normalizar_dataframe(datasets.get("dossie_contador.sql", pl.DataFrame()))
    historico_fac = _normalizar_dataframe(datasets.get("dossie_historico_fac.sql", pl.DataFrame()))
    rascunho_fac = _normalizar_dataframe(datasets.get("dossie_rascunho_fac_contador.sql", pl.DataFrame()))
    req_inscricao = _normalizar_dataframe(datasets.get("dossie_req_inscricao_contador.sql", pl.DataFrame()))
    socios = _normalizar_dataframe(datasets.get("dossie_historico_socios.sql", pl.DataFrame()))
    filiais_raiz = _normalizar_dataframe(datasets.get("dossie_filiais_raiz.sql", pl.DataFrame()))
    nfe = _normalizar_dataframe(datasets.get("NFe.sql", pl.DataFrame()))
    nfce = _normalizar_dataframe(datasets.get("NFCe.sql", pl.DataFrame()))

    linhas.extend(_linhas_empresa_principal(cnpj_limpo, cnpj_raiz, cadastro))
    linhas.extend(_linhas_empresa_fac(cnpj_limpo, cnpj_raiz, historico_fac))
    linhas.extend(_linhas_filiais_mesma_raiz(cnpj_limpo, cnpj_raiz, filiais_raiz))

    contadores_consolidados = _consolidar_contadores(contadores, historico_fac, rascunho_fac, req_inscricao)
    telefones_contador = _mapear_telefones_contador(contadores_consolidados, [nfe, nfce])
    linhas.extend(_linhas_contadores(cnpj_limpo, cnpj_raiz, contadores_consolidados, telefones_contador))
    linhas.extend(_linhas_socios(cnpj_limpo, cnpj_raiz, socios))
    linhas.extend(_linhas_emails_notas(cnpj_limpo, cnpj_raiz, "NFe.sql", nfe, 50))
    linhas.extend(_linhas_emails_notas(cnpj_limpo, cnpj_raiz, "NFCe.sql", nfce, 55))

    if not linhas:
        return pl.DataFrame(schema={coluna: pl.Utf8 for coluna in COLUNAS_CONTATO})

    dataframe = pl.DataFrame(linhas)
    for coluna in COLUNAS_CONTATO:
        if coluna not in dataframe.columns:
            dataframe = dataframe.with_columns(pl.lit(None).alias(coluna))

    return (
        dataframe.select(COLUNAS_CONTATO)
        .with_columns(pl.col("ordem_exibicao").cast(pl.Int64, strict=False))
        .sort(["ordem_exibicao", "tipo_vinculo", "nome_referencia", "cpf_cnpj_referencia"], nulls_last=True)
    )


def _normalizar_secao_contato_consolidada(dataframe: pl.DataFrame) -> pl.DataFrame:
    """Aceita o resultado direto do SQL consolidado mantendo o contrato final da secao."""

    dataframe_normalizado = _normalizar_dataframe(dataframe)
    for coluna in COLUNAS_CONTATO:
        if coluna not in dataframe_normalizado.columns:
            dataframe_normalizado = dataframe_normalizado.with_columns(pl.lit(None).alias(coluna))

    return (
        dataframe_normalizado.select(COLUNAS_CONTATO)
        .with_columns(pl.col("ordem_exibicao").cast(pl.Int64, strict=False))
        .sort(["ordem_exibicao", "tipo_vinculo", "nome_referencia", "cpf_cnpj_referencia"], nulls_last=True)
    )


def _normalizar_dataframe(dataframe: pl.DataFrame) -> pl.DataFrame:
    if dataframe.is_empty():
        return dataframe
    mapa_colunas: dict[str, str] = {}
    nomes_usados: dict[str, int] = {}

    for coluna in dataframe.columns:
        nome_base = _normalizar_nome_coluna(coluna)
        indice_existente = nomes_usados.get(nome_base, 0)
        nome_final = nome_base if indice_existente == 0 else f"{nome_base}_{indice_existente}"
        nomes_usados[nome_base] = indice_existente + 1
        mapa_colunas[coluna] = nome_final

    return dataframe.rename(mapa_colunas)


def _normalizar_nome_coluna(coluna: str) -> str:
    texto = unicodedata.normalize("NFKD", str(coluna))
    texto = "".join(caractere for caractere in texto if not unicodedata.combining(caractere))
    texto = re.sub(r"[^a-zA-Z0-9]+", "_", texto).strip("_").lower()
    return texto or "coluna"


def _normalizar_texto(valor: object) -> str | None:
    if valor is None:
        return None
    texto = str(valor).strip()
    return texto or None


def _normalizar_texto_comparavel(valor: object) -> str:
    texto = unicodedata.normalize("NFKD", str(valor or ""))
    texto = "".join(caractere for caractere in texto if not unicodedata.combining(caractere))
    return texto.strip().upper()


def _iterar_registros(dataframe: pl.DataFrame) -> Iterable[dict[str, object]]:
    """Itera registros nomeados sem criar listas intermediarias desnecessarias."""

    return dataframe.iter_rows(named=True)


def _somente_digitos(valor: object) -> str:
    return "".join(caractere for caractere in str(valor or "") if caractere.isdigit())


def _registro_contato_base(
    *,
    tipo_vinculo: str,
    cnpj_consultado: str,
    cnpj_raiz: str,
    cpf_cnpj_referencia: str | None,
    nome_referencia: str | None,
    crc_contador: str | None = None,
    endereco: str | None = None,
    telefone: str | None = None,
    telefone_nfe_nfce: str | None = None,
    email: str | None = None,
    telefones_por_fonte: str | None = None,
    emails_por_fonte: str | None = None,
    fontes_contato: str | None = None,
    situacao_cadastral: str | None = None,
    indicador_matriz_filial: str | None = None,
    origem_dado: str | None = None,
    tabela_origem: str | None = None,
    ordem_exibicao: int = 0,
) -> dict[str, object]:
    return {
        "tipo_vinculo": tipo_vinculo,
        "cnpj_consultado": cnpj_consultado,
        "cnpj_raiz": cnpj_raiz,
        "cpf_cnpj_referencia": cpf_cnpj_referencia,
        "nome_referencia": nome_referencia,
        "crc_contador": crc_contador,
        "endereco": endereco,
        "telefone": telefone,
        "telefone_nfe_nfce": telefone_nfe_nfce,
        "email": email,
        "telefones_por_fonte": telefones_por_fonte,
        "emails_por_fonte": emails_por_fonte,
        "fontes_contato": fontes_contato,
        "situacao_cadastral": situacao_cadastral,
        "indicador_matriz_filial": indicador_matriz_filial,
        "origem_dado": origem_dado,
        "tabela_origem": tabela_origem,
        "ordem_exibicao": ordem_exibicao,
    }


def _linhas_empresa_principal(cnpj: str, cnpj_raiz: str, cadastro: pl.DataFrame) -> list[dict[str, object]]:
    if cadastro.is_empty():
        return []

    linha = cadastro.to_dicts()[0]
    return [
        _registro_contato_base(
            tipo_vinculo="EMPRESA_PRINCIPAL",
            cnpj_consultado=cnpj,
            cnpj_raiz=cnpj_raiz,
            cpf_cnpj_referencia=_normalizar_texto(linha.get("cnpj")) or cnpj,
            nome_referencia=_normalizar_texto(linha.get("nome")),
            endereco=_normalizar_texto(linha.get("endereco")),
            situacao_cadastral=_normalizar_texto(linha.get("situacao_da_ie")),
            indicador_matriz_filial="EMPRESA",
            origem_dado="dados_cadastrais.sql",
            tabela_origem="BI.DM_PESSOA; BI.DM_LOCALIDADE; BI.DM_REGIME_PAGTO_DESCRICAO; SITAFE.SITAFE_HISTORICO_SITUACAO",
            ordem_exibicao=10,
        )
    ]


def _linhas_empresa_fac(cnpj: str, cnpj_raiz: str, historico_fac: pl.DataFrame) -> list[dict[str, object]]:
    """Aproveita a FAC mais recente como fonte complementar de contato da empresa."""

    if historico_fac.is_empty():
        return []

    linha = historico_fac.to_dicts()[0]
    endereco = _montar_endereco_textual(
        _normalizar_texto(linha.get("logradouro")),
        _normalizar_texto(linha.get("num")),
        _normalizar_texto(linha.get("municipio")),
        _normalizar_texto(linha.get("uf")),
    ) or _montar_endereco_textual(
        _normalizar_texto(linha.get("logradouro_corr")),
        _normalizar_texto(linha.get("bairro_corr")),
        _normalizar_texto(linha.get("municipio_corr")),
        _normalizar_texto(linha.get("uf_corr")),
    )
    telefone = _normalizar_texto(linha.get("telefone")) or _normalizar_texto(linha.get("telefone_corr"))
    email = _normalizar_texto(linha.get("email")) or _normalizar_texto(linha.get("email_corr"))

    if endereco is None and telefone is None and email is None:
        return []

    return [
        _registro_contato_base(
            tipo_vinculo="EMPRESA_FAC_ATUAL",
            cnpj_consultado=cnpj,
            cnpj_raiz=cnpj_raiz,
            cpf_cnpj_referencia=cnpj,
            nome_referencia=_normalizar_texto(linha.get("nome")),
            endereco=endereco,
            telefone=telefone,
            email=email,
            telefones_por_fonte=_formatar_bloco_fonte_contato(
                "FAC atual",
                _normalizar_texto(linha.get("telefone")),
                _normalizar_texto(linha.get("telefone_corr")),
            ),
            emails_por_fonte=_formatar_bloco_fonte_contato(
                "FAC atual",
                _normalizar_texto(linha.get("email")),
                _normalizar_texto(linha.get("email_corr")),
            ),
            fontes_contato="FAC atual",
            situacao_cadastral="FAC atual",
            indicador_matriz_filial="EMPRESA",
            origem_dado="dossie_historico_fac.sql",
            tabela_origem="SITAFE.SITAFE_HISTORICO_CONTRIBUINTE; SITAFE.SITAFE_PESSOA; SITAFE.SITAFE_TABELAS_CADASTRO; BI.DM_LOCALIDADE",
            ordem_exibicao=15,
        )
    ]


def _linhas_filiais_mesma_raiz(
    cnpj_consultado: str,
    cnpj_raiz: str,
    filiais_raiz: pl.DataFrame,
) -> list[dict[str, object]]:
    if not filiais_raiz.is_empty():
        linhas = []
        for linha in _iterar_registros(filiais_raiz):
            tipo_vinculo = _normalizar_texto(linha.get("tipo_vinculo"))
            cpf_cnpj_referencia = _somente_digitos(linha.get("cpf_cnpj_referencia")) or None
            if not tipo_vinculo or cpf_cnpj_referencia == cnpj_consultado:
                continue
            linhas.append(
                _registro_contato_base(
                    tipo_vinculo=tipo_vinculo,
                    cnpj_consultado=cnpj_consultado,
                    cnpj_raiz=cnpj_raiz,
                    cpf_cnpj_referencia=cpf_cnpj_referencia,
                    nome_referencia=_normalizar_texto(linha.get("nome_referencia")),
                    endereco=_normalizar_texto(linha.get("endereco")),
                    situacao_cadastral=_normalizar_texto(linha.get("situacao_cadastral")),
                    indicador_matriz_filial=_normalizar_texto(linha.get("indicador_matriz_filial")),
                    origem_dado=_normalizar_texto(linha.get("origem_dado")) or "dossie_filiais_raiz.sql",
                    tabela_origem=_normalizar_texto(linha.get("tabela_origem")) or "BI.DM_PESSOA; BI.DM_SITUACAO_CONTRIBUINTE",
                    ordem_exibicao=int(linha.get("ordem_exibicao") or (20 if tipo_vinculo == "MATRIZ_RAIZ" else 25)),
                )
            )
        return linhas

    linhas: list[dict[str, object]] = []
    for pasta_cnpj in sorted(CNPJ_ROOT.glob(f"{cnpj_raiz}*")):
        if not pasta_cnpj.is_dir():
            continue
        cnpj_relacionado = pasta_cnpj.name
        if cnpj_relacionado == cnpj_consultado or len(cnpj_relacionado) != 14:
            continue

        caminho_cadastro = pasta_cnpj / "arquivos_parquet" / f"dados_cadastrais_{cnpj_relacionado}.parquet"
        if not caminho_cadastro.exists():
            continue

        cadastro = _normalizar_dataframe(pl.scan_parquet(caminho_cadastro).limit(1).collect())
        if cadastro.is_empty():
            continue

        linha = cadastro.to_dicts()[0]
        indicador = "MATRIZ" if cnpj_relacionado[8:12] == "0001" else "FILIAL"
        tipo_vinculo = "MATRIZ_RAIZ" if indicador == "MATRIZ" else "FILIAL_RAIZ"
        linhas.append(
            _registro_contato_base(
                tipo_vinculo=tipo_vinculo,
                cnpj_consultado=cnpj_consultado,
                cnpj_raiz=cnpj_raiz,
                cpf_cnpj_referencia=cnpj_relacionado,
                nome_referencia=_normalizar_texto(linha.get("nome")),
                endereco=_normalizar_texto(linha.get("endereco")),
                situacao_cadastral=_normalizar_texto(linha.get("situacao_da_ie")),
                indicador_matriz_filial=indicador,
                origem_dado="dados_cadastrais_reutilizado",
                tabela_origem="BI.DM_PESSOA; BI.DM_LOCALIDADE; BI.DM_REGIME_PAGTO_DESCRICAO; SITAFE.SITAFE_HISTORICO_SITUACAO",
                ordem_exibicao=20 if indicador == "MATRIZ" else 25,
            )
        )
    return linhas


def _linhas_contadores(
    cnpj_consultado: str,
    cnpj_raiz: str,
    contadores: pl.DataFrame,
    telefones_contador: dict[str, str],
) -> list[dict[str, object]]:
    if contadores.is_empty():
        return []

    linhas: list[dict[str, object]] = []
    for linha in _iterar_registros(contadores):
        cpf_cnpj = _somente_digitos(linha.get("co_cnpj_cpf_contador")) or None
        linhas.append(
            _registro_contato_base(
                tipo_vinculo="CONTADOR_EMPRESA",
                cnpj_consultado=cnpj_consultado,
                cnpj_raiz=cnpj_raiz,
                cpf_cnpj_referencia=cpf_cnpj,
                nome_referencia=_normalizar_texto(linha.get("nome")),
                crc_contador=_normalizar_texto(linha.get("crc_contador")),
                endereco=_normalizar_texto(linha.get("endereco")),
                telefone=_normalizar_texto(linha.get("telefone")),
                telefone_nfe_nfce=telefones_contador.get(cpf_cnpj or "", None),
                email=_normalizar_texto(linha.get("email")),
                telefones_por_fonte=_normalizar_texto(linha.get("telefones_por_fonte")),
                emails_por_fonte=_normalizar_texto(linha.get("emails_por_fonte")),
                fontes_contato=_normalizar_texto(linha.get("fontes_contato")),
                situacao_cadastral=_formatar_lista_textual_distinta(linha.get("situacoes_vinculo_contador"))
                or _normalizar_texto(linha.get("situacao")),
                indicador_matriz_filial="CONTADOR",
                origem_dado=_normalizar_texto(linha.get("origem_contador")) or "dossie_contador.sql",
                tabela_origem=_normalizar_texto(linha.get("tabela_origem_contador")) or "SITAFE.SITAFE_HISTORICO_CONTRIBUINTE; BI.DM_PESSOA",
                ordem_exibicao=30,
            )
        )
    return linhas


def _consolidar_contadores(
    contadores: pl.DataFrame,
    historico_fac: pl.DataFrame,
    rascunho_fac: pl.DataFrame,
    req_inscricao: pl.DataFrame,
) -> pl.DataFrame:
    """Consolida contadores mantendo a FAC como referencia principal quando existir."""

    registros_consolidados: list[dict[str, object]] = []
    indice_por_chave: dict[tuple[str | None, str | None], int] = {}

    for origem, dataframe in (
        ("dossie_contador.sql", contadores),
        ("dossie_historico_fac.sql", historico_fac),
        ("dossie_rascunho_fac_contador.sql", rascunho_fac),
        ("dossie_req_inscricao_contador.sql", req_inscricao),
    ):
        if dataframe.is_empty():
            continue

        for linha in _iterar_registros(dataframe):
            registro = _normalizar_registro_contador(origem, linha)
            if registro is None:
                continue

            chave = (
                _somente_digitos(registro.get("co_cnpj_cpf_contador")) or None,
                _normalizar_texto(registro.get("nome")),
            )
            if chave in indice_por_chave:
                indice_existente = indice_por_chave[chave]
                registros_consolidados[indice_existente] = _combinar_registros_contador(
                    registros_consolidados[indice_existente],
                    registro,
                )
                continue

            indice_por_chave[chave] = len(registros_consolidados)
            registros_consolidados.append(registro)

    return pl.DataFrame(registros_consolidados) if registros_consolidados else pl.DataFrame()


def _combinar_registros_contador(principal: dict[str, object], complementar: dict[str, object]) -> dict[str, object]:
    """Mescla dados complementares do contador sem perder a rastreabilidade por fonte."""

    referencia, complemento = _ordenar_registros_contador(principal, complementar)
    resultado = dict(referencia)
    for campo in ("nome", "situacao", "endereco", "telefone", "email", "crc_contador"):
        if _normalizar_texto(resultado.get(campo)) is None and _normalizar_texto(complemento.get(campo)) is not None:
            resultado[campo] = complemento.get(campo)

    if _normalizar_texto(resultado.get("tabela_origem_contador")) and _normalizar_texto(complementar.get("tabela_origem_contador")):
        tabelas = []
        for bloco in (
            _normalizar_texto(resultado.get("tabela_origem_contador")),
            _normalizar_texto(complementar.get("tabela_origem_contador")),
        ):
            for tabela in str(bloco).split(";"):
                tabela_limpa = tabela.strip()
                if tabela_limpa and tabela_limpa not in tabelas:
                    tabelas.append(tabela_limpa)
        resultado["tabela_origem_contador"] = "; ".join(tabelas)

    resultado["origens_contador_envolvidas"] = _unir_listas_distintas(
        referencia.get("origens_contador_envolvidas"),
        complemento.get("origens_contador_envolvidas"),
    )
    resultado["situacoes_vinculo_contador"] = _unir_listas_distintas(
        referencia.get("situacoes_vinculo_contador"),
        complemento.get("situacoes_vinculo_contador"),
    )
    resultado["emails_fontes_contador"] = _combinar_contatos_por_fonte(
        referencia.get("emails_fontes_contador"),
        complemento.get("emails_fontes_contador"),
    )
    resultado["telefones_fontes_contador"] = _combinar_contatos_por_fonte(
        referencia.get("telefones_fontes_contador"),
        complemento.get("telefones_fontes_contador"),
    )
    resultado["emails_por_fonte"] = _formatar_contatos_por_fonte(resultado.get("emails_fontes_contador"))
    resultado["telefones_por_fonte"] = _formatar_contatos_por_fonte(resultado.get("telefones_fontes_contador"))
    resultado["fontes_contato"] = _formatar_origens_contador(resultado.get("origens_contador_envolvidas"))
    resultado["situacao"] = _formatar_lista_textual_distinta(resultado.get("situacoes_vinculo_contador")) or _normalizar_texto(
        resultado.get("situacao")
    )

    return resultado


def _normalizar_registro_contador(origem: str, linha: dict[str, object]) -> dict[str, object] | None:
    """Padroniza os campos minimos do contador entre fontes fiscais distintas."""

    if origem == "dossie_contador.sql":
        cpf_cnpj = _somente_digitos(linha.get("co_cnpj_cpf_contador")) or None
        nome = _normalizar_texto(linha.get("nome"))
        situacao_vinculo = _normalizar_texto(linha.get("situacao"))
        if cpf_cnpj is None and nome is None:
            return None
        return {
            "co_cnpj_cpf_contador": cpf_cnpj,
            "nome": nome,
            "situacao": situacao_vinculo,
            "endereco": _montar_endereco_textual(
                _normalizar_texto(linha.get("municipio")),
                _normalizar_texto(linha.get("uf")),
            ),
            "telefone": _normalizar_texto(linha.get("telefone")),
            "email": _normalizar_texto(linha.get("email")),
            "crc_contador": _normalizar_texto(linha.get("crc_contador")),
            "emails_fontes_contador": _registrar_contatos_por_fonte(origem, _normalizar_texto(linha.get("email"))),
            "telefones_fontes_contador": _registrar_contatos_por_fonte(origem, _normalizar_texto(linha.get("telefone"))),
            "origens_contador_envolvidas": [origem],
            "situacoes_vinculo_contador": [situacao_vinculo] if situacao_vinculo else [],
            "prioridade_origem_contador": PRIORIDADE_FONTE_CONTADOR.get(origem, 99),
            "origem_contador": origem,
            "tabela_origem_contador": "SITAFE.SITAFE_HISTORICO_CONTRIBUINTE; BI.DM_PESSOA; BI.DM_LOCALIDADE",
        }

    if origem == "dossie_historico_fac.sql":
        cpf_cnpj = _somente_digitos(linha.get("cpf_contador")) or None
        nome = _normalizar_texto(linha.get("no_contador"))
        ult_fac = _normalizar_texto(linha.get("ult_fac"))
        if cpf_cnpj is None and nome is None:
            return None
        return {
            "co_cnpj_cpf_contador": cpf_cnpj,
            "nome": nome,
            "situacao": "Atual" if ult_fac == "9" else "Historico FAC",
            "endereco": _montar_endereco_textual(
                _normalizar_texto(linha.get("logradouro")),
                _normalizar_texto(linha.get("num")),
                _normalizar_texto(linha.get("municipio")),
                _normalizar_texto(linha.get("uf")),
            ),
            "telefone": _normalizar_texto(linha.get("telefone")),
            "email": _normalizar_texto(linha.get("email")) or _normalizar_texto(linha.get("email_corr")),
            "crc_contador": _normalizar_texto(linha.get("crc_contador")),
            "emails_fontes_contador": _registrar_contatos_por_fonte(
                origem,
                _normalizar_texto(linha.get("email")),
                _normalizar_texto(linha.get("email_corr")),
            ),
            "telefones_fontes_contador": _registrar_contatos_por_fonte(origem, _normalizar_texto(linha.get("telefone"))),
            "origens_contador_envolvidas": [origem],
            "situacoes_vinculo_contador": ["FAC atual" if ult_fac == "9" else "Historico FAC"],
            "prioridade_origem_contador": PRIORIDADE_FONTE_CONTADOR.get(origem, 99),
            "origem_contador": origem,
            "tabela_origem_contador": "SITAFE.SITAFE_HISTORICO_CONTRIBUINTE; SITAFE.SITAFE_PESSOA; SITAFE.SITAFE_TABELAS_CADASTRO; BI.DM_PESSOA; BI.DM_LOCALIDADE",
        }

    if origem == "dossie_rascunho_fac_contador.sql":
        cpf_cnpj = _somente_digitos(linha.get("cpf_contador")) or None
        nome = _normalizar_texto(linha.get("no_contador"))
        if cpf_cnpj is None and nome is None:
            return None
        return {
            "co_cnpj_cpf_contador": cpf_cnpj,
            "nome": nome,
            "situacao": "Rascunho FAC",
            "endereco": _montar_endereco_textual(
                _normalizar_texto(linha.get("logradouro")),
                _normalizar_texto(linha.get("bairro")),
                _normalizar_texto(linha.get("municipio")),
                _normalizar_texto(linha.get("uf")),
            ),
            "telefone": _normalizar_texto(linha.get("telefone")),
            "email": _normalizar_texto(linha.get("email")),
            "crc_contador": _normalizar_texto(linha.get("crc_contador")),
            "emails_fontes_contador": _registrar_contatos_por_fonte(origem, _normalizar_texto(linha.get("email"))),
            "telefones_fontes_contador": _registrar_contatos_por_fonte(origem, _normalizar_texto(linha.get("telefone"))),
            "origens_contador_envolvidas": [origem],
            "situacoes_vinculo_contador": ["Rascunho FAC"],
            "prioridade_origem_contador": PRIORIDADE_FONTE_CONTADOR.get(origem, 99),
            "origem_contador": origem,
            "tabela_origem_contador": "SITAFE.SITAFE_RASCUNHO_FAC",
        }

    if origem == "dossie_req_inscricao_contador.sql":
        cpf_cnpj = _somente_digitos(linha.get("cpf_contador")) or None
        nome = _normalizar_texto(linha.get("no_contador"))
        if cpf_cnpj is None and nome is None:
            return None
        return {
            "co_cnpj_cpf_contador": cpf_cnpj,
            "nome": nome,
            "situacao": "Requerimento de Inscricao",
            "endereco": _montar_endereco_textual(
                _normalizar_texto(linha.get("municipio")),
                _normalizar_texto(linha.get("uf")),
            ),
            "telefone": _normalizar_texto(linha.get("telefone")),
            "crc_contador": _normalizar_texto(linha.get("crc_contador")),
            "emails_fontes_contador": {},
            "telefones_fontes_contador": _registrar_contatos_por_fonte(origem, _normalizar_texto(linha.get("telefone"))),
            "origens_contador_envolvidas": [origem],
            "situacoes_vinculo_contador": ["Requerimento de Inscricao"],
            "prioridade_origem_contador": PRIORIDADE_FONTE_CONTADOR.get(origem, 99),
            "origem_contador": origem,
            "tabela_origem_contador": "SITAFE.SITAFE_REQ_INSCRICAO; BI.DM_LOCALIDADE",
        }

    return None


def _ordenar_registros_contador(
    registro_a: dict[str, object],
    registro_b: dict[str, object],
) -> tuple[dict[str, object], dict[str, object]]:
    """Escolhe o registro de referencia pela prioridade funcional acordada."""

    prioridade_a = registro_a.get("prioridade_origem_contador")
    prioridade_b = registro_b.get("prioridade_origem_contador")
    prioridade_a = int(prioridade_a) if prioridade_a is not None else 99
    prioridade_b = int(prioridade_b) if prioridade_b is not None else 99
    if prioridade_b < prioridade_a:
        return registro_b, registro_a
    return registro_a, registro_b


def _registrar_contatos_por_fonte(origem: str, *valores: str | None) -> dict[str, list[str]]:
    contatos = [valor for valor in (_normalizar_texto(item) for item in valores) if valor]
    if not contatos:
        return {}
    return {origem: contatos}


def _combinar_contatos_por_fonte(
    contatos_a: object,
    contatos_b: object,
) -> dict[str, list[str]]:
    resultado: dict[str, list[str]] = {}
    for bloco in (contatos_a, contatos_b):
        if not isinstance(bloco, dict):
            continue
        for origem, valores in bloco.items():
            lista_atual = resultado.setdefault(str(origem), [])
            for valor in valores if isinstance(valores, list) else []:
                valor_limpo = _normalizar_texto(valor)
                if valor_limpo and valor_limpo not in lista_atual:
                    lista_atual.append(valor_limpo)
    return resultado


def _unir_listas_distintas(lista_a: object, lista_b: object) -> list[str]:
    valores: list[str] = []
    for bloco in (lista_a, lista_b):
        if not isinstance(bloco, list):
            continue
        for valor in bloco:
            valor_limpo = _normalizar_texto(valor)
            if valor_limpo and valor_limpo not in valores:
                valores.append(valor_limpo)
    return valores


def _formatar_contatos_por_fonte(contatos_por_fonte: object) -> str | None:
    if not isinstance(contatos_por_fonte, dict) or not contatos_por_fonte:
        return None

    blocos_formatados: list[str] = []
    for origem, valores in contatos_por_fonte.items():
        contatos_validos = [valor for valor in valores if _normalizar_texto(valor)]
        if not contatos_validos:
            continue
        rotulo = ROTULO_FONTE_CONTADOR.get(str(origem), str(origem))
        blocos_formatados.append(f"{rotulo}: {', '.join(contatos_validos)}")
    return " | ".join(blocos_formatados) if blocos_formatados else None


def _formatar_origens_contador(origens: object) -> str | None:
    if not isinstance(origens, list) or not origens:
        return None
    return " | ".join(ROTULO_FONTE_CONTADOR.get(origem, origem) for origem in origens)


def _formatar_bloco_fonte_contato(rotulo: str, *valores: str | None) -> str | None:
    valores_distintos: list[str] = []
    for valor in valores:
        valor_limpo = _normalizar_texto(valor)
        if valor_limpo and valor_limpo not in valores_distintos:
            valores_distintos.append(valor_limpo)
    if not valores_distintos:
        return None
    return f"{rotulo}: {', '.join(valores_distintos)}"


def _formatar_lista_textual_distinta(valores: object) -> str | None:
    if not isinstance(valores, list):
        return None

    valores_distintos: list[str] = []
    for valor in valores:
        valor_limpo = _normalizar_texto(valor)
        if valor_limpo and valor_limpo not in valores_distintos:
            valores_distintos.append(valor_limpo)

    if not valores_distintos:
        return None
    return " | ".join(valores_distintos)


def _montar_endereco_textual(*partes: str | None) -> str | None:
    """Monta um endereco textual simples removendo partes vazias."""

    partes_validas = [parte for parte in partes if parte]
    return ", ".join(partes_validas) if partes_validas else None


def _linhas_socios(cnpj_consultado: str, cnpj_raiz: str, socios: pl.DataFrame) -> list[dict[str, object]]:
    if socios.is_empty():
        return []

    linhas: list[dict[str, object]] = []
    for linha in _iterar_registros(socios):
        situacao = _normalizar_texto(linha.get("situacao")) or ""
        situacao_normalizada = _normalizar_texto_comparavel(situacao)
        tipo_vinculo = "SOCIO_ATUAL" if "ATUAL" in situacao_normalizada else "SOCIO_ANTIGO"
        linhas.append(
            _registro_contato_base(
                tipo_vinculo=tipo_vinculo,
                cnpj_consultado=cnpj_consultado,
                cnpj_raiz=cnpj_raiz,
                cpf_cnpj_referencia=_somente_digitos(linha.get("co_cnpj_cpf")) or None,
                nome_referencia=_normalizar_texto(linha.get("nome")),
                endereco=_normalizar_texto(linha.get("endereco")),
                telefone=_normalizar_texto(linha.get("telefone")),
                email=_normalizar_texto(linha.get("email")),
                situacao_cadastral=situacao,
                indicador_matriz_filial="SOCIO",
                origem_dado="dossie_historico_socios.sql",
                tabela_origem="SITAFE.SITAFE_HISTORICO_SOCIO; SITAFE.SITAFE_PESSOA; SITAFE.SITAFE_TABELAS_CADASTRO; BI.DM_LOCALIDADE",
                ordem_exibicao=40 if tipo_vinculo == "SOCIO_ATUAL" else 45,
            )
        )
    return linhas


def _linhas_emails_notas(
    cnpj_consultado: str,
    cnpj_raiz: str,
    sql_id: str,
    documentos: pl.DataFrame,
    ordem_exibicao: int,
) -> list[dict[str, object]]:
    if documentos.is_empty() or "email_dest" not in documentos.columns:
        return []

    emails = (
        documentos.select(pl.col("email_dest").cast(pl.Utf8, strict=False).alias("email_dest"))
        .drop_nulls()
        .filter(pl.col("email_dest").str.strip_chars() != "")
        .unique()
        .to_series()
        .to_list()
    )

    return [
        _registro_contato_base(
            tipo_vinculo="EMAIL_NFE",
            cnpj_consultado=cnpj_consultado,
            cnpj_raiz=cnpj_raiz,
            cpf_cnpj_referencia=cnpj_consultado,
            nome_referencia="Email observado em documento fiscal",
            email=_normalizar_texto(email),
            indicador_matriz_filial="EMPRESA",
            origem_dado=sql_id,
            tabela_origem="BI.FATO_NFE_DETALHE" if sql_id == "NFe.sql" else "BI.FATO_NFCE_DETALHE",
            ordem_exibicao=ordem_exibicao,
        )
        for email in emails
    ]


def _mapear_telefones_contador(contadores: pl.DataFrame, documentos: Iterable[pl.DataFrame]) -> dict[str, str]:
    if contadores.is_empty() or "co_cnpj_cpf_contador" not in contadores.columns:
        return {}

    identificadores = {
        _normalizar_documento_contador(valor)
        for valor in contadores.get_column("co_cnpj_cpf_contador").to_list()
        if _normalizar_documento_contador(valor)
    }
    if not identificadores:
        return {}

    telefones_por_contador: dict[str, set[str]] = {identificador: set() for identificador in identificadores}
    for dataframe in documentos:
        if dataframe.is_empty():
            continue
        for identificador, telefone in _iterar_telefones_reconciliados(dataframe):
            if identificador in telefones_por_contador:
                telefones_por_contador[identificador].add(telefone)

    return {
        identificador: ", ".join(sorted(telefones))
        for identificador, telefones in telefones_por_contador.items()
        if telefones
    }


def _iterar_telefones_reconciliados(dataframe: pl.DataFrame) -> Iterable[tuple[str, str]]:
    """Retorna apenas telefones reconciliados por CPF/CNPJ valido do contador."""

    for coluna_id, coluna_fone in (("co_emitente", "fone_emit"), ("co_destinatario", "fone_dest")):
        if coluna_id not in dataframe.columns or coluna_fone not in dataframe.columns:
            continue

        pares = dataframe.select([pl.col(coluna_id), pl.col(coluna_fone)]).drop_nulls()
        for par in _iterar_registros(pares):
            identificador = _normalizar_documento_contador(par.get(coluna_id))
            telefone = _normalizar_telefone_observado(par.get(coluna_fone))
            if identificador and telefone:
                yield identificador, telefone


def _normalizar_documento_contador(valor: object) -> str | None:
    """Aceita apenas CPF ou CNPJ completos para reconciliacao conservadora."""

    documento = _somente_digitos(valor)
    if len(documento) in {11, 14}:
        return documento
    return None


def _normalizar_telefone_observado(valor: object) -> str | None:
    """Descarta telefones incompletos ou nao numericos antes do enriquecimento."""

    telefone = _somente_digitos(valor)
    if len(telefone) >= 8:
        return telefone
    return None
