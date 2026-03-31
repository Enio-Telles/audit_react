"""Analisa SQLs fiscais e propoe extracoes Oracle orientadas a CNPJ."""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from extrair_oracle import obter_mapeamento_fontes_oracle

CATALOGO_FONTES_ORACLE: dict[str, dict[str, Any]] = {
    "BI.FATO_NFE_DETALHE": {
        "dominio": "documentos_fiscais_nfe",
        "camada_bronze": "bronze/documentos_nfe_item",
        "camada_silver": "silver/documentos_fiscais_nfe",
        "camada_gold": "gold/documentos_fiscais_por_cfop",
        "chave_principal": ["chave_acesso", "prod_nitem"],
        "chave_recorte": "cnpj",
        "filtro_cnpj": "co_emitente = :CNPJ OR co_destinatario = :CNPJ",
        "filtro_temporal": "dhemi ou dt_gravacao <= :data_limite_processamento",
        "colunas_fiscais": [
            "chave_acesso",
            "prod_nitem",
            "co_emitente",
            "co_destinatario",
            "dhemi",
            "co_cfop",
            "prod_ncm",
            "prod_cest",
            "icms_cst",
            "icms_csosn",
            "icms_vbc",
            "icms_vicms",
            "icms_vbcst",
            "icms_vicmsst",
            "icms_vfcp",
            "ide_co_mod",
        ],
        "recomposicao_polars": "Ler bronze por CNPJ e competencia, separar cabecalho e itens por chave_documento, depois consolidar regras de ICMS/ST/FCP em LazyFrame.",
    },
    "BI.FATO_NFCE_DETALHE": {
        "dominio": "documentos_fiscais_nfce",
        "camada_bronze": "bronze/documentos_nfce_item",
        "camada_silver": "silver/documentos_fiscais_nfce",
        "camada_gold": "gold/documentos_fiscais_por_cfop",
        "chave_principal": ["chave_acesso", "prod_nitem"],
        "chave_recorte": "cnpj",
        "filtro_cnpj": "co_emitente = :CNPJ OR co_destinatario = :CNPJ",
        "filtro_temporal": "dhemi ou dt_gravacao <= :data_limite_processamento",
        "colunas_fiscais": [
            "chave_acesso",
            "prod_nitem",
            "co_emitente",
            "co_destinatario",
            "dhemi",
            "co_cfop",
            "prod_ncm",
            "prod_cest",
            "icms_cst",
            "icms_csosn",
            "icms_vbc",
            "icms_vicms",
            "icms_vbcst",
            "icms_vicmsst",
            "icms_vfcp",
            "ide_co_mod",
        ],
        "recomposicao_polars": "Unificar NFC-e itemizada em silver com estrutura equivalente a NF-e para comparacoes por CFOP, CST e regime tributario.",
    },
    "BI.NFE_XML": {
        "dominio": "xml_nfe_st_fcp",
        "camada_bronze": "bronze/xml_nfe_st_item",
        "camada_silver": "silver/nfe_dados_st",
        "camada_gold": "gold/st_itens",
        "chave_principal": ["chave_acesso", "prod_nitem"],
        "chave_recorte": "cnpj",
        "filtro_cnpj": "chave_acesso contida na lista de NF-e do CNPJ filtrado em BI.FATO_NFE_DETALHE",
        "filtro_temporal": "Herdado do detalhe da NF-e por dt_gravacao <= :data_limite_processamento",
        "colunas_fiscais": [
            "chave_acesso",
            "prod_nitem",
            "vbcst",
            "vicmsst",
            "vicmssubstituto",
            "vicmsstret",
            "vbcfcpst",
            "pfcpst",
            "vfcpst",
        ],
        "recomposicao_polars": "Explodir o XML por item apenas para tributos de ST/FCP e reconciliar o resultado com o detalhe estruturado da NF-e fora do Oracle.",
    },
    "SPED.REG_0000": {
        "dominio": "efd_seed_arquivo",
        "camada_bronze": "bronze/efd_reg_0000",
        "camada_silver": "silver/efd_arquivos_validos",
        "camada_gold": "gold/efd_periodos_validos",
        "chave_principal": ["id"],
        "chave_recorte": "reg_0000_id",
        "filtro_cnpj": "cnpj = :CNPJ",
        "filtro_temporal": "dt_ini/dt_fin e data_entrega <= :data_limite_processamento",
        "colunas_fiscais": [
            "id",
            "cnpj",
            "ie",
            "uf",
            "dt_ini",
            "dt_fin",
            "data_entrega",
            "cod_fin",
        ],
        "recomposicao_polars": "Materializar a semente de arquivos validos por CNPJ e competencia para reutilizar o recorte de reg_0000_id em todos os registros dependentes do SPED.",
    },
    "SPED.REG_C100": {
        "dominio": "efd_documentos_c100",
        "camada_bronze": "bronze/efd_reg_c100",
        "camada_silver": "silver/efd_documentos_fiscais",
        "camada_gold": "gold/efd_documentos_por_periodo",
        "chave_principal": ["id"],
        "chave_recorte": "reg_0000_id",
        "filtro_cnpj": "Filtrar pelos reg_0000_id validos do CNPJ",
        "filtro_temporal": "dt_doc ou dt_e_s dentro do periodo da auditoria",
        "colunas_fiscais": [
            "id",
            "reg_0000_id",
            "chv_nfe",
            "cod_sit",
            "ind_emit",
            "ind_oper",
            "num_doc",
            "ser",
            "dt_doc",
            "dt_e_s",
        ],
        "recomposicao_polars": "Cruzar com a semente REG_0000 para reduzir volume antes de unir com itens C170 e inventario.",
    },
    "SPED.REG_C170": {
        "dominio": "efd_itens_c170",
        "camada_bronze": "bronze/efd_reg_c170",
        "camada_silver": "silver/efd_itens_documento",
        "camada_gold": "gold/efd_itens_por_cfop",
        "chave_principal": ["reg_c100_id", "num_item"],
        "chave_recorte": "reg_0000_id",
        "filtro_cnpj": "Filtrar pelos reg_0000_id validos do CNPJ",
        "filtro_temporal": "Herdado do arquivo/regime do REG_0000",
        "colunas_fiscais": [
            "reg_c100_id",
            "reg_0000_id",
            "num_item",
            "cod_item",
            "cfop",
            "cst_icms",
            "qtd",
            "unid",
            "vl_item",
            "vl_bc_icms",
            "vl_icms",
            "vl_bc_icms_st",
            "vl_icms_st",
        ],
        "recomposicao_polars": "Juntar itens C170 com C100 e cadastros 0200/0220 em LazyFrame para construir bases reutilizaveis de movimentacao e conciliacao fiscal.",
    },
    "SPED.REG_C176": {
        "dominio": "efd_ressarcimento_st_c176",
        "camada_bronze": "bronze/efd_reg_c176",
        "camada_silver": "silver/c176_xml",
        "camada_gold": "gold/st_itens",
        "chave_principal": ["reg_c100_id", "reg_c170_id"],
        "chave_recorte": "reg_0000_id",
        "filtro_cnpj": "Filtrar pelos reg_0000_id validos do CNPJ",
        "filtro_temporal": "Herdado do arquivo EFD valido por competencia",
        "colunas_fiscais": [
            "reg_0000_id",
            "reg_c100_id",
            "reg_c170_id",
            "cod_mot_res",
            "chave_nfe_ult",
            "num_item_ult_e",
            "dt_ult_e",
            "vl_unit_ult_e",
            "vl_unit_icms_ult_e",
            "vl_unit_res",
        ],
        "recomposicao_polars": "Materializar a trilha de ressarcimento/ST por item e reconciliar os valores com XML de NF-e e cadastro de produtos fora do banco.",
    },
    "SPED.REG_E111": {
        "dominio": "efd_ajustes_apuracao_e111",
        "camada_bronze": "bronze/efd_reg_e111",
        "camada_silver": "silver/e111_ajustes",
        "camada_gold": "gold/ajustes_e111",
        "chave_principal": ["reg_0000_id", "cod_aj_apur", "descr_compl_aj"],
        "chave_recorte": "reg_0000_id",
        "filtro_cnpj": "Filtrar pelos reg_0000_id validos do CNPJ",
        "filtro_temporal": "Data de entrega do arquivo EFD <= :data_limite_processamento",
        "colunas_fiscais": [
            "reg_0000_id",
            "cod_aj_apur",
            "descr_compl_aj",
            "vl_aj_apur",
        ],
        "recomposicao_polars": "Separar os ajustes E111 por competencia, preservar codigo e descricao e consolidar a trilha de apuracao em parquet auditavel.",
    },
    "BI.DM_EFD_AJUSTES": {
        "dominio": "dicionario_ajustes_efd",
        "camada_bronze": "bronze/efd_dm_ajustes",
        "camada_silver": "silver/e111_ajustes",
        "camada_gold": "gold/ajustes_e111",
        "chave_principal": ["co_cod_aj"],
        "chave_recorte": "codigo_ajuste",
        "filtro_cnpj": "Sem filtro direto; usado como dicionario em join com REG_E111",
        "filtro_temporal": "Nao aplicavel",
        "colunas_fiscais": [
            "co_cod_aj",
            "no_cod_aj",
        ],
        "recomposicao_polars": "Usar apenas como dicionario de enriquecimento da trilha E111 sem incorporar logica de apuracao ao Oracle.",
    },
    "SPED.REG_0200": {
        "dominio": "efd_cadastro_produtos_0200",
        "camada_bronze": "bronze/efd_reg_0200",
        "camada_silver": "silver/efd_cadastro_produtos",
        "camada_gold": "gold/produtos_cadastrados_por_cnpj",
        "chave_principal": ["id"],
        "chave_recorte": "reg_0000_id",
        "filtro_cnpj": "Filtrar pelos reg_0000_id validos do CNPJ",
        "filtro_temporal": "Periodo do arquivo SPED valido",
        "colunas_fiscais": [
            "id",
            "reg_0000_id",
            "cod_item",
            "descr_item",
            "cod_barra",
            "cod_ncm",
            "cest",
            "tipo_item",
            "unid_inv",
        ],
        "recomposicao_polars": "Padronizar o cadastro de itens e preservar historico de descricao, NCM, CEST e unidade de inventario fora do banco.",
    },
    "SPED.REG_0205": {
        "dominio": "efd_historico_produtos_0205",
        "camada_bronze": "bronze/efd_reg_0205",
        "camada_silver": "silver/efd_historico_produtos",
        "camada_gold": "gold/produtos_historico_descricao",
        "chave_principal": ["reg_0200_id", "dt_ini"],
        "chave_recorte": "reg_0000_id",
        "filtro_cnpj": "Filtrar pelos reg_0000_id validos do CNPJ",
        "filtro_temporal": "dt_ini/dt_fim dentro do periodo analisado",
        "colunas_fiscais": [
            "reg_0000_id",
            "reg_0200_id",
            "cod_ant_item",
            "descr_ant_item",
            "dt_ini",
            "dt_fim",
        ],
        "recomposicao_polars": "Aplicar historico de descricao e codigo anterior como enriquecimento controlado sobre o cadastro 0200.",
    },
    "SPED.REG_0220": {
        "dominio": "efd_fatores_conversao_0220",
        "camada_bronze": "bronze/efd_reg_0220",
        "camada_silver": "silver/efd_fatores_conversao",
        "camada_gold": "gold/produtos_fatores_conversao",
        "chave_principal": ["reg_0200_id", "unid_conv"],
        "chave_recorte": "reg_0000_id",
        "filtro_cnpj": "Filtrar pelos reg_0000_id validos do CNPJ",
        "filtro_temporal": "Periodo do arquivo SPED valido",
        "colunas_fiscais": [
            "reg_0000_id",
            "reg_0200_id",
            "unid_conv",
            "fat_conv",
        ],
        "recomposicao_polars": "Aplicar fatores 0220 fora do banco para recompor unidades de referencia em produtos, NFe de entrada e estoque.",
    },
    "SPED.REG_H005": {
        "dominio": "efd_inventario_cabecalho_h005",
        "camada_bronze": "bronze/efd_reg_h005",
        "camada_silver": "silver/efd_inventario_cabecalho",
        "camada_gold": "gold/inventario_periodico",
        "chave_principal": ["id"],
        "chave_recorte": "reg_0000_id",
        "filtro_cnpj": "Filtrar pelos reg_0000_id validos do CNPJ",
        "filtro_temporal": "dt_inv e data_entrega <= :data_limite_processamento",
        "colunas_fiscais": [
            "id",
            "reg_0000_id",
            "dt_inv",
            "vl_inv",
        ],
        "recomposicao_polars": "Usar o cabecalho do inventario como ancora temporal para cruzar os itens H010 e a tributacao H020.",
    },
    "SPED.REG_H010": {
        "dominio": "efd_inventario_itens_h010",
        "camada_bronze": "bronze/efd_reg_h010",
        "camada_silver": "silver/efd_inventario_itens",
        "camada_gold": "gold/inventario_por_item",
        "chave_principal": ["reg_h005_id", "cod_item"],
        "chave_recorte": "reg_0000_id",
        "filtro_cnpj": "Filtrar pelos reg_0000_id validos do CNPJ",
        "filtro_temporal": "Herdado da ancora H005",
        "colunas_fiscais": [
            "reg_h005_id",
            "reg_0000_id",
            "cod_item",
            "qtd",
            "unid",
            "vl_item",
        ],
        "recomposicao_polars": "Cruzar o inventario itemizado com cadastro 0200/0220 para recompor saldo em unidade de referencia.",
    },
    "SPED.REG_H020": {
        "dominio": "efd_inventario_tributacao_h020",
        "camada_bronze": "bronze/efd_reg_h020",
        "camada_silver": "silver/efd_inventario_tributacao",
        "camada_gold": "gold/inventario_tributario",
        "chave_principal": ["reg_h010_id"],
        "chave_recorte": "reg_0000_id",
        "filtro_cnpj": "Filtrar pelos reg_0000_id validos do CNPJ",
        "filtro_temporal": "Herdado da ancora H005/H010",
        "colunas_fiscais": [
            "reg_h010_id",
            "reg_0000_id",
            "bc_icms",
            "vl_icms",
            "vl_icms_st",
        ],
        "recomposicao_polars": "Enriquecer o inventario com base e imposto para analises de estoque, ST e conciliacao de credito.",
    },
}

MAPA_DIMENSOES_FISCAIS: dict[str, list[str]] = {
    "cnpj_ie_uf": ["CNPJ", "IE", "UF", "CO_CAD_ICMS", "DESTINATARIO", "EMITENTE"],
    "documento_modelo_chave": ["CHAVE_ACESSO", "CHV_NFE", "MODELO", "MOD", "NUM_DOC", "SER"],
    "periodo_competencia": ["DT_INI", "DT_FIM", "DT_DOC", "DT_E_S", "DHEMI", "DATA_ENTREGA", "DT_INV"],
    "cfop": ["CFOP"],
    "cst_csosn": ["CST", "CSOSN"],
    "ncm_cest": ["NCM", "CEST"],
    "st_mva": ["ICMS_ST", "MVA", "BCST", "VST", "PST"],
    "difal_fcp": ["UFDEST", "UFREMET", "FCP", "DIFAL", "ICMSINTERPART"],
    "inventario_bloco_h": ["REG_H005", "REG_H010", "REG_H020", "BLOCO_H", "DT_INV"],
    "efd_blocos": ["REG_0000", "REG_C100", "REG_C170", "REG_0200", "REG_0205", "REG_0220", "EFD"],
}

MAPA_CATEGORIAS_DEMANDA: dict[str, list[str]] = {
    "performance SQL": ["ROW_NUMBER", "OVER(", "DISTINCT", "GROUP BY", "UNION", "UNION ALL"],
    "reconciliacao fiscal": ["CHAVE_ACESSO", "CHV_NFE", "REG_C100", "REG_C170", "NFE", "NFCE"],
    "apuracao tributaria": ["ICMS", "ST", "FCP", "DIFAL", "E111"],
    "obrigacao acessoria": ["SPED", "REG_", "EFD"],
    "auditoria ou monitoramento": ["COD_SIT", "STATUS", "OMISSAO", "VERIFICACAO", "MALHA"],
    "consolidacao por cnpj": [":CNPJ", "CNPJ_FILTRO", "CO_EMITENTE", "CO_DESTINATARIO", "R.CNPJ"],
    "geracao de dataset reutilizavel": ["REG_0200", "REG_0220", "BLOCO_H", "REG_0000"],
    "migracao de logica do banco para Polars": ["CASE", "ROW_NUMBER", "ORDER BY", "LEFT JOIN", "WITH "],
}

OBJETIVOS_POR_HEURISTICA: list[tuple[str, str]] = [
    ("BLOCO_H", "Reconstruir inventario declarado por CNPJ com rastreabilidade de quantidade, valor e tributacao."),
    ("REG_0200", "Isolar o cadastro fiscal de produtos por CNPJ e competencia para reuso nos demais pipelines."),
    ("REG_C170", "Recompor itens escriturados da EFD por documento e produto fora do banco."),
    ("REG_C100", "Consolidar cabecalhos fiscais da EFD para reconciliar documentos e itens por CNPJ."),
    ("NFE", "Consolidar documentos NF-e itemizados do contribuinte com prova fiscal completa."),
    ("NFCE", "Consolidar documentos NFC-e itemizados do contribuinte para analise de saidas e tributacao."),
    ("CTE", "Mapear documentos de transporte por CNPJ preservando chaves e valores tributarios."),
    ("DIF", "Isolar a logica de DIFAL/FCP em datasets reaproveitaveis por CNPJ e competencia."),
    ("C176", "Separar bases de ST e ressarcimento para reprocessamento controlado fora do Oracle."),
]


def _ler_sql(caminho_sql: Path) -> str:
    """Le o SQL bruto preservando o texto original para analise estrutural."""
    sql_template = caminho_sql.read_text(encoding="utf-8", errors="replace")
    fontes_oracle = obter_mapeamento_fontes_oracle()

    def substituir_placeholder(match: re.Match[str]) -> str:
        chave = match.group(1).strip().upper()
        return fontes_oracle.get(chave, match.group(0))

    return re.sub(r"\{\{\s*([A-Z0-9_]+)\s*\}\}", substituir_placeholder, sql_template)


def _remover_comentarios(sql: str) -> str:
    """Remove comentarios para facilitar a leitura estrutural via regex."""
    sql_sem_blocos = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--.*?$", " ", sql_sem_blocos, flags=re.MULTILINE)


def _normalizar_sql(sql: str) -> str:
    """Normaliza espacos e caixa alta para heuristicas de parsing."""
    sql_sem_comentarios = _remover_comentarios(sql)
    return re.sub(r"\s+", " ", sql_sem_comentarios).strip().upper()


def _normalizar_identificador(nome: str) -> str:
    """Normaliza identificador Oracle para comparacoes consistentes."""
    return nome.strip().strip(",").strip(")").strip("(").replace('"', "").upper()


def _extrair_ctes(sql_normalizado: str) -> set[str]:
    """Extrai nomes de CTEs para nao confundir com tabelas fisicas."""
    ctes = re.findall(r"(?:WITH|,)\s*([A-Z0-9_]+)\s+AS\s*\(", sql_normalizado)
    return {_normalizar_identificador(cte) for cte in ctes}


def _extrair_tabelas_raiz(sql_normalizado: str, ctes: set[str]) -> list[str]:
    """Extrai tabelas e views referenciadas em FROM/JOIN ignorando CTEs."""
    tabelas: list[str] = []

    for correspondencia in re.finditer(r"\b(?:FROM|JOIN)\s+([A-Z0-9_.$#\"]+)", sql_normalizado):
        tabela = _normalizar_identificador(correspondencia.group(1))
        if (
            not tabela
            or tabela in ctes
            or tabela == "DUAL"
            or tabela.startswith("SELECT")
        ):
            continue
        tabelas.append(tabela)

    tabelas_ordenadas = sorted(dict.fromkeys(tabelas))
    return tabelas_ordenadas


def _extrair_binds(sql_normalizado: str) -> list[str]:
    """Extrai binds Oracle declarados na SQL."""
    binds = re.findall(r":([A-Z0-9_]+)", sql_normalizado)
    return sorted(dict.fromkeys(binds))


def _detectar_dimensoes_fiscais(sql_normalizado: str) -> list[str]:
    """Detecta dimensoes fiscais relevantes para o contrato dos Parquets."""
    dimensoes: list[str] = []

    for nome_dimensao, termos in MAPA_DIMENSOES_FISCAIS.items():
        if any(termo in sql_normalizado for termo in termos):
            dimensoes.append(nome_dimensao)

    return dimensoes


def _classificar_categorias(sql_normalizado: str) -> list[str]:
    """Classifica a demanda conforme as heuristicas da skill fiscal."""
    categorias: list[str] = []

    for categoria, termos in MAPA_CATEGORIAS_DEMANDA.items():
        if any(termo in sql_normalizado for termo in termos):
            categorias.append(categoria)

    return categorias or ["geracao de dataset reutilizavel"]


def _inferir_objetivo_real(nome_arquivo: str, sql_normalizado: str) -> str:
    """Resume o objetivo analitico real da consulta em uma frase."""
    heuristica = f"{nome_arquivo.upper()} {sql_normalizado}"
    for termo, objetivo in OBJETIVOS_POR_HEURISTICA:
        if termo in heuristica:
            return objetivo

    return "Isolar fontes fiscais do CNPJ em blocos menores e auditaveis para recomposicao fora do Oracle."


def _detectar_gargalos(sql_normalizado: str, tabelas_raiz: list[str]) -> list[str]:
    """Diagnostica gargalos operacionais relevantes no banco."""
    gargalos: list[str] = []

    if "ROW_NUMBER" in sql_normalizado or " OVER(" in sql_normalizado:
        gargalos.append(
            "Uso de janela analitica para versionamento; vale extrair a semente de arquivos validos por CNPJ e competencia uma vez e reaproveitar nos demais datasets."
        )

    if len(tabelas_raiz) >= 3:
        gargalos.append(
            "Join de alto volume entre multiplas fontes; a recomendacao e extrair cada dominio em Parquet e recompor fora do banco com pruning por colunas."
        )

    if "DISTINCT" in sql_normalizado:
        gargalos.append(
            "DISTINCT aparente para corrigir duplicidade estrutural; validar cardinalidade das chaves antes de levar isso para a camada silver."
        )

    if "GROUP BY" in sql_normalizado:
        gargalos.append(
            "Agregacao ainda no Oracle; preferir granularidade documental no bronze/silver e totalizacao tardia no Polars."
        )

    if "CASE " in sql_normalizado:
        gargalos.append(
            "Regras fiscais embutidas em CASE WHEN; essas classificacoes devem migrar para funcoes explicitas no Polars com versao de regra rastreavel."
        )

    if "SELECT *" in sql_normalizado:
        gargalos.append(
            "Selecao ampla de colunas no banco; reduzir projecao na extracao para diminuir I/O sem perder campos de prova fiscal."
        )

    if not gargalos:
        gargalos.append(
            "Consulta estruturalmente simples, mas ainda candidata a extracao orientada a CNPJ para reuso, auditoria e desacoplamento do Oracle."
        )

    return gargalos


def _inferir_particoes(dominio: str, dimensoes_fiscais: list[str]) -> list[str]:
    """Define sugestao de particionamento considerando acesso por CNPJ e competencia."""
    particoes = ["cnpj_raiz"]

    if "periodo_competencia" in dimensoes_fiscais:
        particoes.extend(["ano", "mes"])

    if "documento_modelo_chave" in dimensoes_fiscais:
        particoes.append("modelo_documento")

    if "cnpj_ie_uf" in dimensoes_fiscais:
        particoes.append("uf")

    if "inventario_bloco_h" in dimensoes_fiscais and "tipo_movimento" not in particoes:
        particoes.append("tipo_movimento")

    return particoes


def _inferir_metadados_fonte(
    fonte_oracle: str,
    arquivos_sql: list[str],
    dimensoes_fiscais: list[str],
) -> dict[str, Any]:
    """Enriquece uma fonte raiz com contrato de extracao recomendado."""
    metadados_base = CATALOGO_FONTES_ORACLE.get(fonte_oracle, {})
    owner, objeto = (fonte_oracle.split(".", 1) + [""])[:2] if "." in fonte_oracle else ("", fonte_oracle)
    nome_objeto = objeto or owner
    dominio = metadados_base.get("dominio", f"oracle_{nome_objeto.lower()}")

    return {
        "fonte_oracle": fonte_oracle,
        "owner": owner,
        "objeto": nome_objeto,
        "dominio": dominio,
        "arquivos_sql": arquivos_sql,
        "camada_bronze": metadados_base.get("camada_bronze", f"bronze/{nome_objeto.lower()}"),
        "camada_silver": metadados_base.get("camada_silver", f"silver/{nome_objeto.lower()}"),
        "camada_gold": metadados_base.get("camada_gold", f"gold/{nome_objeto.lower()}"),
        "chave_principal": metadados_base.get("chave_principal", ["id_origem"]),
        "chave_recorte": metadados_base.get("chave_recorte", "cnpj"),
        "filtro_cnpj": metadados_base.get(
            "filtro_cnpj",
            "Aplicar :CNPJ diretamente quando a fonte expuser o contribuinte; caso contrario, semear IDs relacionais antes da extracao.",
        ),
        "filtro_temporal": metadados_base.get(
            "filtro_temporal",
            "Aplicar competencia e data_limite_processamento o mais cedo possivel.",
        ),
        "colunas_fiscais_obrigatorias": metadados_base.get(
            "colunas_fiscais",
            ["cnpj", "ie", "uf", "data_referencia", "id_origem"],
        ),
        "particoes_sugeridas": _inferir_particoes(dominio, dimensoes_fiscais),
        "paralelizavel": True,
        "justificativa_tecnica": (
            "Extrair esta fonte separadamente reduz joins prematuros no Oracle, preserva rastreabilidade documental e permite recomposicao lazy em Polars."
        ),
        "recomposicao_polars": metadados_base.get(
            "recomposicao_polars",
            "Ler a camada bronze com scan_parquet, normalizar chaves, aplicar filtros por CNPJ/competencia e recompor joins apenas no final do grafo lazy.",
        ),
    }


def _montar_blocos_extracao(fontes_raiz: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Consolida as fontes raiz em blocos logicos de extracao reutilizaveis."""
    blocos_por_dominio: dict[str, dict[str, Any]] = {}

    for fonte in fontes_raiz:
        dominio = fonte["dominio"]
        bloco = blocos_por_dominio.setdefault(
            dominio,
            {
                "nome_bloco": dominio,
                "fontes_oracle": [],
                "filtro_cnpj": fonte["filtro_cnpj"],
                "filtro_temporal": fonte["filtro_temporal"],
                "chave_principal": [],
                "colunas_fiscais_obrigatorias": [],
                "paralelizavel": True,
                "parquet_saida": fonte["camada_bronze"],
                "justificativa": fonte["justificativa_tecnica"],
            },
        )

        bloco["fontes_oracle"].append(fonte["fonte_oracle"])
        bloco["chave_principal"].extend(fonte["chave_principal"])
        bloco["colunas_fiscais_obrigatorias"].extend(fonte["colunas_fiscais_obrigatorias"])

    for bloco in blocos_por_dominio.values():
        bloco["fontes_oracle"] = sorted(dict.fromkeys(bloco["fontes_oracle"]))
        bloco["chave_principal"] = sorted(dict.fromkeys(bloco["chave_principal"]))
        bloco["colunas_fiscais_obrigatorias"] = sorted(dict.fromkeys(bloco["colunas_fiscais_obrigatorias"]))

    return sorted(blocos_por_dominio.values(), key=lambda item: item["nome_bloco"])


def _montar_estrategia_polars(fontes_raiz: list[dict[str, Any]]) -> list[str]:
    """Monta sequencia recomendada de recomposicao lazy no Polars."""
    dominios = {fonte["dominio"] for fonte in fontes_raiz}
    passos: list[str] = [
        "Ler cada camada bronze com scan_parquet aplicando pushdown de CNPJ, ano e mes antes de qualquer join.",
        "Selecionar apenas colunas minimas de prova fiscal, chaves documentais e indicadores tributarios para reduzir I/O.",
    ]

    if "efd_seed_arquivo" in dominios:
        passos.append(
            "Materializar primeiro a semente silver de REG_0000 validos por CNPJ/competencia para reutilizar reg_0000_id em C100, C170, 0200, 0220 e Bloco H."
        )

    if {"efd_documentos_c100", "efd_itens_c170"} & dominios:
        passos.append(
            "Recompor documentos e itens da EFD via joins por reg_0000_id e reg_c100_id somente depois de deduplicar e tipar as chaves relacionais."
        )

    if {"efd_cadastro_produtos_0200", "efd_fatores_conversao_0220"} & dominios:
        passos.append(
            "Enriquecer produtos com cadastro 0200, historico 0205 e fatores 0220 fora do banco para manter regras de unidade em funcoes versionadas."
        )

    if {"documentos_fiscais_nfe", "documentos_fiscais_nfce"} & dominios:
        passos.append(
            "Separar cabecalho e itens documentais de NF-e/NFC-e em silver e consolidar CFOP, CST/CSOSN, ST e FCP apenas na camada gold."
        )

    if {"efd_inventario_cabecalho_h005", "efd_inventario_itens_h010", "efd_inventario_tributacao_h020"} & dominios:
        passos.append(
            "Cruzar inventario H005/H010/H020 com cadastro de produtos somente no fim para preservar a rastreabilidade do saldo declarado."
        )

    passos.append(
        "Aplicar regras fiscais em funcoes Polars pequenas e nomeadas em portugues, mantendo materializacao apenas no collect final."
    )
    return passos


def analisar_mapeamento_raiz_sql_oracle(diretorio_sql: str | Path) -> dict[str, Any]:
    """Analisa um diretorio de SQLs e retorna mapa raiz de fontes Oracle."""
    pasta_sql = Path(diretorio_sql)
    if not pasta_sql.exists() or not pasta_sql.is_dir():
        raise FileNotFoundError(f"Diretorio SQL nao encontrado: {pasta_sql}")

    arquivos_analise: list[dict[str, Any]] = []
    fontes_por_objeto: dict[str, dict[str, Any]] = {}
    fontes_para_dimensoes: dict[str, set[str]] = defaultdict(set)
    fontes_para_arquivos: dict[str, list[str]] = defaultdict(list)

    for caminho_sql in sorted(pasta_sql.glob("*.sql")):
        sql_bruto = _ler_sql(caminho_sql)
        sql_normalizado = _normalizar_sql(sql_bruto)
        ctes = _extrair_ctes(sql_normalizado)
        tabelas_raiz = _extrair_tabelas_raiz(sql_normalizado, ctes)
        binds = _extrair_binds(sql_normalizado)
        dimensoes_fiscais = _detectar_dimensoes_fiscais(sql_normalizado)
        categorias = _classificar_categorias(sql_normalizado)
        gargalos = _detectar_gargalos(sql_normalizado, tabelas_raiz)
        objetivo_real = _inferir_objetivo_real(caminho_sql.stem, sql_normalizado)

        analise_arquivo = {
            "arquivo": caminho_sql.name,
            "caminho": str(caminho_sql),
            "objetivo_real": objetivo_real,
            "categorias": categorias,
            "ctes": sorted(ctes),
            "tabelas_raiz": tabelas_raiz,
            "binds": binds,
            "tem_bind_cnpj": "CNPJ" in binds,
            "tem_bind_periodo": any(bind.startswith("DATA") or bind.startswith("DT_") for bind in binds),
            "tem_window_function": " OVER(" in sql_normalizado,
            "tem_group_by": "GROUP BY" in sql_normalizado,
            "tem_distinct": "DISTINCT" in sql_normalizado,
            "tem_union": "UNION" in sql_normalizado,
            "dimensoes_fiscais": dimensoes_fiscais,
            "gargalos": gargalos,
        }
        arquivos_analise.append(analise_arquivo)

        for tabela_raiz in tabelas_raiz:
            fontes_para_dimensoes[tabela_raiz].update(dimensoes_fiscais)
            fontes_para_arquivos[tabela_raiz].append(caminho_sql.name)

    for fonte_oracle, arquivos in sorted(fontes_para_arquivos.items()):
        dimensoes = sorted(fontes_para_dimensoes[fonte_oracle])
        fontes_por_objeto[fonte_oracle] = _inferir_metadados_fonte(
            fonte_oracle=fonte_oracle,
            arquivos_sql=sorted(dict.fromkeys(arquivos)),
            dimensoes_fiscais=dimensoes,
        )
        fontes_por_objeto[fonte_oracle]["dimensoes_fiscais"] = dimensoes
        fontes_por_objeto[fonte_oracle]["ocorrencias"] = len(arquivos)

    fontes_raiz = sorted(fontes_por_objeto.values(), key=lambda item: item["fonte_oracle"])
    blocos_extracao = _montar_blocos_extracao(fontes_raiz)

    return {
        "diretorio_analisado": str(pasta_sql),
        "resumo": {
            "total_sqls": len(arquivos_analise),
            "total_fontes_raiz": len(fontes_raiz),
            "total_blocos_extracao": len(blocos_extracao),
            "total_sqls_com_bind_cnpj": sum(1 for item in arquivos_analise if item["tem_bind_cnpj"]),
            "total_sqls_com_window": sum(1 for item in arquivos_analise if item["tem_window_function"]),
        },
        "arquivos_sql": arquivos_analise,
        "fontes_raiz": fontes_raiz,
        "blocos_extracao": blocos_extracao,
        "estrategia_polars": _montar_estrategia_polars(fontes_raiz),
    }
