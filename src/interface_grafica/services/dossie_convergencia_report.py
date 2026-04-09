from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import json
import re


@dataclass(frozen=True)
class ResumoContatoConvergencia:
    """Representa o recorte util do relatorio tecnico da secao contato para um CNPJ."""

    cnpj: str
    caminho_relatorio: str
    ultimo_status: str | None
    ultima_estrategia: str | None
    ultima_sql_principal: str | None
    ultima_estrategia_referencia: str | None
    ultima_sql_principal_referencia: str | None
    ultimo_cache_key: str | None
    ultimo_total_chaves_faltantes: str | None
    ultimo_total_chaves_extras: str | None
    ultima_amostra_chaves_faltantes: str | None
    ultima_amostra_chaves_extras: str | None
    deltas_campos_criticos: tuple[str, ...]


@dataclass(frozen=True)
class EstadoEvidenciaContato:
    """Representa o estado da evidencia da secao contato para um CNPJ no relatorio mestre."""

    status: str
    detalhe: str
    resumo: ResumoContatoConvergencia | None = None


def carregar_json(caminho_arquivo: Path) -> dict:
    """Carrega um arquivo JSON de apoio ao relatorio mestre."""

    return json.loads(caminho_arquivo.read_text(encoding="utf-8"))


def obter_caminho_relatorio_contato(raiz_cnpj: Path, cnpj: str) -> Path:
    """Resolve o caminho canonico do relatorio markdown da secao contato."""

    return raiz_cnpj / cnpj / "arquivos_parquet" / "dossie" / f"relatorio_comparacao_contato_{cnpj}.md"


def extrair_resumo_relatorio_contato(cnpj: str, caminho_relatorio: Path) -> ResumoContatoConvergencia:
    """Extrai os campos chave do markdown persistido da secao contato."""

    conteudo = caminho_relatorio.read_text(encoding="utf-8")

    def extrair(campo: str) -> str | None:
        padrao = rf"- {re.escape(campo)}: `([^`]+)`"
        correspondencia = re.search(padrao, conteudo)
        if not correspondencia:
            return None
        valor = correspondencia.group(1).strip()
        return None if valor == "nao informado" else valor

    deltas_campos_criticos = tuple(
        linha.removeprefix("- ").strip()
        for linha in conteudo.splitlines()
        if linha.startswith("- `") and " | referencia=`" in linha and " | delta=`" in linha
    )

    return ResumoContatoConvergencia(
        cnpj=cnpj,
        caminho_relatorio=str(caminho_relatorio),
        ultimo_status=extrair("Ultimo status de comparacao"),
        ultima_estrategia=extrair("Ultima estrategia executada"),
        ultima_sql_principal=extrair("Ultima SQL principal"),
        ultima_estrategia_referencia=extrair("Ultima estrategia de referencia"),
        ultima_sql_principal_referencia=extrair("Ultima SQL principal de referencia"),
        ultimo_cache_key=extrair("Ultimo cache comparado"),
        ultimo_total_chaves_faltantes=extrair("Ultimas chaves faltantes"),
        ultimo_total_chaves_extras=extrair("Ultimas chaves extras"),
        ultima_amostra_chaves_faltantes=extrair("Amostra de chaves faltantes"),
        ultima_amostra_chaves_extras=extrair("Amostra de chaves extras"),
        deltas_campos_criticos=deltas_campos_criticos,
    )


def diagnosticar_evidencia_contato(raiz_cnpj: Path, cnpj: str) -> EstadoEvidenciaContato:
    """Diagnostica se ja existe evidencia materializada da secao contato para o CNPJ."""

    pasta_dossie = raiz_cnpj / cnpj / "arquivos_parquet" / "dossie"
    if not pasta_dossie.exists():
        return EstadoEvidenciaContato(
            status="diretorio_dossie_ausente",
            detalhe="Diretorio do Dossie ainda nao materializado para o CNPJ.",
        )

    caminho_relatorio = obter_caminho_relatorio_contato(raiz_cnpj, cnpj)
    if not caminho_relatorio.exists():
        return EstadoEvidenciaContato(
            status="relatorio_contato_ausente",
            detalhe="Diretorio do Dossie existe, mas ainda nao ha relatorio tecnico da secao contato.",
        )

    return EstadoEvidenciaContato(
        status="relatorio_contato_disponivel",
        detalhe="Relatorio tecnico da secao contato encontrado.",
        resumo=extrair_resumo_relatorio_contato(cnpj, caminho_relatorio),
    )


def _montar_secao_cnpj(
    cnpj: str,
    comparacao_cnpj: dict,
    evidencia_contato: EstadoEvidenciaContato,
) -> list[str]:
    """Monta o trecho markdown de um CNPJ dentro do relatorio mestre."""

    if comparacao_cnpj.get("erro"):
        return [
            f"## CNPJ {cnpj}",
            "",
            f"- Erro de comparacao: `{comparacao_cnpj['erro']}`",
            "",
        ]

    somente_antes = comparacao_cnpj.get("somente_antes", [])
    somente_depois = comparacao_cnpj.get("somente_depois", [])
    alterados = comparacao_cnpj.get("alterados", [])
    convergencia_total = bool(comparacao_cnpj.get("convergencia_total"))

    linhas = [
        f"## CNPJ {cnpj}",
        "",
        f"- Convergencia total da extracao: `{'sim' if convergencia_total else 'nao'}`",
        f"- Arquivos somente no antes: `{len(somente_antes)}`",
        f"- Arquivos somente no depois: `{len(somente_depois)}`",
        f"- Arquivos alterados: `{len(alterados)}`",
    ]

    if evidencia_contato.resumo is not None:
        resumo_contato = evidencia_contato.resumo
        prioridade_contato = _classificar_prioridade_contato(resumo_contato)
        linhas.extend(
            [
                f"- Prioridade operacional da secao contato: `{prioridade_contato}`",
                f"- Relatorio tecnico da secao contato: `{resumo_contato.caminho_relatorio}`",
                f"- Ultimo status da secao contato: `{resumo_contato.ultimo_status or 'nao informado'}`",
                f"- Ultima estrategia da secao contato: `{resumo_contato.ultima_estrategia or 'nao informado'}`",
                f"- Ultima SQL principal da secao contato: `{resumo_contato.ultima_sql_principal or 'nao informado'}`",
                f"- Ultima estrategia de referencia da secao contato: `{resumo_contato.ultima_estrategia_referencia or 'nao informado'}`",
                f"- Ultima SQL de referencia da secao contato: `{resumo_contato.ultima_sql_principal_referencia or 'nao informado'}`",
                f"- Ultimo cache comparado da secao contato: `{resumo_contato.ultimo_cache_key or 'nao informado'}`",
                f"- Ultimas chaves faltantes da secao contato: `{resumo_contato.ultimo_total_chaves_faltantes or 'nao informado'}`",
                f"- Ultimas chaves extras da secao contato: `{resumo_contato.ultimo_total_chaves_extras or 'nao informado'}`",
            ]
        )
        if resumo_contato.ultima_amostra_chaves_faltantes:
            linhas.append(
                f"- Amostra de chaves faltantes da secao contato: `{resumo_contato.ultima_amostra_chaves_faltantes}`"
            )
        if resumo_contato.ultima_amostra_chaves_extras:
            linhas.append(
                f"- Amostra de chaves extras da secao contato: `{resumo_contato.ultima_amostra_chaves_extras}`"
            )
        if resumo_contato.deltas_campos_criticos:
            linhas.append("- Delta de campos criticos da secao contato:")
            linhas.extend(
                [f"  {linha}" for linha in resumo_contato.deltas_campos_criticos[:5]]
            )
    else:
        linhas.append(f"- Evidencia da secao contato: `{evidencia_contato.status}`")
        linhas.append(f"- Detalhe da secao contato: `{evidencia_contato.detalhe}`")

    if somente_antes:
        linhas.append(f"- Exemplo de arquivo ausente no depois: `{somente_antes[0]}`")
    if somente_depois:
        linhas.append(f"- Exemplo de arquivo novo no depois: `{somente_depois[0]}`")
    if alterados:
        primeiro_alterado = alterados[0]
        linhas.append(f"- Exemplo de arquivo alterado: `{primeiro_alterado.get('caminho_relativo', 'nao informado')}`")

    linhas.append("")
    return linhas


def _classificar_prioridade_contato(resumo: ResumoContatoConvergencia) -> str:
    """Define a prioridade operacional da divergencia de contato por CNPJ."""

    if resumo.ultimo_status == "divergencia_funcional":
        return "alta"
    if resumo.ultimo_status == "divergencia_basica":
        return "media"
    if resumo.ultimo_status == "convergencia_basica":
        return "monitorar"
    if resumo.ultimo_status == "convergencia_funcional":
        return "baixa"
    return "indeterminada"


def _peso_prioridade_contato(prioridade: str) -> int:
    """Converte a prioridade textual em peso para ordenacao operacional."""

    pesos = {
        "alta": 0,
        "media": 1,
        "monitorar": 2,
        "baixa": 3,
        "indeterminada": 4,
    }
    return pesos.get(prioridade, 99)


def gerar_relatorio_mestre_convergencia(
    comparacao_json: Path,
    raiz_cnpj: Path,
    caminho_saida: Path,
) -> str:
    """Gera um markdown mestre para acompanhar convergencia por CNPJ do plano."""

    comparacao = carregar_json(comparacao_json)
    comparacoes_cnpjs = comparacao.get("cnpjs", {})
    cnpjs = list(comparacoes_cnpjs.keys())

    total_cnpjs = len(cnpjs)
    total_convergentes = sum(
        1 for dados_cnpj in comparacoes_cnpjs.values() if isinstance(dados_cnpj, dict) and dados_cnpj.get("convergencia_total") is True
    )
    prioridades_contato = {"alta": 0, "media": 0, "monitorar": 0, "baixa": 0, "indeterminada": 0}
    for cnpj in cnpjs:
        evidencia_contato = diagnosticar_evidencia_contato(raiz_cnpj, cnpj)
        if evidencia_contato.resumo is None:
            prioridades_contato["indeterminada"] += 1
            continue
        prioridades_contato[_classificar_prioridade_contato(evidencia_contato.resumo)] += 1

    linhas = [
        "# Relatorio Mestre de Convergencia do Plano",
        "",
        f"- Gerado em: `{datetime.now().isoformat()}`",
        f"- Comparacao fonte: `{comparacao_json}`",
        f"- CNPJs avaliados: `{total_cnpjs}`",
        f"- CNPJs com convergencia total: `{total_convergentes}`",
        f"- CNPJs com divergencia pendente: `{total_cnpjs - total_convergentes}`",
        f"- Prioridade alta na secao contato: `{prioridades_contato['alta']}`",
        f"- Prioridade media na secao contato: `{prioridades_contato['media']}`",
        f"- Prioridade monitorar na secao contato: `{prioridades_contato['monitorar']}`",
        "",
    ]

    for cnpj in cnpjs:
        evidencia_contato = diagnosticar_evidencia_contato(raiz_cnpj, cnpj)
        linhas.extend(_montar_secao_cnpj(cnpj, comparacoes_cnpjs[cnpj], evidencia_contato))

    conteudo = "\n".join(linhas) + "\n"
    caminho_saida.parent.mkdir(parents=True, exist_ok=True)
    caminho_saida.write_text(conteudo, encoding="utf-8")
    return conteudo


def gerar_painel_prioridades_contato(
    comparacao_json: Path,
    raiz_cnpj: Path,
    caminho_saida: Path,
) -> str:
    """Gera um painel markdown ranqueando os CNPJs pela prioridade da secao contato."""

    comparacao = carregar_json(comparacao_json)
    comparacoes_cnpjs = comparacao.get("cnpjs", {})
    itens_priorizados: list[tuple[int, str, list[str]]] = []

    for cnpj in comparacoes_cnpjs:
        evidencia_contato = diagnosticar_evidencia_contato(raiz_cnpj, cnpj)
        if evidencia_contato.resumo is None:
            prioridade = "indeterminada"
            resumo = None
        else:
            resumo = evidencia_contato.resumo
            prioridade = _classificar_prioridade_contato(resumo)

        faltantes = resumo.ultimo_total_chaves_faltantes if resumo else None
        extras = resumo.ultimo_total_chaves_extras if resumo else None
        status = resumo.ultimo_status if resumo else evidencia_contato.status
        estrategia_atual = resumo.ultima_estrategia if resumo else None
        estrategia_referencia = resumo.ultima_estrategia_referencia if resumo else None
        amostra_faltante = resumo.ultima_amostra_chaves_faltantes if resumo else None

        linha_tabela = (
            f"| `{cnpj}` | `{prioridade}` | `{status or 'nao informado'}` | "
            f"`{estrategia_atual or 'nao informado'}` | `{estrategia_referencia or 'nao informado'}` | "
            f"`{faltantes or '0'}` | `{extras or '0'}` | "
            f"`{amostra_faltante or 'nao informado'}` |"
        )
        detalhes = [
            f"### CNPJ {cnpj}",
            "",
            f"- Prioridade da secao contato: `{prioridade}`",
            f"- Status atual da comparacao: `{status or 'nao informado'}`",
            f"- Estrategia atual: `{estrategia_atual or 'nao informado'}`",
            f"- Estrategia de referencia: `{estrategia_referencia or 'nao informado'}`",
            f"- Chaves faltantes / extras: `{faltantes or '0'} / {extras or '0'}`",
            f"- Relatorio tecnico: `{resumo.caminho_relatorio if resumo else 'nao informado'}`",
        ]
        if amostra_faltante:
            detalhes.append(f"- Amostra de chave faltante: `{amostra_faltante}`")
        if resumo and resumo.deltas_campos_criticos:
            detalhes.append("- Delta de campos criticos:")
            detalhes.extend([f"  - {linha}" for linha in resumo.deltas_campos_criticos[:3]])
        detalhes.append("")
        itens_priorizados.append((_peso_prioridade_contato(prioridade), cnpj, [linha_tabela, *detalhes]))

    itens_priorizados.sort(key=lambda item: (item[0], item[1]))

    linhas = [
        "# Painel de Prioridades da Secao Contato",
        "",
        f"- Gerado em: `{datetime.now().isoformat()}`",
        f"- Comparacao fonte: `{comparacao_json}`",
        f"- Total de CNPJs avaliados: `{len(comparacoes_cnpjs)}`",
        "",
        "| CNPJ | Prioridade | Status | Estrategia atual | Estrategia referencia | Faltantes | Extras | Exemplo de chave faltante |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for _, _, bloco in itens_priorizados:
        linhas.append(bloco[0])
    linhas.append("")
    linhas.append("## Detalhamento")
    linhas.append("")
    for _, _, bloco in itens_priorizados:
        linhas.extend(bloco[1:])

    conteudo = "\n".join(linhas) + "\n"
    caminho_saida.parent.mkdir(parents=True, exist_ok=True)
    caminho_saida.write_text(conteudo, encoding="utf-8")
    return conteudo
