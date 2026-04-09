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
    ultimo_cache_key: str | None


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

    return ResumoContatoConvergencia(
        cnpj=cnpj,
        caminho_relatorio=str(caminho_relatorio),
        ultimo_status=extrair("Ultimo status de comparacao"),
        ultima_estrategia=extrair("Ultima estrategia executada"),
        ultima_sql_principal=extrair("Ultima SQL principal"),
        ultimo_cache_key=extrair("Ultimo cache comparado"),
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
        linhas.extend(
            [
                f"- Relatorio tecnico da secao contato: `{resumo_contato.caminho_relatorio}`",
                f"- Ultimo status da secao contato: `{resumo_contato.ultimo_status or 'nao informado'}`",
                f"- Ultima estrategia da secao contato: `{resumo_contato.ultima_estrategia or 'nao informado'}`",
                f"- Ultima SQL principal da secao contato: `{resumo_contato.ultima_sql_principal or 'nao informado'}`",
                f"- Ultimo cache comparado da secao contato: `{resumo_contato.ultimo_cache_key or 'nao informado'}`",
            ]
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

    linhas = [
        "# Relatorio Mestre de Convergencia do Plano",
        "",
        f"- Gerado em: `{datetime.now().isoformat()}`",
        f"- Comparacao fonte: `{comparacao_json}`",
        f"- CNPJs avaliados: `{total_cnpjs}`",
        f"- CNPJs com convergencia total: `{total_convergentes}`",
        f"- CNPJs com divergencia pendente: `{total_cnpjs - total_convergentes}`",
        "",
    ]

    for cnpj in cnpjs:
        evidencia_contato = diagnosticar_evidencia_contato(raiz_cnpj, cnpj)
        linhas.extend(_montar_secao_cnpj(cnpj, comparacoes_cnpjs[cnpj], evidencia_contato))

    conteudo = "\n".join(linhas) + "\n"
    caminho_saida.parent.mkdir(parents=True, exist_ok=True)
    caminho_saida.write_text(conteudo, encoding="utf-8")
    return conteudo
