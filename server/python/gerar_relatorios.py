"""
Gerador de Relatórios Fiscais Conclusivos - Fisconforme Não Cumprido
Gera PDFs profissionais por CNPJ e une com as notificações DET.
Também gera um Relatório Geral Consolidado com todos os CNPJs.
"""
import os
import json
from pathlib import Path
from weasyprint import HTML

# Configurações de caminhos
BASE_DIR = Path("/home/ubuntu/audit_react_mock/storage")
CONFIG_DIR = BASE_DIR / "_config"
CNPJ_DIR = BASE_DIR / "CNPJ"
OUTPUT_DIR = Path("/home/ubuntu/relatorios_finais")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def obter_pdf_writer():
    """Carrega PdfWriter apenas quando a mesclagem de PDFs for executada."""
    try:
        from pypdf import PdfWriter
    except ModuleNotFoundError as erro:
        raise RuntimeError(
            "Biblioteca opcional ausente para mesclar PDFs: instale 'pypdf' no ambiente Python ativo."
        ) from erro

    return PdfWriter


# ============================================================
# CSS COMPARTILHADO
# ============================================================
CSS_BASE = """
    @page {
        size: A4;
        margin: 1.8cm 2.2cm 2.2cm 2.2cm;
        @bottom-center {
            content: "SEFIN/CRE/GEFIS - Gerência de Fiscalização | Av. Farquar, nº 2986 - Palácio Rio Madeira - Porto Velho/RO";
            font-size: 7pt;
            color: #888;
        }
        @bottom-right {
            content: "Página " counter(page);
            font-size: 8pt;
            color: #888;
        }
    }

    * { margin: 0; padding: 0; box-sizing: border-box; }

    body {
        font-family: 'Noto Sans', 'DejaVu Sans', Arial, Helvetica, sans-serif;
        font-size: 10.5pt;
        line-height: 1.45;
        color: #222;
    }

    .header {
        text-align: center;
        border-bottom: 2.5px solid #154360;
        padding-bottom: 8px;
        margin-bottom: 12px;
    }
    .header h1 {
        font-size: 13pt; color: #154360; margin: 0;
        letter-spacing: 1.5px; text-transform: uppercase;
    }
    .header h2 {
        font-size: 10.5pt; color: #2c3e50; margin: 2px 0; font-weight: 400;
    }

    .titulo-relatorio {
        background: #154360; color: #fff; text-align: center;
        padding: 7px 15px; margin: 10px 0;
        font-size: 12.5pt; font-weight: 700;
        letter-spacing: 2.5px; text-transform: uppercase;
    }

    .dados-contribuinte {
        background-color: #f4f6f7;
        border-left: 3.5px solid #154360;
        padding: 8px 12px; margin: 10px 0;
    }
    .dados-contribuinte table { width: 100%; border-collapse: collapse; }
    .dados-contribuinte td { padding: 2px 6px; vertical-align: top; font-size: 10pt; }
    .dados-contribuinte td.label {
        font-weight: 700; color: #154360; width: 145px; white-space: nowrap;
    }

    .secao { margin: 10px 0 5px 0; }
    .secao-titulo {
        background-color: #d6eaf8; border-left: 3.5px solid #2980b9;
        padding: 4px 10px; font-size: 11pt; font-weight: 700;
        color: #154360; margin-bottom: 5px;
    }
    .secao-corpo { text-align: justify; margin: 4px 0 8px 0; padding: 0 2px; }
    .secao-corpo p { margin: 4px 0; }

    .lista-procedimentos { margin: 4px 0; padding-left: 18px; }
    .lista-procedimentos li { margin-bottom: 2px; font-size: 10pt; }
    .lista-procedimentos li::marker { color: #2980b9; }

    .manifestacao-box {
        border: 1px solid #d5dbdb; padding: 6px 10px;
        margin: 5px 0; background-color: #fdfefe; font-size: 10pt;
    }
    .manifestacao-item { margin: 2px 0; }
    .check {
        display: inline-block; width: 13px; height: 13px;
        border: 1.5px solid #2980b9; text-align: center;
        line-height: 11px; margin-right: 5px; font-size: 9pt;
        vertical-align: middle;
    }
    .check.marcado { background-color: #2980b9; color: #fff; font-weight: 700; }

    .decisao-box {
        background-color: #fef9e7; border: 1px solid #f9e79f;
        border-left: 3.5px solid #f39c12;
        padding: 7px 10px; margin: 5px 0; font-weight: 700; font-size: 10pt;
    }
    .desfecho-box {
        background-color: #fdedec; border: 1px solid #f5b7b1;
        border-left: 3.5px solid #e74c3c;
        padding: 7px 10px; margin: 5px 0; font-size: 10pt;
    }

    /* ===== ASSINATURA COM ESPAÇO PARA ASSINATURA DIGITAL ===== */
    .assinatura {
        text-align: center;
        margin-top: 25px;
        padding-top: 8px;
    }
    .assinatura .local-data {
        text-align: right; margin-bottom: 10px;
        font-style: italic; font-size: 10pt;
    }
    .assinatura .espaco-assinatura {
        height: 80px;
        margin: 0 auto;
        max-width: 350px;
        border: 1px dashed #bbb;
        border-radius: 6px;
        display: flex;
        align-items: center;
        justify-content: center;
        color: #bbb;
        font-size: 8pt;
        font-style: italic;
    }
    .assinatura .linha {
        display: inline-block;
        border-top: 1px solid #444;
        min-width: 300px;
        padding-top: 4px;
        margin-top: 5px;
    }
    .assinatura .nome { font-weight: 700; font-size: 11pt; color: #154360; }
    .assinatura .cargo, .assinatura .matricula { font-size: 9.5pt; color: #555; }
    .assinatura .orgao { font-size: 8.5pt; color: #777; margin-top: 2px; }

    /* ===== RELATÓRIO GERAL ===== */
    .subtitulo-geral {
        font-size: 10pt; text-align: center;
        color: #555; margin-bottom: 10px;
    }
    .tabela-resumo {
        width: 100%; border-collapse: collapse; margin: 10px 0;
        font-size: 9pt;
    }
    .tabela-resumo th {
        background-color: #154360; color: #fff;
        padding: 6px 8px; text-align: left; font-weight: 600;
        border: 1px solid #154360;
    }
    .tabela-resumo td {
        padding: 5px 8px; border: 1px solid #ddd;
        vertical-align: top;
    }
    .tabela-resumo tr:nth-child(even) { background-color: #f8f9fa; }
    .tabela-resumo tr:hover { background-color: #eaf2f8; }

    .numero-ordem {
        display: inline-block;
        background-color: #154360; color: #fff;
        width: 22px; height: 22px; border-radius: 50%;
        text-align: center; line-height: 22px;
        font-size: 10pt; font-weight: 700;
        margin-right: 8px; vertical-align: middle;
    }

    .separador-cnpj {
        page-break-before: always;
    }

    .resumo-individual {
        border: 1px solid #ddd;
        border-radius: 4px;
        padding: 10px 14px;
        margin: 8px 0;
        background-color: #fafafa;
    }
    .resumo-individual p { margin: 3px 0; font-size: 10pt; }
    .resumo-individual .campo-label { font-weight: 700; color: #154360; }
"""


# ============================================================
# CABEÇALHO HTML INSTITUCIONAL
# ============================================================
def html_cabecalho():
    return """
<div class="header">
    <h1>Governo do Estado de Rondônia</h1>
    <h2>Secretaria de Estado de Finanças</h2>
    <h2>Coordenadoria da Receita Estadual</h2>
</div>"""


# ============================================================
# BLOCO DE ASSINATURA COM ESPAÇO PARA ASSINATURA DIGITAL
# ============================================================
def html_assinatura(auditor: dict):
    return f"""
<div class="assinatura">
    <div class="local-data">{auditor['local_data']}</div>
    <div class="espaco-assinatura">Espaço reservado para assinatura digital</div>
    <div class="linha">
        <div class="nome">{auditor['nome']}</div>
        <div class="cargo">{auditor['cargo']}</div>
        <div class="matricula">Matrícula: {auditor['matricula']}</div>
    </div>
</div>"""


# ============================================================
# RELATÓRIO INDIVIDUAL POR CNPJ
# ============================================================
def gerar_html_individual(empresa: dict, auditor: dict) -> str:
    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head><meta charset="UTF-8"><style>{CSS_BASE}</style></head>
<body>

{html_cabecalho()}

<div class="titulo-relatorio">Relatório Fiscal Conclusivo</div>

<div class="dados-contribuinte">
    <table>
        <tr><td class="label">Contribuinte:</td><td><strong>{empresa['contribuinte']}</strong></td></tr>
        <tr><td class="label">CNPJ:</td><td>{empresa['cnpj']}</td></tr>
        <tr><td class="label">Inscrição Estadual:</td><td>{empresa['ie']}</td></tr>
        <tr><td class="label">DSF:</td><td>{empresa['dsf']}</td></tr>
        <tr><td class="label">Notificação DET nº:</td><td>{empresa['notificacao_det']}</td></tr>
    </table>
</div>

<div class="secao">
    <div class="secao-titulo">1. Identificação da Ação Fiscal</div>
    <div class="secao-corpo">
        <p>Trata-se de ação fiscal preliminar decorrente de Designação de Serviço Fiscal (DSF), 
        no âmbito do trabalho de Acervo do sistema Fisconforme, cujo objetivo consistiu na análise 
        e cobrança de regularização de pendências fiscais classificadas como prioritárias no painel 
        <strong>"Fisconforme Não Atendido"</strong>.</p>
        <p>As pendências decorrem de inconsistências detectadas por malhas fiscais automatizadas 
        relativas ao cumprimento de obrigações tributárias acessórias.</p>
    </div>
</div>

<div class="secao">
    <div class="secao-titulo">2. Procedimentos Realizados</div>
    <div class="secao-corpo">
        <p>Foram adotadas as seguintes providências:</p>
        <ul class="lista-procedimentos">
            <li>Consulta ao Sismonitora para identificação das pendências e status;</li>
            <li>Verificação cadastral e fiscal do contribuinte no sistema Visão 360º;</li>
            <li>Conferência do status das inconsistências no sistema Fisconforme;</li>
            <li>Verificação da existência de monitoramentos fiscais relacionados;</li>
            <li>Emissão de notificação formal via DET, fixando prazo para regularização.</li>
        </ul>
    </div>
</div>

<div class="secao">
    <div class="secao-titulo">3. Manifestação do Contribuinte</div>
    <div class="secao-corpo">
        <p>Após a notificação, verificou-se que o contribuinte:</p>
        <div class="manifestacao-box">
            <div class="manifestacao-item"><span class="check">&nbsp;</span> Regularizou integralmente as pendências</div>
            <div class="manifestacao-item"><span class="check">&nbsp;</span> Apresentou contestação</div>
            <div class="manifestacao-item"><span class="check">&nbsp;</span> Solicitou prorrogação de prazo</div>
            <div class="manifestacao-item"><span class="check marcado">X</span> <strong>Não apresentou manifestação</strong></div>
        </div>
        <p>{empresa['contatos_realizados']}</p>
    </div>
</div>

<div class="secao">
    <div class="secao-titulo">4. Análise Fiscal</div>
    <div class="secao-corpo">
        <p>A análise observou os princípios de uniformidade procedimental e vedação à contestação 
        parcial das pendências. Foram examinadas as justificativas apresentadas e confrontadas com 
        os dados fiscais disponíveis, tendo sido adotadas as seguintes decisões:</p>
        <div class="decisao-box">{empresa['decisao_fiscal']}</div>
    </div>
</div>

<div class="secao">
    <div class="secao-titulo">5. Situação Final das Pendências</div>
    <div class="secao-corpo">
        <p>Após a conclusão da ação fiscal, as pendências apresentaram os seguintes desfechos:</p>
        <div class="desfecho-box">{empresa['desfecho']}</div>
    </div>
</div>

<div class="secao">
    <div class="secao-titulo">6. Conclusão</div>
    <div class="secao-corpo">
        <p>Diante do exposto, considera-se concluída a presente ação fiscal preliminar, permanecendo 
        as pendências não regularizadas sujeitas à adoção das medidas fiscais cabíveis, nos termos 
        da legislação vigente.</p>
    </div>
</div>

{html_assinatura(auditor)}

</body>
</html>"""


# ============================================================
# RELATÓRIO GERAL CONSOLIDADO
# ============================================================
def gerar_html_geral(empresas: list, auditor: dict) -> str:
    # Tabela resumo
    linhas_tabela = ""
    for i, emp in enumerate(empresas, 1):
        linhas_tabela += f"""
        <tr>
            <td style="text-align:center; font-weight:700;">{i}</td>
            <td>{emp['contribuinte']}</td>
            <td>{emp['cnpj']}</td>
            <td>{emp['ie']}</td>
            <td>{emp['notificacao_det']}</td>
            <td>{emp['manifestacao']}</td>
        </tr>"""

    # Blocos detalhados por CNPJ
    blocos_cnpj = ""
    for i, emp in enumerate(empresas, 1):
        separador = 'class="separador-cnpj"' if i > 1 else ''
        blocos_cnpj += f"""
<div {separador}>
    <div class="secao-titulo">
        <span class="numero-ordem">{i}</span> {emp['contribuinte']}
    </div>
    <div class="resumo-individual">
        <p><span class="campo-label">CNPJ:</span> {emp['cnpj']}</p>
        <p><span class="campo-label">Inscrição Estadual:</span> {emp['ie']}</p>
        <p><span class="campo-label">DSF:</span> {emp['dsf']}</p>
        <p><span class="campo-label">Notificação DET nº:</span> {emp['notificacao_det']}</p>
    </div>

    <div class="secao" style="margin-top:8px;">
        <div style="font-weight:700; color:#154360; margin-bottom:3px;">Contatos e Manifestação:</div>
        <div class="secao-corpo">
            <p>{emp['contatos_realizados']}</p>
        </div>
    </div>

    <div class="secao">
        <div style="font-weight:700; color:#154360; margin-bottom:3px;">Decisão Fiscal:</div>
        <div class="decisao-box">{emp['decisao_fiscal']}</div>
    </div>

    <div class="secao">
        <div style="font-weight:700; color:#154360; margin-bottom:3px;">Situação Final:</div>
        <div class="desfecho-box">{emp['desfecho']}</div>
    </div>
</div>
"""

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head><meta charset="UTF-8"><style>{CSS_BASE}</style></head>
<body>

{html_cabecalho()}

<div class="titulo-relatorio">Relatório Geral Consolidado</div>

<div class="subtitulo-geral">
    Fisconforme Não Cumprido — DSF nº {empresas[0]['dsf']}<br>
    Total de contribuintes analisados: <strong>{len(empresas)}</strong>
</div>

<div class="secao">
    <div class="secao-titulo">1. Objeto</div>
    <div class="secao-corpo">
        <p>O presente relatório consolida os resultados da ação fiscal preliminar realizada no âmbito 
        da Designação de Serviço Fiscal (DSF) nº {empresas[0]['dsf']}, referente ao trabalho de 
        Acervo do sistema Fisconforme, com foco nas pendências classificadas como prioritárias no 
        painel <strong>"Fisconforme Não Atendido"</strong>.</p>
    </div>
</div>

<div class="secao">
    <div class="secao-titulo">2. Quadro Resumo dos Contribuintes</div>
    <div class="secao-corpo">
        <table class="tabela-resumo">
            <thead>
                <tr>
                    <th style="width:30px; text-align:center;">Nº</th>
                    <th>Contribuinte</th>
                    <th style="width:120px;">CNPJ</th>
                    <th style="width:115px;">IE</th>
                    <th style="width:80px;">DET nº</th>
                    <th style="width:110px;">Manifestação</th>
                </tr>
            </thead>
            <tbody>
                {linhas_tabela}
            </tbody>
        </table>
    </div>
</div>

<div class="secao">
    <div class="secao-titulo">3. Procedimentos Comuns</div>
    <div class="secao-corpo">
        <p>Para todos os contribuintes listados, foram adotadas as seguintes providências:</p>
        <ul class="lista-procedimentos">
            <li>Consulta ao Sismonitora para identificação das pendências e status;</li>
            <li>Verificação cadastral e fiscal do contribuinte no sistema Visão 360º;</li>
            <li>Conferência do status das inconsistências no sistema Fisconforme;</li>
            <li>Verificação da existência de monitoramentos fiscais relacionados;</li>
            <li>Emissão de notificação formal via DET, fixando prazo para regularização;</li>
            <li>Tentativa de contato por e-mail, WhatsApp e telefone.</li>
        </ul>
    </div>
</div>

<div class="secao">
    <div class="secao-titulo">4. Análise Individual por Contribuinte</div>
</div>

{blocos_cnpj}

<div class="separador-cnpj">
    <div class="secao">
        <div class="secao-titulo">5. Conclusão Geral</div>
        <div class="secao-corpo">
            <p>Diante do exposto, conclui-se que dos <strong>{len(empresas)}</strong> contribuintes 
            notificados no âmbito da DSF nº {empresas[0]['dsf']}, nenhum promoveu a regularização 
            integral das pendências apontadas pelo sistema Fisconforme dentro do prazo estabelecido 
            nas respectivas notificações DET.</p>
            <p>Recomenda-se o encaminhamento para as providências fiscais cabíveis, nos termos da 
            legislação vigente, em relação aos contribuintes que permanecem com pendências ativas.</p>
        </div>
    </div>

    {html_assinatura(auditor)}
</div>

</body>
</html>"""


# ============================================================
# FUNÇÕES DE GERAÇÃO E MESCLAGEM
# ============================================================
def gerar_pdf_cnpj(empresa: dict, auditor: dict) -> str:
    html_content = gerar_html_individual(empresa, auditor)
    temp_pdf_path = f"/tmp/relatorio_{empresa['cnpj']}.pdf"
    HTML(string=html_content).write_pdf(temp_pdf_path)
    return temp_pdf_path


def gerar_pdf_geral(empresas: list, auditor: dict) -> str:
    html_content = gerar_html_geral(empresas, auditor)
    temp_pdf_path = "/tmp/relatorio_geral.pdf"
    HTML(string=html_content).write_pdf(temp_pdf_path)
    return temp_pdf_path


def mesclar_pdfs(pdfs: list, output_path: str):
    """Mescla uma lista de PDFs em um único arquivo."""
    classe_pdf_writer = obter_pdf_writer()
    merger = classe_pdf_writer()
    for pdf in pdfs:
        if os.path.exists(pdf):
            merger.append(pdf)
        else:
            print(f"  AVISO: PDF não encontrado: {pdf}")
    with open(output_path, "wb") as fout:
        merger.write(fout)


# ============================================================
# MAIN
# ============================================================
def main():
    with open(CONFIG_DIR / "auditor.json", "r", encoding="utf-8") as f:
        auditor = json.load(f)

    print("=" * 70)
    print("  GERAÇÃO DE RELATÓRIOS FISCAIS CONCLUSIVOS")
    print("  Fisconforme Não Cumprido")
    print("=" * 70)
    print(f"\n  Auditor: {auditor['nome']}")
    print(f"  Matrícula: {auditor['matricula']}")
    print()

    # Carregar todas as empresas
    empresas = []
    cnpj_dirs = sorted(CNPJ_DIR.iterdir())
    for cnpj_dir in cnpj_dirs:
        if not cnpj_dir.is_dir():
            continue
        dados_path = cnpj_dir / "relatorio" / "dados.json"
        if not dados_path.exists():
            continue
        with open(dados_path, "r", encoding="utf-8") as f:
            empresas.append(json.load(f))

    # ---- RELATÓRIOS INDIVIDUAIS ----
    print("  [INDIVIDUAIS]")
    for empresa in empresas:
        cnpj = empresa['cnpj']
        nome_curto = empresa['contribuinte'].split()[0]

        print(f"    [{cnpj}] {empresa['contribuinte']}")

        pdf_temp = gerar_pdf_cnpj(empresa, auditor)

        output_filename = f"Relatorio_Final_{nome_curto}_{cnpj}.pdf"
        output_path = OUTPUT_DIR / output_filename
        mesclar_pdfs([pdf_temp, empresa['pdf_det']], str(output_path))
        print(f"      -> {output_path}")

        # Cópia na pasta do CNPJ
        cnpj_output = CNPJ_DIR / cnpj / "relatorio" / output_filename
        mesclar_pdfs([pdf_temp, empresa['pdf_det']], str(cnpj_output))

        if os.path.exists(pdf_temp):
            os.remove(pdf_temp)

    # ---- RELATÓRIO GERAL CONSOLIDADO ----
    print(f"\n  [GERAL CONSOLIDADO] ({len(empresas)} contribuintes)")
    pdf_geral = gerar_pdf_geral(empresas, auditor)

    # Unir: Relatório Geral + todos os DETs
    pdfs_para_unir = [pdf_geral]
    for empresa in empresas:
        pdfs_para_unir.append(empresa['pdf_det'])

    output_geral = OUTPUT_DIR / "Relatorio_Geral_Consolidado_Fisconforme.pdf"
    mesclar_pdfs(pdfs_para_unir, str(output_geral))
    print(f"    -> {output_geral}")

    if os.path.exists(pdf_geral):
        os.remove(pdf_geral)

    print(f"\n  Todos os relatórios em: {OUTPUT_DIR}")
    print("=" * 70)


if __name__ == "__main__":
    main()
