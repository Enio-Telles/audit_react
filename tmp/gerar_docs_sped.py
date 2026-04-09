import polars as pl
import os
from pathlib import Path

# Configurações
BASE_PATH = Path(r"c:\Sistema_react\dados\referencias\referencias\mapeamento\sped")
OUTPUT_DIR = Path(r"c:\Sistema_react\docs\mapeamento_sped")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Mapeamento de blocos e registros core
CORE_REGISTERS = {
    "0": ["0000", "0150", "0200", "0220"],
    "C": ["C100", "C113", "C170", "C176", "C190"],
    "D": ["D100", "D190"],
    "E": ["E110", "E210"],
    "H": ["H010"],
    "K": ["K100", "K200", "K230"]
}

BLOCK_NAMES = {
    "0": "Bloco 0 - Abertura, Identificação e Referências",
    "C": "Bloco C - Documentos Fiscais I (Mercadorias - ICMS/IPI)",
    "D": "Bloco D - Documentos Fiscais II (Serviços - ICMS)",
    "E": "Bloco E - Apuração do ICMS e do IPI",
    "H": "Bloco H - Inventário Físico",
    "K": "Bloco K - Controle da Produção e do Estoque"
}

def get_register_description(reg):
    # Dicionário simplificado para títulos de registro
    titles = {
        "0000": "Abertura do Arquivo Digital e Identificação da Entidade",
        "0150": "Tabela de Participantes",
        "0200": "Tabela de Identificação do Item (Produto e Serviços)",
        "0220": "Fatores de Conversão de Unidades",
        "C100": "Nota Fiscal (Código 01), Nota Fiscal Avulsa (Código 1B), Nota Fiscal de Produtor (Código 04), NF-e (Código 55) e NFC-e (Código 65)",
        "C113": "Documento Fiscal Referenciado",
        "C170": "Itens do Documento (Código 01, 1B, 04 e 55)",
        "C176": "Ressarcimento de ICMS e Fundo de Combate à Pobreza (FCP) em Operações com Substituição Tributária (Código 01, 55)",
        "C190": "Registro Analítico do Documento (Código 01, 1B, 04, 55 e 65)",
        "D100": "Nota Fiscal de Serviço de Transporte (Código 07) e CT-e (Código 57)",
        "D190": "Registro Analítico dos Documentos (Código 07, 08, 09, 10, 11, 26, 27, 57 e 67)",
        "E110": "Apuração do ICMS - Operações Próprias",
        "E210": "Apuração do ICMS - Substituição Tributária",
        "H010": "Inventário",
        "K100": "Período de Apuração do ICMS/IPI",
        "K200": "Estoque Escriturado",
        "K230": "Itens Produzidos"
    }
    return titles.get(reg, "Descrição não disponível")

def generate_markdown_for_block(block_id, registers):
    content = [f"# {BLOCK_NAMES[block_id]}\n"]
    
    for reg in registers:
        file_name = f"sped_reg_{reg}.parquet"
        file_path = BASE_PATH / file_name
        
        if not file_path.exists():
            # Tentar com prefixo minúsculo caso exista
            file_name_lower = f"sped_reg_{reg.lower()}.parquet"
            file_path = BASE_PATH / file_name_lower
            
        if not file_path.exists():
            print(f"Aviso: Arquivo não encontrado para o registro {reg}")
            continue
            
        try:
            df = pl.read_parquet(file_path)
            content.append(f"## Registro {reg} - {get_register_description(reg)}\n")
            
            # Formatar tabela
            # Colunas: Campo, Tipo, Descrição
            headers = ["Campo", "Tipo", "Descrição"]
            content.append(f"| {' | '.join(headers)} |")
            content.append(f"| {' | '.join(['---'] * len(headers))} |")
            
            # Ordenar por 'coluna' ou manter ordem original se possível
            # Aqui vamos assumir que a ordem no Parquet é a ordem do Guia
            for row in df.iter_rows(named=True):
                campo = row.get('coluna', 'N/A')
                tipo = row.get('tipo_dado', 'N/A')
                desc = row.get('descricao_consolidada', row.get('descricao_pdf', 'N/A'))
                
                # Limpar quebras de linha na descrição para não quebrar a tabela MD
                desc = str(desc).replace("\n", " ").replace("\r", " ").strip()
                
                content.append(f"| {campo} | {tipo} | {desc} |")
            
            content.append("\n---\n")
            
        except Exception as e:
            print(f"Erro ao processar {reg}: {e}")
            
    return "\n".join(content)

def main():
    for block_id, registers in CORE_REGISTERS.items():
        print(f"Processando Bloco {block_id}...")
        md_content = generate_markdown_for_block(block_id, registers)
        
        output_file = OUTPUT_DIR / f"bloco_{block_id.lower()}.md"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(md_content)
        print(f"Arquivo gerado: {output_file}")

if __name__ == "__main__":
    main()
