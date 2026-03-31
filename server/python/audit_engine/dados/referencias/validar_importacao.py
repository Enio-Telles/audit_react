#!/usr/bin/env python3
"""Script de validação das tabelas de referência importadas."""

import sys
from pathlib import Path

# Adicionar o diretório audit_engine ao path
AUDIT_ENGINE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(AUDIT_ENGINE_DIR))

import polars as pl
from utils.referencias import (
    carregar_ncm,
    carregar_cest,
    carregar_cfop,
    carregar_cst,
    carregar_dominios_nfe,
    carregar_mapeamento_nfe,
    carregar_dominios_eventos_nfe,
    carregar_malhas_fisconforme,
    validar_ncm,
    validar_cest,
    validar_cfop,
)


def validar_tabela(nome: str, df: pl.DataFrame, minimo: int = 0) -> bool:
    """Valida se tabela tem registros suficientes."""
    if df.is_empty():
        print(f"❌ {nome}: VAZIA")
        return False
    
    total = len(df)
    if total < minimo:
        print(f"⚠️  {nome}: {total} registros (abaixo do mínimo {minimo})")
        return False
    
    print(f"✅ {nome}: {total:,} registros")
    return True


def main():
    """Executa validação completa."""
    print("=" * 60)
    print("VALIDAÇÃO DE TABELAS DE REFERÊNCIA")
    print("=" * 60)
    print()
    
    resultados = []
    
    # NCM
    print("📚 NCM (Nomenclatura Comum do Mercosul)")
    print("-" * 40)
    df_ncm = carregar_ncm()
    resultados.append(validar_tabela("NCM", df_ncm, minimo=10000))
    
    if not df_ncm.is_empty():
        print(f"   Colunas: {', '.join(df_ncm.columns[:5])}...")
        
        # Testar busca
        if validar_ncm("01012100"):
            print("   ✅ Busca por NCM funcional")
        else:
            print("   ⚠️  Busca por NCM pode ter problemas")
    print()
    
    # CEST
    print("🏷️  CEST (Código Especificador da ST)")
    print("-" * 40)
    df_cest = carregar_cest()
    resultados.append(validar_tabela("CEST", df_cest, minimo=1000))
    
    if not df_cest.is_empty():
        print(f"   Colunas: {', '.join(df_cest.columns)}")
        
        # Testar validação
        if validar_cest("0100140"):
            print("   ✅ Validação de CEST funcional")
    print()
    
    # CFOP
    print("🔢 CFOP (Código Fiscal de Operações)")
    print("-" * 40)
    df_cfop = carregar_cfop()
    resultados.append(validar_tabela("CFOP", df_cfop, minimo=500))
    
    if not df_cfop.is_empty():
        print(f"   Colunas: {', '.join(df_cfop.columns)}")
        
        # Testar validação
        if validar_cfop("5102"):
            print("   ✅ Validação de CFOP funcional")
    print()
    
    # CST
    print("💰 CST (Código de Situação Tributária)")
    print("-" * 40)
    df_cst = carregar_cst()
    resultados.append(validar_tabela("CST", df_cst, minimo=50))
    
    if not df_cst.is_empty():
        print(f"   Colunas: {', '.join(df_cst.columns)}")
    print()
    
    # NFe Domínios
    print("📄 NFe - Domínios")
    print("-" * 40)
    dominios_nfe = carregar_dominios_nfe()
    print(f"   {len(dominios_nfe)} domínios carregados:")
    for chave, df in dominios_nfe.items():
        resultados.append(validar_tabela(f"  - {chave}", df))
    print()
    
    # NFe Mapeamento
    print("🗺️  NFe - Mapeamento")
    print("-" * 40)
    df_mapeamento = carregar_mapeamento_nfe()
    resultados.append(validar_tabela("Mapeamento NFe", df_mapeamento))
    print()
    
    # NFe Eventos
    print("🔔 NFe - Eventos")
    print("-" * 40)
    eventos_nfe = carregar_dominios_eventos_nfe()
    print(f"   {len(eventos_nfe)} tipos de eventos carregados:")
    for chave, df in eventos_nfe.items():
        resultados.append(validar_tabela(f"  - {chave}", df))
    print()
    
    # Fisconforme
    print("🔍 Fisconforme - Malhas")
    print("-" * 40)
    df_malhas = carregar_malhas_fisconforme()
    resultados.append(validar_tabela("Malhas Fisconforme", df_malhas))
    print()
    
    # Resumo
    print("=" * 60)
    print("RESUMO")
    print("=" * 60)
    
    total = len(resultados)
    aprovados = sum(resultados)
    reprovados = total - aprovados
    
    print(f"Total de verificações: {total}")
    print(f"✅ Aprovadas: {aprovados}")
    print(f"❌ Reprovadas: {reprovados}")
    print()
    
    if reprovados == 0:
        print("🎉 TODAS AS VALIDAÇÕES PASSARAM!")
        return 0
    else:
        print(f"⚠️  {reprovados} validação(ões) falharam. Verifique os arquivos.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
