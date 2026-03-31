# Importação de Tabelas de Referência — SEFIN

**Data da Importação:** 31 de março de 2026  
**Origem:** `C:\funcoes - Copia\dados\referencias\referencias`  
**Destino:** `server/python/audit_engine/dados/referencias/`

---

## ✅ Arquivos Importados

### 1. **NCM (Nomenclatura Comum do Mercosul)**
**Diretório:** `NCM/`

| Arquivo                 | Registros | Descrição                                         |
|-------------------------|-----------|---------------------------------------------------|
| `tabela_ncm.parquet`    | 13.288    | Tabela completa de NCM com descrições e vigências |
| `ncm_capitulos.parquet` | -         | Capítulos de NCM para agrupamento                 |
| `ncm_posicao.parquet`   | -         | Posições de NCM                                   |
| `ncm_postgres.parquet`  | -         | Versão PostgreSQL                                 |
| `ncm_tabela.parquet`    | -         | Tabela auxiliar                                   |

**Uso no sistema:**
- Validação de NCM em produtos
- Enriquecimento de descrições
- Classificação fiscal de mercadorias

---

### 2. **CEST (Código Especificador da Substituição Tributária)**
**Diretório:** `CEST/`

| Arquivo                         | Registros | Descrição                |
|---------------------------------|-----------|--------------------------|
| `cest.parquet`                  | 1.604     | Tabela CEST completa     |
| `segmentos_mercadorias.parquet` | -         | Segmentos de mercadorias |

**Uso no sistema:**
- Identificação de produtos sujeitos à ST
- Conciliação com matriz SEFIN
- Validação de CFOP + NCM + CEST

---

### 3. **CFOP (Código Fiscal de Operações e Prestações)**
**Diretório:** `cfop/`

| Arquivo                 | Registros | Descrição                         |
|-------------------------|-----------|-----------------------------------|
| `cfop.parquet`          | 580       | Tabela completa de CFOP           |
| `cfop_1_digito.parquet` | -         | Resumo por primeiro dígito        |
| `cfop_bi.parquet`       | -         | Versão BI (Business Intelligence) |
| `cfop_subgrupo.parquet` | -         | Subgrupos de CFOP                 |

**Uso no sistema:**
- Classificação de tipo de operação (entrada/saída)
- Determinação de tipo de movimento
- Validação de documentos fiscais

---

### 4. **CST (Código de Situação Tributária)**
**Diretório:** `cst/`

| Arquivo                 | Registros | Descrição                  |
|-------------------------|-----------|----------------------------|
| `cst.parquet`           | 106       | Tabela completa de CST     |
| `cst_1_dig.parquet`     | -         | Resumo por primeiro dígito |
| `cst_2_digitos.parquet` | -         | Detalhamento por 2 dígitos |

**Uso no sistema:**
- Tributação de ICMS
- Determinação de alíquotas
- Conciliação ST

---

### 5. **CO_SEFIN (Matriz de Produtos SEFIN)**
**Diretório:** `CO_SEFIN/`

| Arquivo                            | Descrição                                              |
|------------------------------------|--------------------------------------------------------|
| `sitafe_produto_sefin_aux.parquet` | Matriz auxiliar de produtos SEFIN para validação de ST |

**Uso no sistema:**
- Validação de produtos em relação à matriz SEFIN
- Cálculo de ST por produto
- Conciliação de diferenças

---

### 6. **NFe (Nota Fiscal Eletrônica) - Domínios**
**Diretório:** `NFe/`

| Arquivo                              | Descrição                         |
|--------------------------------------|-----------------------------------|
| `dominio_CO_CRT.parquet`             | CRT - Código de Regime Tributário |
| `dominio_CO_FINNFE.parquet`          | Finalidade da NFe                 |
| `dominio_CO_IDDEST.parquet`          | Indicador de destino              |
| `dominio_CO_INDFINAL.parquet`        | Indicador de consumidor final     |
| `dominio_CO_INDIEDEST.parquet`       | Indicador de IE do destinatário   |
| `dominio_CO_INDPRES.parquet`         | Indicador de presença             |
| `dominio_CO_TPEMIS.parquet`          | Tipo de emissão                   |
| `dominio_CO_TP_NF.parquet`           | Tipo de operação da NFe           |
| `dominio_INFPROT_CSTAT.parquet`      | Código de status da resposta      |
| `dominio_PROD_INDTOT.parquet`        | Indicador de totalização          |
| `dominio_PROD_NITEM.parquet`         | Número do item                    |
| `dominio_VEIC_PROD_CONDVEIC.parquet` | Condição do veículo               |
| `dominio_VEIC_PROD_TPREST.parquet`   | Tipo de prestação                 |
| `dominio_VEIC_PROD_VIN.parquet`      | VIN do veículo                    |
| `mapeamento_NFe.parquet`             | Mapeamento de campos NFe          |

**Uso no sistema:**
- Validação de campos de NFe
- Classificação de tipo de operação
- Verificação de status de resposta

---

### 7. **NFE_eventos (Eventos de NFe)**
**Diretório:** `NFE_eventos/`

| Arquivo                                | Descrição                    |
|----------------------------------------|------------------------------|
| `dominio_evento_tpevento.parquet`      | Tipos de eventos de NFe      |
| `dominio_evento_tpevento-Enio.parquet` | Variação de tipos de eventos |

**Uso no sistema:**
- Classificação de eventos (cancelamento, carta de correção, etc.)
- Validação de tipo de evento

---

### 8. **Fisconforme (Malhas de Fiscalização)**
**Diretório:** `Fisconforme/`

| Arquivo          | Descrição                             |
|------------------|---------------------------------------|
| `malhas.parquet` | Malhas de fiscalização para auditoria |

**Uso no sistema:**
- Identificação de padrões de auditoria
- Detecção de anomalias fiscais

---

### 9. **Documentação**

| Arquivo                         | Descrição                                                  |
|---------------------------------|------------------------------------------------------------|
| `fatores_conversao_unidades.md` | Documentação completa do processo de conversão de unidades |
| `README_IMPORTACAO.md`          | Este arquivo - documentação da importação                  |

---

## 📁 Estrutura de Diretórios Resultante

```
server/python/audit_engine/dados/referencias/
├── NCM/
│   ├── tabela_ncm.parquet           # 13.288 registros
│   ├── ncm_capitulos.parquet
│   ├── ncm_posicao.parquet
│   ├── ncm_postgres.parquet
│   └── ncm_tabela.parquet
├── CEST/
│   ├── cest.parquet                 # 1.604 registros
│   └── segmentos_mercadorias.parquet
├── cfop/
│   ├── cfop.parquet                 # 580 registros
│   ├── cfop_1_digito.parquet
│   ├── cfop_bi.parquet
│   └── cfop_subgrupo.parquet
├── cst/
│   ├── cst.parquet                  # 106 registros
│   ├── cst_1_dig.parquet
│   └── cst_2_digitos.parquet
├── NFe/
│   ├── dominio_CO_CRT.parquet
│   ├── dominio_CO_FINNFE.parquet
│   ├── dominio_CO_IDDEST.parquet
│   ├── dominio_CO_INDFINAL.parquet
│   ├── dominio_CO_INDIEDEST.parquet
│   ├── dominio_CO_INDPRES.parquet
│   ├── dominio_CO_TPEMIS.parquet
│   ├── dominio_CO_TP_NF.parquet
│   ├── dominio_INFPROT_CSTAT.parquet
│   ├── dominio_PROD_INDTOT.parquet
│   ├── dominio_PROD_NITEM.parquet
│   ├── dominio_VEIC_PROD_CONDVEIC.parquet
│   ├── dominio_VEIC_PROD_TPREST.parquet
│   ├── dominio_VEIC_PROD_VIN.parquet
│   └── mapeamento_NFe.parquet
├── NFE_eventos/
│   ├── dominio_evento_tpevento.parquet
│   └── dominio_evento_tpevento-Enio.parquet
├── Fisconforme/
│   └── malhas.parquet
├── CO_SEFIN/
│   └── sitafe_produto_sefin_aux.parquet
├── fatores_conversao_unidades.md
└── README_IMPORTACAO.md
```

---

## 🔧 Utilitários Criados

### Módulo: `utils/referencias.py`

Funções disponíveis para carregamento e validação:

```python
# Carregamento completo
from audit_engine.utils.referencias import (
    carregar_ncm,
    carregar_cest,
    carregar_cfop,
    carregar_cst,
)

# Busca específica
from audit_engine.utils.referencias import (
    buscar_ncm_por_codigo,
    buscar_cest_por_codigo,
    buscar_cfop_por_codigo,
)

# Validação
from audit_engine.utils.referencias import (
    validar_ncm,
    validar_cest,
    validar_cfop,
)

# Enriquecimento de DataFrames
from audit_engine.utils.referencias import (
    enriquecer_com_ncm,
    enriquecer_com_cest,
    enriquecer_com_cfop,
)
```

---

## 📊 Resumo Quantitativo

| Tipo        | Total Arquivos | Total Registros (principais) |
|-------------|----------------|------------------------------|
| NCM         | 5              | 13.288                       |
| CEST        | 2              | 1.604                        |
| CFOP        | 4              | 580                          |
| CST         | 3              | 106                          |
| NFe         | 15             | -                            |
| NFE_eventos | 2              | -                            |
| Fisconforme | 1              | -                            |
| CO_SEFIN    | 1              | -                            |
| **Total**   | **33**         | **~15.578**                  |

---

## ✅ Validação Realizada

Todos os arquivos foram testados com sucesso:

```python
import polars as pl

# NCM: 13288 registros ✓
pl.read_parquet('dados/referencias/NCM/tabela_ncm.parquet')

# CEST: 1604 registros ✓
pl.read_parquet('dados/referencias/CEST/cest.parquet')

# CFOP: 580 registros ✓
pl.read_parquet('dados/referencias/cfop/cfop.parquet')

# CST: 106 registros ✓
pl.read_parquet('dados/referencias/cst/cst.parquet')
```

---

## 🔄 Próximos Passos Sugeridos

1. **Integrar validação de NCM/CEST/CFOP/CST nos geradores** de tabelas
2. **Usar matriz SEFIN** para validação automática de produtos
3. **Implementar enriquecimento** de descrições nas tabelas do pipeline
4. **Criar endpoints de consulta** de referências na API
5. **Adicionar validações fiscais** no frontend

---

## 📝 Notas Técnicas

- **Formato:** Todos os arquivos estão em Parquet com compressão padrão
- **Codificação:** UTF-8 para textos
- **Compatibilidade:** Polars ≥ 0.19.0
- **Localização:** Acessível via `DIRETORIO_REFERENCIAS` em `utils/referencias.py`

---

**Status:** ✅ **IMPORTAÇÃO CONCLUÍDA**
