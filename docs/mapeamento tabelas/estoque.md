# Mapeamento de Tabelas - Aba Estoque

Este documento descreve exaustivamente as tabelas que compõem a análise de inventário, movimentação física e reconciliação fiscal.

---

## 1. Movimentação de Estoque (`mov_estoque_{cnpj}.parquet`)

**Localização**: `dados/CNPJ/{cnpj}/analises/produtos/`
**Origens Oracle**:
* `SPED.REG_C170`: Descrições complementares e ocorrências em itens de notas.
* `SPED.REG_0200`: Cadastro original de mercadorias do contribuinte.
* `SPED.REG_H010`: Inventário físico (Bloco H).
* `BI.FATO_NFE_DETALHE`: Dados de notas fiscais eletrônicas (NFe).
* `BI.FATO_NFCE_DETALHE`: Dados de notas fiscais de consumidor eletrônicas (NFCe).

### Funcionalidade
Esta é a tabela de fatos mestre. Ela contém cada evento de entrada ou saída, normalizado e enriquecido com dados de auditoria.

### Principais Campos (Mapeamento Completo)

| Grupo | Campo | Tipo | Descrição/Origem |
| :--- | :--- | :--- | :--- |
| **Auditoria** | `ordem_operacoes` | `UInt32` | Sequência cronológica rigorosa de todas as operações. |
| | `fonte` | `String` | Origem da linha: `c170`, `nfe`, `bloco_h` ou `gerado` (saldo inicial). |
| | `nsu` | `Int64` | Número Sequencial Único da nota no banco de dados. |
| | `id_linha_origem` | `String` | Rastreabilidade direta para o registro original na EFD/Oracle. |
| **Documento** | `Chv_nfe` | `String` | **Chave de 44 dígitos** da Nota Fiscal Eletrônica. |
| | `num_nfe` | `String` | Número do documento fiscal. |
| | `Dt_doc` | `Datetime` | Data de emissão ou entrada do documento. |
| | `Tipo_operacao` | `String` | Categoria: `ENTRADA`, `SAIDA`, `ESTOQUE INICIAL`, `ESTOQUE FINAL`. |
| **Fiscal** | `Cfop` | `String` | Código Fiscal de Operações e Prestações. |
| | `Cst` | `String` | Código de Situação Tributária (ICMS). |
| | `Vl_item` | `Float64` | Valor contábil bruto do item. |
| | `Aliq_icms` | `Float64` | Alíquota de ICMS aplicada na operação. |
| | `co_sefin_final` | `String` | **Classificação SEFIN-RO**: Código de mercadoria estadual. |
| **Quantidade** | `Qtd` | `Float64` | Quantidade original do documento. |
| | `q_conv` | `Float64` | **Quantidade Normalizada**: `Qtd * Fator`. Usada para cálculo de saldo. |
| | `Unid` | `String` | Unidade de medida original do documento. |
| | `unid_ref` | `String` | Unidade de medida de referência do sistema. |
| **Análise** | `excluir_estoque` | `Boolean` | Flag para ignorar movimentações que não afetam estoque físico (ex: Remessas). |
| | `dev_simples` | `Boolean` | Indica se é uma operação de devolução simples. |
| | `saldo_estoque_anual`| `Float64`| Saldo progressivo calculado ao longo do ano. |

---

## 2. Apuração Mensal (`aba_mensal_{cnpj}.parquet`)

**Localização**: `dados/CNPJ/{cnpj}/analises/produtos/`
**Origem**: Consolidado via Polars a partir da `mov_estoque`.

| Campo | Tipo | Descrição Detalhada |
| :--- | :--- | :--- |
| `ano` / `mes` | `Int` | Período de referência da apuração. |
| `id_agregado` | `String` | Identificador do grupo de produtos. |
| `ST` | `String` | Status da Substituição Tributária no período (visto como string de intervalo). |
| `it_in_st` | `String` | Indicador simplificado de ST (`S`/`N`). |
| `qtd_entradas` | `Float64` | Soma das quantidades (`q_conv`) de entrada no mês. |
| `qtd_saidas` | `Float64` | Soma das quantidades (`q_conv`) de saída no mês. |
| `pme_mes` | `Float64` | **Preço Médio de Entrada** do mês de referência. |
| `saldo_mes` | `Float64` | Saldo físico projetado ao final do mês. |
| `entradas_desacob` | `Float64`| **Divergência**: Entradas não justificadas por documentos fiscais. |
| `valor_estoque` | `Float64` | Valorização do saldo final (`saldo_mes * custo_medio_mes`). |

---

## 3. Apuração Anual (`aba_anual_{cnpj}.parquet`)

**Localização**: `dados/CNPJ/{cnpj}/analises/produtos/`
**Origem**: Consolidado anual para confronto com Bloco H.

| Campo | Tipo | Descrição Detalhada |
| :--- | :--- | :--- |
| `estoque_inicial` | `Float64` | Saldo herdado do inventário do ano anterior. |
| `estoque_final` | `Float64` | Saldo físico calculado ao final do exercício. |
| `saidas_calculadas`| `Float64` | Saídas projetadas com base no inventário declarado (`Inicial + Entradas - Final Declarado`). |
| `saidas_desacob` | `Float64` | Quantidade de vendas sem nota fiscal detectada (Omissão de Saída). |
| `ICMS_saidas_desac`| `Float64`| **Risco Fiscal**: Imposto estimado sobre as saídas desacobertadas. |

---

## 4. Inventário Bloco H (`bloco_h_{cnpj}.parquet`)

**Localização**: `dados/CNPJ/{cnpj}/arquivos_parquet/`
**Origem Oracle**: `SPED.REG_H010` (Itens de inventário) e `SPED.REG_H005` (Totais).

| Campo | Tipo | Descrição Detalhada |
| :--- | :--- | :--- |
| `dt_inv` | `Datetime` | Data em que o inventário foi realizado (normalmente 31/12). |
| `cod_mot_inv` | `String` | Motivo do inventário (ex: `01` - Final de período). |
| `mot_inv_desc` | `String` | Descrição textual do motivo. |
| `codigo_produto` | `String` | Código original do produto informado na EFD. |
| `quantidade` | `Int64` | Quantidade física declarada. |
| `valor_unitario` | `Float64` | Valor unitário do item no inventário. |
| `valor_item` | `Float64` | Valor total do item (`quantidade * valor_unitario`). |
| `indicador_propriedade`| `String`| `0` (Própria), `1` (Terceiros em posse da empresa), `2` (Própria em posse de terceiros). |

---

## Relações e Integridade
*   **Confronto Auditável**: O sistema cruza o `estoque_final` da `aba_anual` com a `quantidade` do `bloco_h`. Se o `saldo_final` for menor que o declarado, houve compra sem nota. Se maior, houve venda sem nota.
*   **Rastreabilidade**: Todas as tabelas mantêm o `id_agrupado` para permitir o "drill-down" das apurações anuais até a nota fiscal individual na `mov_estoque`.
