# Mapeamento de Tabela - Aba Agregação

Este documento descreve a estrutura exaustiva, funcionalidade e origens de dados da tabela principal utilizada na aba de Agregação de Produtos.

## Tabela: `produtos_final_{cnpj}.parquet`

**Localização**: `dados/CNPJ/{cnpj}/analises/produtos/`

**Origem de Dados (Oracle)**:

* `SPED.REG_C170`: Descrições complementares e ocorrências em itens de notas.
* `SPED.REG_0200`: Cadastro original de mercadorias do contribuinte.
* `SPED.REG_H010`: Inventário físico (Bloco H).
* `BI.FATO_NFE_DETALHE`: Dados de notas fiscais eletrônicas (NFe).
* `BI.FATO_NFCE_DETALHE`: Dados de notas fiscais de consumidor eletrônicas (NFCe).

### Funcionalidade

Esta tabela funciona como o **Cadastro Mestre de Produtos** do sistema. Ela consolida informações de diversas origens e tenta unificar itens idênticos sob um mesmo `id_agrupado`. A aba Agregação permite que o auditor refine esse agrupamento, mesclando IDs que o sistema não conseguiu identificar automaticamente.

### Mapeamento Completo de Campos

| Campo | Tipo Polars | Origem/Regra | Descrição Detalhada |
| :--- | :--- | :--- | :--- |
| **Identificadores** | | | |
| `id_agrupado` | `String` | Sistema | **Golden Thread**. Chave primária do grupo de produtos. Conecta todas as tabelas analíticas. |
| `id_descricao` | `String` | MD5(Desc) | Identificador único baseado puramente na descrição normalizada. |
| `chave_item` | `String` | De-para | Chave técnica para join com a tabela de itens originais. |
| `chave_produto` | `String` | De-para | Chave técnica para join com a tabela de produtos base. |
| **Atributos Padronizados** | | | |
| `descr_padrao` | `String` | 0200 / H010 | Descrição sugerida pelo sistema (geralmente a mais frequente ou do Bloco H). |
| `ncm_padrao` | `String` | 0200 | NCM (Nomenclatura Comum do Mercosul) associado ao grupo. |
| `cest_padrao` | `String` | 0200 | CEST (Código Especificador da Substituição Tributária). |
| `gtin_padrao` | `String` | 0200 / NFe | Código de barras (EAN/GTIN) principal do grupo. |
| `unid_ref_sugerida` | `String` | Estatística | Unidade de medida sugerida como base para o estoque (ex: UN). |
| **Atributos de Decisão (Finais)** | | | |
| `descricao_final` | `String` | Manual/Auto | Descrição que será exibida nos relatórios finais após ajustes do auditor. |
| `ncm_final` | `String` | Manual/Auto | NCM validado pelo auditor ou herdado da padronização. |
| `cest_final` | `String` | Manual/Auto | CEST validado. Crucial para regras de Ressarcimento ST. |
| `co_sefin_final` | `String` | SEFIN-RO | **Classificação Fiscal**: Código de mercadoria da SEFIN-RO. Usado para identificação tributária específica no estado de Rondônia. |
| **Informações Coletadas (Listas)** | | | |
| `lista_desc_compl` | `List(String)` | C170/NFe | Todas as variações de "Descrição Complementar" encontradas para este produto. |
| `lista_codigos` | `List(String)` | 0200 / H | Todos os códigos de produto (COD_ITEM) associados a este grupo. |
| `lista_ncm` | `List(String)` | 0200 | Lista de todos os NCMs que já foram associados a este grupo. |
| `fontes` | `List(String)` | Sistema | Indica de quais origens este produto foi detectado (ex: `['c170', 'bloco_h', 'nfe']`). |
| **Metadados e Auditoria** | | | |
| `co_sefin_divergentes` | `Boolean` | Cálculo | `True` se houver mais de um código SEFIN associado aos itens do grupo. |
| `descricao_normalizada`| `String` | Regex | Descrição em maiúsculas e sem caracteres especiais para indexação. |

### Relações e Fluxo

1. **Chave Mestre**: O `id_agrupado` é propagado para `mov_estoque`, `aba_mensal` e `ressarcimento_st_item`.
2. **Persistência**: Ajustes manuais feitos na UI (como mesclar dois IDs) são salvos no banco de metadados e aplicados sobre esta tabela a cada reprocessamento do pipeline.
3. **Origem**: Gerada pelo módulo `src/transformacao/produtos_final.py`.

---
> [!NOTE]
> Os campos `*_final` têm precedência sobre os `*_padrao` em todas as visões de exportação e relatórios.
