# Mapeamento de Tabela - Aba Conversão

Este documento descreve a estrutura, funcionalidade e origens de dados da tabela de Fatores de Conversão de Unidades.

## Tabela: `fatores_conversao_{cnpj}.parquet`

**Localização**: `dados/CNPJ/{cnpj}/analises/produtos/`
**Origem de Dados (Oracle)**:
*   `SPED.REG_0220`: Fatores de conversão informados pelo contribuinte.
*   `SPED.REG_0190`: Cadastro de unidades de medida.
*   `SPED.REG_C170`: Histórico de compras para cálculo de preço médio de entrada.
*   `BI.FATO_NFE_DETALHE / NFCE_DETALHE`: Histórico de vendas para cálculo de preço médio de saída (utilizado como fallback).

### Funcionalidade
Esta tabela gerencia as **conversões de unidade de medida**. Ela é vital para a normalização do estoque, garantindo que "CAIXA COM 12" e "UNIDADE" sejam tratados na mesma escala volumétrica.

### Mapeamento Completo de Campos

| Campo | Tipo Polars | Origem/Regra | Descrição Detalhada |
| :--- | :--- | :--- | :--- |
| `id_agrupado` | `String` | `produtos_final` | Identificador único do grupo de produtos (FK). |
| `id_produtos` | `String` | `0220 / 0200` | Identificador original do produto na EFD. |
| `descr_padrao` | `String` | `0200 / H` | Descrição para facilitar a identificação visual. |
| `unid` | `String` | `0190 / Doc Fiscal`| Unidade de medida de origem (ex: CX, FD). |
| `unid_ref` | `String` | Sistema / Manual | Unidade de medida de destino/referência (ex: UN). |
| `fator` | `Float64` | `0220` / Manual | Multiplicador: `Qtd Ref = Qtd Origem * Fator`. |
| `fator_manual` | `Boolean` | Auditoria | Se `True`, indica que o auditor alterou o fator na UI. Protege contra sobrescrita automática. |
| `unid_ref_manual` | `Boolean` | Auditoria | Se `True`, indica que a unidade de referência foi fixada pelo auditor. |
| `preco_medio` | `Float64` | C170 / NFe | Valor monetário médio para este produto nesta unidade. |
| `origem_preco` | `String` | Logica | Indica de onde veio o `preco_medio`: `ENTRADA` (preferencial) ou `VENDA` (fallback). |

### Relações e Fluxo
1.  **Normalização**: O pipeline de estoque (`mov_estoque`) consulta esta tabela para cada linha de movimentação. Se o `id_agrupado` e a `unid` coincidirem, a quantidade é multiplicada pelo `fator`.
2.  **Ressarcimento**: O `preco_medio` (principalmente de ENTRADA) é a base de cálculo para a valorização de pedidos de ressarcimento de ICMS ST.
3.  **Persistência**: Alterações marcadas como `*_manual` em `fatores_conversao_manual.json` são reintegradas ao Parquet a cada execução.

---
> [!TIP]
> Um `fator` inconsistente (ex: CX=1 quando deveria ser 12) é a causa mais comum de "furos" de estoque fictícios. A aba Conversão é a ferramenta primária para sanar essas divergências.
