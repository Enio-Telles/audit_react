# Documentacao da Camada Silver

Esta pasta documenta a camada `silver` do `audit_react`. Ela fica entre `extraidos` e `parquets` e concentra a normalizacao intermediaria em Polars usada para recompor o core fiscal e a trilha ST.

## Papel da Camada

- padronizar documentos e itens vindos das extracoes Oracle por CNPJ
- manter rastreabilidade por documento, item e produto antes das tabelas gold
- oferecer uma base intermediaria consultavel pela API com `camada=silver`
- sustentar manifesto, diagnostico e reprocessamentos sem congelar o contrato com o mesmo rigor das gold

## Tabelas Materializadas

| Tabela | Origem principal | Consumo principal |
| --- | --- | --- |
| `tb_documentos` | `nfe`, `nfce`, `c170`, `bloco_h` | Consulta documental e auditoria |
| `item_unidades` | `fontes_produtos` | `produtos_unidades`, `fatores_conversao` |
| `itens` | `item_unidades` | `descricao_produtos` e diagnostico do catalogo |
| `descricao_produtos` | `itens` | Apoio ao catalogo e analise de descricoes |
| `fontes_produtos` | `nfe`, `nfce`, `c170`, `bloco_h` | `nfe_entrada`, `mov_estoque` e base unificada do core |
| `c170_xml` | `c170` | Consulta rastreavel das entradas escrituradas |
| `c176_xml` | `c176` | `st_itens` |
| `nfe_dados_st` | `nfe_dados_st` | `st_itens` |
| `e111_ajustes` | `e111` | `ajustes_e111` |

## Observacoes

- A camada `silver` pode evoluir internamente sem o mesmo grau de congelamento das tabelas gold.
- Algumas tabelas podem existir com schema valido e zero linhas quando a extracao correspondente nao estiver disponivel para o CNPJ.
- Isso e esperado principalmente na trilha ST, que depende de `c176`, `nfe_dados_st` e `e111` materializados em `extraidos`.

## Arquivos

- [tb_documentos](./tb_documentos.md)
- [item_unidades](./item_unidades.md)
- [itens](./itens.md)
- [descricao_produtos](./descricao_produtos.md)
- [fontes_produtos](./fontes_produtos.md)
- [c170_xml](./c170_xml.md)
- [c176_xml](./c176_xml.md)
- [nfe_dados_st](./nfe_dados_st.md)
- [e111_ajustes](./e111_ajustes.md)
