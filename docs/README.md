# Documentacao do Pipeline

Esta pasta documenta o pipeline fiscal do `audit_react` no mesmo espirito do diretorio `C:\funcoes - Copia\docs`, mas aderente ao comportamento real do repositorio.

## Visao Rapida das Camadas

O pipeline trabalha por CNPJ com a seguinte organizacao:

- `extraidos/`: extracoes Oracle brutas e isoladas por CNPJ.
- `silver/`: normalizacao intermediaria em Polars, reutilizavel e auditavel.
- `parquets/`: tabelas gold publicas consumidas pela API, UI e exportacoes.

As tabelas documentadas aqui ficam em:

```text
storage/CNPJ/{cnpj}/silver/
storage/CNPJ/{cnpj}/parquets/
```

## Tabelas Gold Publicas

| Tabela | Depende de | Finalidade principal |
| --- | --- | --- |
| `produtos_unidades` | `silver/item_unidades` | Base de produtos por origem fiscal com unidades predominantes |
| `produtos` | `produtos_unidades` | Catalogo publico de produtos consolidados |
| `produtos_agrupados` | `produtos` | Grupos mestres com agregacao automatica e manual |
| `id_agrupados` | `produtos_agrupados` | Ponte entre `id_produto` e `id_agrupado` |
| `fatores_conversao` | `produtos_agrupados` | Conversao entre compra, venda e unidade de referencia |
| `produtos_final` | `produtos_agrupados`, `fatores_conversao` | Catalogo final de produto com fator aplicado |
| `nfe_entrada` | `produtos_final`, `id_agrupados` | Entradas padronizadas na unidade de referencia |
| `mov_estoque` | `nfe_entrada`, `produtos_final`, `id_agrupados` | Trilha cronologica de estoque |
| `aba_mensal` | `mov_estoque` | Fechamento mensal por produto agrupado |
| `aba_anual` | `aba_mensal` | Fechamento anual por produto agrupado |
| `produtos_selecionados` | `produtos_final` | Lista de produtos marcados para analise detalhada |
| `ajustes_e111` | `silver/e111_ajustes` | Trilha gold de ajustes E111 por competencia |
| `st_itens` | `produtos`, `id_agrupados`, `produtos_final`, `silver/c176_xml`, `silver/nfe_dados_st` | Conciliacao de ST por documento e item |

## Arquivos Gold

- [produtos_unidades](./tabelas/produtos_unidades.md)
- [produtos](./tabelas/produtos.md)
- [produtos_agrupados](./tabelas/produtos_agrupados.md)
- [id_agrupados](./tabelas/id_agrupados.md)
- [fatores_conversao](./tabelas/fatores_conversao.md)
- [produtos_final](./tabelas/produtos_final.md)
- [nfe_entrada](./tabelas/nfe_entrada.md)
- [mov_estoque](./tabelas/mov_estoque.md)
- [aba_mensal](./tabelas/aba_mensal.md)
- [aba_anual](./tabelas/aba_anual.md)
- [produtos_selecionados](./tabelas/produtos_selecionados.md)
- [ajustes_e111](./tabelas/ajustes_e111.md)
- [st_itens](./tabelas/st_itens.md)

## Camada Silver

A camada `silver` e um contrato interno operacional do pipeline. Ela aparece no manifesto, pode ser consultada pela API com `camada=silver` e serve como base recomponivel entre `extraidos` e `parquets`.

| Tabela | Origem principal em `extraidos` | Finalidade principal |
| --- | --- | --- |
| `tb_documentos` | `nfe`, `nfce`, `c170`, `bloco_h` | Dimensao documental consolidada para consulta e auditoria |
| `item_unidades` | `nfe`, `nfce`, `c170`, `bloco_h` | Consolidacao por produto-origem e unidade |
| `itens` | `item_unidades` | Consolidacao por produto-origem antes da agregacao |
| `descricao_produtos` | `itens` | Dimensao padronizada de descricoes fiscais |
| `fontes_produtos` | `nfe`, `nfce`, `c170`, `bloco_h` | Base unificada de movimentos do core |
| `c170_xml` | `c170` | Recorte documental das entradas escrituradas da EFD |
| `c176_xml` | `c176` | Trilha silver de ressarcimento/ST por documento e item |
| `nfe_dados_st` | `nfe_dados_st` | Base XML de ST e FCP por item de NF-e |
| `e111_ajustes` | `e111` | Trilha intermediaria de ajustes E111 por competencia |

## Arquivos Silver

- [tb_documentos](./silver/tb_documentos.md)
- [item_unidades](./silver/item_unidades.md)
- [itens](./silver/itens.md)
- [descricao_produtos](./silver/descricao_produtos.md)
- [fontes_produtos](./silver/fontes_produtos.md)
- [c170_xml](./silver/c170_xml.md)
- [c176_xml](./silver/c176_xml.md)
- [nfe_dados_st](./silver/nfe_dados_st.md)
- [e111_ajustes](./silver/e111_ajustes.md)

## Observacoes

- Esta pasta agora cobre as tabelas gold publicas e a camada `silver`.
- A camada `silver` nao e o contrato publico principal da UI, mas e consultavel pela API e auditavel no manifesto.
- A documentacao descreve o comportamento atual do `audit_react`, inclusive quando alguma tabela pode sair vazia legitimamente.
