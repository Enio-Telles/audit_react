# C170 Xml

## Papel da Tabela na Camada Silver

A `c170_xml` e o recorte rastreavel das entradas escrituradas no `C170` dentro do schema documental da camada `silver`. Ela facilita auditoria da origem EFD sem depender de releitura direta dos `extraidos`.

## Origem em `extraidos`

- `extraidos/c170.parquet`

A tabela e derivada de `silver/fontes_produtos.parquet`, filtrando apenas a fonte `c170`.

## Regras Principais de Normalizacao

- reaproveita o mesmo contrato documental padronizado usado em `fontes_produtos`
- preserva somente linhas da EFD `C170`
- mantem quantidade, valor, data, CFOP e referencia por produto e documento

## Campos Criticos

- `id_linha_origem`
- `fonte`
- `chave_documento`
- `codigo_fonte`, `codigo_produto`
- `descricao`
- `ncm`, `cest`
- `unidade`, `quantidade`
- `valor_unitario`, `valor_total`
- `cfop`, `data_documento`
- `cnpj_referencia`

## Tabelas Gold que Consomem a Tabela

- Nenhuma tabela gold consome `c170_xml` diretamente na versao atual.
- Ela existe para consulta auditavel e rastreabilidade da trilha de entradas escrituradas.

## Limitacoes e Observacoes

- pode sair vazia legitimamente se `c170` nao tiver sido extraido para o CNPJ
- mesmo vazia, a tabela pode ser gravada com schema valido para preservar previsibilidade operacional
- seu papel atual e diagnostico e documental, nao de contrato publico da UI

## Saida Gerada

```text
storage/CNPJ/{cnpj}/silver/c170_xml.parquet
```
