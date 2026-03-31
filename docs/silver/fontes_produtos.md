# Fontes Produtos

## Papel da Tabela na Camada Silver

A `fontes_produtos` e a principal base unificada de movimentos do core fiscal. Ela padroniza em um unico contrato intermediario as NF-e, NFC-e, entradas escrituradas da EFD e o inventario do Bloco H.

## Origem em `extraidos`

- `extraidos/nfe.parquet`
- `extraidos/nfce.parquet`
- `extraidos/c170.parquet`
- `extraidos/bloco_h.parquet`

## Regras Principais de Normalizacao

- converte cada fonte documental para um schema unico com produto, documento, quantidade, valor e CNPJs envolvidos
- cria `descricao_normalizada` para estabilizar comparacoes de produto no pipeline
- gera `id_linha_origem` a partir de documento e item para manter rastreabilidade
- classifica `tipo_movimento` como `entrada`, `saida` ou `inventario`
- preserva `codigo_fonte` como elo entre origem documental, catalogo e estoque

## Campos Criticos

- `id_linha_origem`
- `fonte`, `tipo_movimento`
- `chave_documento`, `item_documento`
- `codigo_fonte`, `codigo_produto`
- `descricao`, `descricao_normalizada`
- `ncm`, `cest`, `gtin`
- `unidade`, `quantidade`
- `valor_unitario`, `valor_total`
- `cfop`, `data_documento`
- `cnpj_referencia`, `cnpj_emitente`, `cnpj_destinatario`

## Tabelas Gold que Consomem a Tabela

- `nfe_entrada`
- `mov_estoque`

## Limitacoes e Observacoes

- pode sair vazia legitimamente se o CNPJ nao tiver nenhuma das extracoes documentais base
- e a base central do core, mas continua sendo contrato interno operacional e nao camada publica principal
- erros ou ausencias documentais nas extracoes se propagam para a qualidade de produto, estoque e resumos

## Saida Gerada

```text
storage/CNPJ/{cnpj}/silver/fontes_produtos.parquet
```
