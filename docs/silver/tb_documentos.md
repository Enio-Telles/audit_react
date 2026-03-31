# Tb Documentos

## Papel da Tabela na Camada Silver

A `tb_documentos` consolida os metadados documentais da camada `silver`. Ela resume cada documento fiscal ou inventario em uma linha auditavel, preservando chave, data, origem e quantidade de itens.

## Origem em `extraidos`

- `extraidos/nfe.parquet`
- `extraidos/nfce.parquet`
- `extraidos/c170.parquet`
- `extraidos/bloco_h.parquet`

A tabela nasce da consolidacao de `silver/fontes_produtos.parquet`.

## Regras Principais de Normalizacao

- unifica documentos de NF-e, NFC-e, EFD `C170` e inventario `Bloco H`
- agrega por `fonte`, `chave_documento`, data, tipo de movimento e CNPJs relacionados
- soma `valor_total` e conta itens distintos para formar uma visao documental resumida
- classifica `tipo_documento` como `NFE`, `NFCE`, `EFD` ou `INVENTARIO`

## Campos Criticos

- `fonte`, `tipo_documento`
- `chave_documento`, `numero_documento`
- `data_documento`
- `tipo_movimento`
- `cnpj_referencia`, `cnpj_emitente`, `cnpj_destinatario`
- `valor_total`, `quantidade_itens`

## Tabelas Gold que Consomem a Tabela

- Nenhuma tabela gold consome `tb_documentos` diretamente na versao atual.
- Ela sustenta consulta auditavel, manifesto e diagnostico da trilha documental.

## Limitacoes e Observacoes

- pode sair vazia legitimamente quando nao houver movimentos documentais materializados em `fontes_produtos`
- nao substitui as tabelas gold; ela existe para rastreabilidade intermediaria
- como contrato interno operacional, pode evoluir sem o mesmo congelamento das gold

## Saida Gerada

```text
storage/CNPJ/{cnpj}/silver/tb_documentos.parquet
```
