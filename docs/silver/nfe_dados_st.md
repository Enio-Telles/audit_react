# Nfe Dados St

## Papel da Tabela na Camada Silver

A `nfe_dados_st` padroniza a base XML de ST e FCP por item de NF-e. Ela concentra os campos tributarios necessarios para conciliacao da trilha ST sem misturar essa leitura diretamente ao core de estoque.

## Origem em `extraidos`

- `extraidos/nfe_dados_st.parquet`

## Regras Principais de Normalizacao

- converte a extracao XML de NF-e para um schema silver por item de documento
- preserva produto, classificacao fiscal, CNPJs envolvidos e data do documento
- materializa bases e valores de ST, FCP e ICMS substituto no mesmo contrato
- gera `id_linha_origem` com documento e item para conciliacao posterior

## Campos Criticos

- `id_linha_origem`
- `chave_documento`, `item_documento`
- `codigo_fonte`, `codigo_produto`, `descricao`
- `ncm`, `cest`, `cfop`
- `cst`, `csosn`
- `data_documento`
- `cnpj_referencia`, `cnpj_emitente`, `cnpj_destinatario`
- `quantidade`, `valor_total`
- `bc_st`, `vl_st`, `vl_icms_substituto`, `vl_st_retido`
- `bc_fcp_st`, `p_fcp_st`, `vl_fcp_st`

## Tabelas Gold que Consomem a Tabela

- `st_itens`

## Limitacoes e Observacoes

- a trilha ST depende de extracao local de `nfe_dados_st`; por isso esta tabela ainda pode sair vazia legitimamente em alguns CNPJs
- isso nao implica paridade externa concluida; a paridade segue dependente da materializacao real e da comparacao com `C:\\funcoes - Copia`
- a tabela complementa a visao tributaria de ST e nao substitui `fontes_produtos` no core
- no piloto `37671507000187`, a extracao real foi fechada apos ajuste de sessao Oracle com `NLS_NUMERIC_CHARACTERS = '.,'`

## Saida Gerada

```text
storage/CNPJ/{cnpj}/silver/nfe_dados_st.parquet
```
