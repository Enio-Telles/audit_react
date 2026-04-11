# Fiscal Parquet Analyzer

Ferramenta de extraÃ§Ã£o, transformaÃ§Ã£o e auditoria de dados fiscais com persistÃªncia em Parquet, pipeline modular em `src/transformacao/` e interface grÃ¡fica em PySide6.

## Objetivo

O projeto organiza o fluxo fiscal em trÃªs camadas:

- extraÃ§Ã£o Oracle para Parquet por CNPJ;
- transformaÃ§Ã£o analÃ­tica com foco em rastreabilidade e auditoria;
- consulta e operaÃ§Ã£o do pipeline pela interface grÃ¡fica.

O princÃ­pio central Ã© preservar a linha original do documento fiscal e permitir que qualquer total analÃ­tico seja auditado de volta Ã  origem.

## Pipeline oficial

A ordem ativa do pipeline estÃ¡ em `src/orquestrador_pipeline.py`:

1. `tb_documentos`
2. `item_unidades`
3. `itens`
4. `descricao_produtos`
5. `produtos_final`
6. `fontes_produtos`
7. `fatores_conversao`
8. `c170_xml`
9. `c176_xml`
10. `movimentacao_estoque`
11. `calculos_mensais`
12. `calculos_anuais`

Os wrappers em `src/transformacao/` existem em boa parte para compatibilidade. Ao corrigir ou evoluir regras, a implementaÃ§Ã£o real costuma estar nos subpacotes `*_pkg`.

## Data de corte da entrega EFD

Quando a extração usa o parâmetro `data_limite_processamento`, o significado correto é:

- ele representa a data de corte da entrega EFD;
- a seleção considera apenas arquivos com `data_entrega <= data informada`;
- se o parâmetro vier vazio ou nulo, o sistema usa a data atual;
- isso permite reproduzir a visão fiscal como ela existia até uma determinada data de entrega.

## ExecuÃ§Ã£o rÃ¡pida

InstalaÃ§Ã£o mÃ­nima:

```bash
pip install polars PySide6 openpyxl python-docx docxtpl python-dotenv rich oracledb
```

Observação:

- o endpoint `POST /api/fisconforme/gerar-docx` depende de `docxtpl`;
- se a dependência não estiver instalada, a API continua subindo, mas esse endpoint retorna erro explícito ao ser acionado.

Abrir a aplicaÃ§Ã£o:

```bash
python app.py
```

Rodar a suÃ­te de testes:

```bash
python -m pytest
```

Rodar testes direcionados:

```bash
python -m pytest tests/test_movimentacao_estoque.py
python -m pytest tests/test_calculos_mensais.py
python -m pytest tests/test_calculos_anuais.py
```

## DocumentaÃ§Ã£o oficial

Os documentos ativos do projeto ficam na raiz de `docs/`:

- [MovimentaÃ§Ã£o de Estoque](docs/mov_estoque.md)
- [Tabela Mensal](docs/tabela_mensal.md)
- [Tabela Anual](docs/tabela_anual.md)
- [ConversÃ£o de Unidades](docs/conversao_unidades.md)
- [AgregaÃ§Ã£o de Produtos](docs/agregacao_produtos.md)
- [Dossie - Direcao e Contrato](docs/dossie_main.md)
- [Dossie - Cache Canonico](docs/dossie_cache.md)

## Convenções importantes

- `id_agrupado` é a chave mestra de produto no pipeline.
- `id_agregado` aparece em algumas saídas analíticas como alias de apresentação de `id_agrupado`.
- `__qtd_decl_final_audit__` guarda a quantidade declarada no estoque final para auditoria, sem alterar o saldo físico.
- Todo cruzamento de estoque com layouts da receita federal é norteado pelo arquivo base `map_estoque.json` presente na raiz do sistema.
- Ajustes manuais de conversão e agrupamento devem ser preservados em reprocessamentos.

## DocumentaÃ§Ã£o histÃ³rica

Materiais antigos, planos intermediÃ¡rios, diagnÃ³sticos e anexos foram movidos para `docs/archive/`. Eles permanecem como histÃ³rico e apoio, mas a referÃªncia operacional atual Ã© somente a documentaÃ§Ã£o oficial listada acima.


## Catálogo SQL canônico

Todas as consultas ativas do sistema ficam exclusivamente dentro de `sql/`, organizada por domínio:

```text
sql/
  fiscal/
    efd/
    documentos/
    fronteira/
    validacao/
  fisconforme/
    cadastro/
    malhas/
  apoio/
    dicionarios/
    verificacoes/
  archive/
```

Regras operacionais restritas (vide `AGENTS_SQL.md`):

- `sql/` é a única fonte de verdade das consultas usadas pelo backend, frontend e desktop. Nenhuma query deve ser escrita via concatenação em código Python.
- Todo o cruzamento deve obrigar o uso de *Bind Variables* (ex: `:CNPJ`, `:NOME`) na sintaxe Oracle ao invés de uso de F-Strings, por proteção contra injeções.
- As cargas do Polars baseiam-se em integrações massivas via `read_database` otimizando transferências entre driver (cx_Oracle/oracledb) e a runtime C++.
- Seleções persistidas usam IDs relativos ao catálogo, como `fiscal/efd/c170.sql`.
- Novas integrações de Inteligência Fiscal NFe/NFCe operam usando as expansões lógicas na view materializada internamente como `dossie_contato.sql`.
