# Interface Grafica - Comportamento das Tabelas

## Consulta de Parquet

- A aba `Consulta` nao usa mais paginacao manual.
- O arquivo selecionado e carregado integralmente apos os filtros em Polars.
- A navegacao pelos registros ocorre pela rolagem nativa da `QTableView`.
- O rodape da area de filtros passou a exibir apenas `Linhas filtradas: N`.

## Consulta SQL

- A aba `Consulta SQL` nao divide mais o resultado em paginas.
- O resultado retornado pelo Oracle e exibido integralmente na grade.
- A busca textual continua funcionando sobre o conjunto completo retornado.
- O indicador superior passou a exibir `Total: N` ou `Total filtrado: N`.

## Frontend React

- O componente compartilhado `DataTable` nao renderiza mais controles de paginacao.
- As tabelas web agora exibem o conjunto completo por rolagem, mantendo filtros, ordenacao, selecao e ocultacao de colunas.
- As APIs paginadas consumidas pelo frontend passaram a concatenar todas as paginas antes de entregar os dados para a grade.

## Observacoes tecnicas

- A mudanca preserva filtros, ordenacao visual, exportacao e selecao de colunas.
- O recorte por pagina foi removido da exibicao desktop e web.
- A rolagem da tabela passa a ser o mecanismo padrao para percorrer todas as linhas.
