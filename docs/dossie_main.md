# Dossie como Direcao Principal do Main

Este documento registra a decisao de produto e manutencao de tratar o **Dossie por CNPJ** como eixo principal da evolucao do sistema.

## Decisao

O fluxo principal do projeto deve priorizar:

- acesso ao Dossie sempre que houver CNPJ selecionado
- reaproveitamento de consultas SQL e datasets ja existentes
- persistencia das extracoes para evitar duplicacao
- navegacao por secoes materializadas e auditaveis
- documentacao curta, clara e atualizada junto com cada evolucao

## O que isso significa na pratica

O Dossie deixou de ser um recurso lateral e passou a ser a linha principal de integracao funcional do sistema.

Na manutencao do `main`, isso implica:

1. o contexto do CNPJ selecionado deve expor acesso direto ao Dossie
2. o backend deve favorecer catalogo, cache e auditoria por secao
3. o frontend deve tratar o Dossie como fluxo principal de navegacao
4. consultas ja existentes tem prioridade sobre novas consultas duplicadas
5. qualquer extracao reaproveitavel deve ser persistida e reutilizada

## Regras de Implementacao

### 1. O Dossie nasce do CNPJ

Sem CNPJ selecionado, o Dossie pode ficar indisponivel.

Com CNPJ selecionado, o acesso ao Dossie deve ser imediato e visivel no fluxo principal da interface.

### 2. Persistencia antes de repeticao

A extracao de dados do Dossie deve ser salva por combinacao de:

- CNPJ
- secao
- parametros normalizados
- versao da consulta efetivamente utilizada

Se a mesma combinacao ja existir, a aplicacao deve reutilizar o resultado em vez de salvar de novo.

### 3. SQL existente tem prioridade

Ao implementar uma nova secao do Dossie, a ordem recomendada e:

1. localizar consulta equivalente no catalogo atual
2. reutilizar a consulta ja existente
3. criar nova consulta apenas quando nao houver equivalente viavel

### 4. Reuso antes de nova extracao

Antes de abrir nova conexao Oracle, o sistema deve tentar reaproveitar:

- datasets compartilhados materializados em `shared_sql`
- artefatos canonicos ja existentes por CNPJ, CNPJ raiz ou chave funcional
- parquets analiticos de outros modulos quando a secao operar em modo `cache_catalog`

### 5. Documentacao faz parte da entrega

Toda mudanca relevante do Dossie deve vir acompanhada de documentacao simples contendo:

- objetivo
- arquivos impactados
- regra de cache ou reuso
- contrato alterado
- limitacoes atuais

## Contrato Atual Implementado

### Backend

- `GET /api/dossie/{cnpj}/secoes` retorna o resumo das secoes com status de cache, quantidade de linhas, estrategia da ultima materializacao, SQL principal quando existir sidecar e `syncEnabled`.
- `POST /api/dossie/{cnpj}/secoes/{secao_id}/sync` e `POST /api/dossie/{cnpj}/sync/{secao_id}` materializam a secao no cache canonico.
- `GET /api/dossie/{cnpj}/secoes/{secao_id}/dados` le o parquet final materializado sem reexecutar Oracle.
- `GET /api/dossie/{cnpj}/secoes/contato/comparacoes` e `GET /api/dossie/{cnpj}/secoes/contato/comparacoes/resumo` expoem historico e consolidado das comparacoes entre `composicao_polars` e `sql_consolidado`.
- `POST /api/dossie/{cnpj}/secoes/contato/comparacoes/relatorio` gera o relatorio tecnico markdown por CNPJ.

### Cache e auditoria

- O cache canonico da secao segue a chave `cnpj + secao + parametros normalizados + versao_consulta`.
- Cada secao materializada pode gerar sidecar `.metadata.json` com `cache_key`, `estrategia_execucao`, `sql_principal`, SQLs executadas ou reutilizadas, quantidade de linhas e comparacao com estrategia alternada.
- A secao `contato` tambem registra historico JSONL de comparacoes por CNPJ e relatorio tecnico markdown ao lado do parquet materializado.
- Toda saida do Dossie deve manter referencia auditavel da fonte do dado, por `origem_dado`, `tabela_origem`, `sql_id`, dataset compartilhado ou arquivo de origem.

### Reuso

- O Dossie opera em modo `cache-first`: antes de nova consulta Oracle, tenta reaproveitar datasets compartilhados e artefatos canonicos ja materializados.
- `NFe.sql` e `NFCe.sql` priorizam `shared_sql` atual, com promocao automatica de legado quando necessario.
- Secoes `cache_catalog` reutilizam artefatos analiticos ja existentes, sem sync Oracle proprio. Hoje isso cobre tambem `estoque`, `ressarcimento_st` e `arrecadacao`.

### Secao `contato`

- O caminho padrao da secao e `composicao_polars`; `sql_consolidado` fica disponivel por ativacao controlada.
- O contrato funcional hoje cobre empresa principal, matriz e filiais por raiz, contador, socios atuais e sinais de `NFe/NFCe`.
- A reconciliacao de telefone do contador por `NFe/NFCe` ficou congelada em modo estrito: so entra quando houver CPF ou CNPJ completo e identico ao do contador.
- A secao `contato` ja atingiu convergencia funcional total entre `composicao_polars` e `sql_consolidado` nos CNPJs `37671507000187` e `84654326000394`.

## Impacto sobre decisoes anteriores

O `main` deve ser avaliado pela coerencia com:

- CNPJ como ponto de entrada
- Dossie como fluxo principal
- reutilizacao de consultas e datasets
- persistencia sem duplicacao
- rastreabilidade auditavel da origem do dado

## Limitacoes Atuais

- Ainda faltam validacao manual formal da UI e cenarios de carga com empresas mais volumosas.
- Ainda faltam decisoes funcionais finais para alguns pontos de contrato, como politica definitiva de filiais por raiz e congelamento final das colunas oficiais da secao `contato`.
- O catalogo global de reuso entre Dossie, estoque, ressarcimento e demais modulos ainda pode ser ampliado.

## Proximos passos

1. consolidar os itens restantes de validacao manual e carga
2. fechar as decisoes funcionais ainda abertas da secao `contato`
3. ampliar o catalogo compartilhado de datasets reutilizaveis entre modulos
4. manter a documentacao do Dossie coerente com o contrato real implementado
