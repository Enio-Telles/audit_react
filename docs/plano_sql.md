# Plano de Implementacao: Analise, Extracao e Evolucao do Dossie

Este documento consolida o plano do Dossie SQL e o status real da implementacao verificado no repositorio em 2026-04-08 apos a integracao do fluxo de sincronizacao entre backend e frontend.

Objetivo: transformar o Dossie de um leitor passivo de artefatos em cache para um fluxo capaz de resolver secoes, executar SQL no Oracle, persistir resultados em Parquet e expor esse ciclo para a API e para a interface web.

Regra adicional obrigatoria:
toda extracao, composicao, persistencia e exibicao deve manter referencia auditavel da fonte do dado, idealmente indicando a tabela ou view de origem no banco, alem do `sql_id`, dataset compartilhado ou parquet reutilizado.

Plano mestre relacionado: `docs/plano_dossie_integral.md`

---

## 1. Status Verificado

| Item do plano | Status | Evidencias no codigo | Observacoes |
| --- | --- | --- | --- |
| Criar `Agent_SQL.md` | Concluido | `Agent_SQL.md` existe com regras de binds, isolamento de SQL e orientacoes para decomposicao do legado XML | O guia foi criado e cobre o objetivo principal do item. |
| Fragmentar consultas do `dossie_nif.xml` em `.sql` | Concluido | Existem `sql/dossie_enderecos.sql`, `sql/dossie_historico_situacao.sql`, `sql/dossie_regime_pagamento.sql`, `sql/dossie_atividades.sql`, `sql/dossie_contador.sql`, `sql/dossie_historico_fac.sql`, `sql/dossie_vistorias.sql` e `sql/dossie_historico_socios.sql` | A secao `cadastro` segue reutilizando `dados_cadastrais.sql`, o que preserva compatibilidade com o catalogo atual. |
| Criar `sql/dossie_contato.sql` consolidado | Concluido | `sql/dossie_contato.sql` foi criado, refinado em execucao real contra Oracle e hoje converge com a composicao Polars nos CNPJs de referencia | O SQL consolidado existe, o builder consome esse contrato, o sync aceita ativacao controlada por parametro e os modos `sql_consolidado` e `composicao_polars` ja convergiram funcionalmente nos CNPJs de validacao. |
| Criar motor de extracao em `dossie_extraction_service.py` | Parcial | `src/interface_grafica/services/dossie_extraction_service.py` agora resolve multiplos `sql_ids`, tenta reuso de datasets compartilhados, executa Oracle apenas quando necessario e salva o cache final da secao | A base arquitetural foi implementada, incluindo metadata sidecar inicial dos datasets compartilhados, mas ainda faltam validacao com Oracle real e refinamento de algumas secoes `mixed`. |
| Expor rota de sincronizacao no FastAPI | Concluido | `backend/routers/dossie.py` possui `POST /{cnpj}/secoes/{secao_id}/sync` e tambem o alias `POST /{cnpj}/sync/{secao_id}` | O backend agora atende o contrato atual e preserva compatibilidade com o contrato originalmente descrito no plano. |
| Expor leitura detalhada do cache por secao | Concluido | `backend/routers/dossie.py` agora possui `GET /{cnpj}/secoes/{secao_id}/dados` para leitura do parquet materializado sem reconsulta Oracle | O frontend passa a inspecionar a secao materializada com rastreabilidade do arquivo de cache e total de linhas. |
| Atualizar `dossie_catalog.py` e `dossie_aliases.py` | Concluido | As secoes `enderecos`, `historico_situacao`, `regime_pagamento`, `atividades`, `contador`, `historico_fac`, `vistorias` e `socios` foram adicionadas e mapeadas | O catalogo e os aliases ja refletem a abertura das subsecoes originadas do XML. |
| Integrar sincronizacao no `frontend/src/api/client.ts` | Concluido | `frontend/src/api/client.ts` agora expone `dossieApi.syncSecao(cnpj, secaoId, parametros?)` | O client React passou a consumir o endpoint de sincronizacao do Dossie. |
| Integrar botoes e feedback de sync no `DossieTab.tsx` | Concluido | `frontend/src/features/dossie/components/DossieTab.tsx` agora usa `useMutation`, invalida cache, exibe feedback por secao e abre a leitura detalhada do cache selecionado | O fluxo de sincronizacao ja pode ser disparado e auditado diretamente pela interface do Dossie. |
| Executar verificacoes do plano | Parcial | Foram adicionados testes do roteador para os dois contratos de sync, `tsc --noEmit` passou e o lint terminou sem erros | Ainda falta validar o comportamento completo com consulta Oracle real e, idealmente, um teste visual ou manual da interface. |

---

## 2. Estado Atual Consolidado

### Backend

- O resumo das secoes continua conservador no `GET /api/dossie/{cnpj}/secoes`, lendo apenas artefatos ja materializados.
- Ja existe um caminho ativo de sincronizacao via `POST /api/dossie/{cnpj}/secoes/{secao_id}/sync`.
- O contrato legado `POST /api/dossie/{cnpj}/sync/{secao_id}` tambem foi mantido para aderencia ao plano original e evitar quebra de integracoes.
- O cache canonico do Dossie usa chave estavel derivada de CNPJ, secao, parametros normalizados e versao de consulta.
- Toda secao do Dossie deve manter, de alguma forma, referencia de origem do dado para auditoria, como `origem_dado`, `sql_id_origem`, tabela/view de origem ou metadata sidecar equivalente.

### Servicos

- `dossie_resolution.py` ja resolve secao, aliases SQL e nome de arquivo de cache.
- `dossie_extraction_service.py` ja salva o resultado em `CNPJ/<cnpj>/arquivos_parquet/dossie/`.
- `SqlService.build_binds()` ja filtra apenas bind variables presentes no SQL, reduzindo erro de execucao por parametro excedente.
- O fluxo atual do Dossie ja tenta reusar datasets compartilhados por SQL e por artefatos canonicos conhecidos antes de abrir nova conexao Oracle.

### Frontend

- O Dossie passou a operar como painel ativo para sincronizacao por secao.
- Os status visuais (`cached`, `loading`, `fresh`, `error`) agora sao usados tambem no fluxo de execucao e retorno da sincronizacao.

### Testes

- Existem testes para resumo de secoes e para chaves de cache.
- Existem testes cobrindo os dois contratos de rota POST de sincronizacao.
- Ja existe cobertura para erro tratavel de sync no backend e para reuso completo de datasets sem nova consulta Oracle.
- O frontend do Dossie ja possui teste automatizado cobrindo feedback de sucesso/erro por secao e atualizacao do resumo sem reload manual apos sync.
- O frontend do Dossie agora tambem possui teste automatizado cobrindo renderizacao com volume alto de linhas na secao `contato`, preservando agrupamento e exibicao dos extremos.
- `DossieContatoDetalhe.tsx` passou a memoizar o agrupamento por `tipo_vinculo`, reduzindo recomputacao desnecessaria em re-render da leitura detalhada.
- `dossie_section_builder.py` passou a iterar filiais, contadores, socios e pares reconciliados de telefone linha a linha, reduzindo listas intermediarias em cenarios volumosos.
- A consolidacao do contador passou a usar a FAC como referencia principal quando disponivel e agora materializa tambem `emails_por_fonte`, `telefones_por_fonte` e `fontes_contato`, mantendo a rastreabilidade auditavel das fontes complementares no backend, no SQL consolidado e na UI.
- A camada de testes do builder agora cobre explicitamente empresa com muitas filiais e varios socios, validando contagem e preservacao das origens no resultado final.
- A cobertura automatizada da secao `contato` agora inclui tambem cenario volumoso com contador sem contato completo, garantindo que o bloco `CONTADOR_EMPRESA` permanece materializado mesmo sem telefone, email e endereco.
- O sync da secao agora registra em metadata e no retorno da API os tempos de materializacao e de sync total, alem do total de SQLs da secao e do percentual efetivo de reuso.
- O sync agora tambem classifica explicitamente o impacto do `cache-first` por execucao, distinguindo reuso total, reuso parcial, ausencia de reuso e reaproveitamento de cache canonico equivalente.
- A validacao manual assistida da UI foi executada no CNPJ `37671507000187`, confirmando carga do painel do Dossie, abertura do detalhe da secao `contato`, sync real com mensagem de sucesso e exibicao coerente de rastreabilidade, estrategia e metricas de reuso.
- A validacao manual tambem cobriu um cenario real de erro na UI, com indisponibilidade temporaria da API durante o sync do `contato` e exibicao da mensagem `Request failed with status code 502` no card da secao.
- A trilha de performance ganhou o script `scripts/medir_performance_dossie_contato.py`, ja executado sobre `37671507000187` e `84654326000394`, com relatorios persistidos em `output/performance_dossie_contato/`.
- `dossie_vistorias.sql` foi normalizada para evitar decode inconsistente em dados remotos e para substituir o binario bruto de assinatura por um indicador textual auditavel (`RELATORIO_ASSINADO`), permitindo a materializacao segura em Parquet e o retorno da secao `vistorias` para a medicao consolidada do Dossie.
- Nas medicoes reais desta etapa, o `cache-first` ficou em `reuso_total` nos dois CNPJs, e o sync do `contato` registrou 55 ms e 38 ms para `37671507000187`, e 268 ms e 47 ms para `84654326000394`, respectivamente nos modos `composicao_polars` e `sql_consolidado`.
- As regras funcionais e tecnicas da secao `contato` ficaram congeladas no contrato documental atual: colunas oficiais, `tipo_vinculo`, ordenacao funcional, chaves canonicas de reuso e criterio tecnico para decidir entre novo parquet base ou composicao logica.
- Ainda nao ha teste ponta a ponta cobrindo execucao Oracle real, persistencia fisica e reflexo visual no frontend.

### Reuso e performance

- O projeto ja reaproveita bem os Parquets derivados no fluxo de estoque:
  `aba_mensal` e `aba_anual` leem `mov_estoque` em vez de reconstruir o fluxo do zero.
- O principal gap atual esta na ampliacao do catalogo de reuso entre modulos:
  o Dossie ja verifica datasets compartilhados conhecidos antes de nova consulta, mas ainda falta conectar estoque, ressarcimento e outras analises ao mesmo catalogo.
- A secao `contato` agora tambem usa `dossie_filiais_raiz.sql` como dataset de apoio no caminho Polars, alinhando a enumeracao de filiais por raiz com o contrato do SQL consolidado.
- O reuso de `NFe.sql` e `NFCe.sql` passou a priorizar `shared_sql` atual sobre artefatos legados, e a camada de extracao ja consegue promover um legado para cache SQL compartilhado atualizado quando isso for necessario para manter aderencia ao contrato vigente.
- O catalogo tecnico de reuso agora tambem reconhece explicitamente os Parquets analiticos `mov_estoque_<cnpj>.parquet`, `aba_mensal_<cnpj>.parquet`, `aba_anual_<cnpj>.parquet` e `ressarcimento_st_*_<cnpj>.parquet` como fontes reutilizaveis conhecidas para composicao futura sem nova extracao Oracle.
- O Dossie ja consegue expor esses artefatos em secoes de leitura `cache_catalog`, permitindo navegacao sobre `estoque` e `ressarcimento_st` sem disparar nova sincronizacao Oracle.
- A secao `arrecadacao` tambem foi formalizada como `cache_catalog`, porque hoje ela resume apenas artefatos ja materializados de E111, fronteira e malhas, sem contrato util de sync proprio.
- `SqlService.executar_sql()` continua como executor direto, mas o Dossie agora o aciona atras de uma camada `cache-first` para datasets compartilhados.
- `SqlService` agora possui montagem resiliente de `DataFrame` para resultados Oracle com schema instavel, normalizando apenas colunas mistas para texto quando o retorno variar tipo entre linhas.
- `QueryWorker` e o sync do Dossie passaram a reutilizar essa mesma rotina resiliente, reduzindo falhas em consultas amplas como `NFe.sql` e `NFCe.sql`.
- O `Fisconforme` backend e desktop tambem passaram a reutilizar a mesma montagem resiliente de `DataFrame`, reduzindo risco de quebra em extrações cadastrais e de malha com colunas Oracle de tipo misto.
- `extracao_oracle_eficiente.py` passou a reaproveitar o mesmo construtor resiliente por lote, reduzindo divergencia entre a extracao paralela para Parquet e os demais fluxos Oracle do projeto.
- `extracao_cadastral.py` passou a reutilizar o mesmo construtor resiliente tambem no resultado unitario de cadastro Oracle, mantendo coerencia com cache e router do `Fisconforme`.
- `extrator_oracle.py` deixou de depender de `pandas.read_sql()` e passou a gravar Parquet de forma incremental por cursor, reduzindo memoria e aproximando o `Fisconforme` do padrao de extracao em lotes do restante do projeto.
- `processador_polars.py` passou a reutilizar `LazyFrame` por tabela na mesma instancia, evitando reabertura redundante de Parquet quando mais de um relatorio analitico usa a mesma base.
- `extracao_cadastral.py` tambem deixou de rematerializar o cache inteiro em exportacao e estatisticas simples, usando copia direta do Parquet e contagem/schema lazy quando possivel.

---

## 3. TODO List Priorizada

### Prioridade Alta

- [x] Adicionar `dossieApi.syncSecao(cnpj, secaoId, parametros?)` em `frontend/src/api/client.ts`.
- [x] Implementar `useMutation` no `frontend/src/features/dossie/components/DossieTab.tsx` para disparar a sincronizacao por secao.
- [x] Invalidar `queryKey` de secoes apos sync concluido para refletir o novo cache sem recarregar a pagina.
- [x] Exibir feedback por secao durante a execucao: carregando, sucesso, erro e quantidade de linhas atualizada.

### Prioridade Media

- [x] Decidir e documentar o contrato canonico da rota de sync.
- [x] Manter compatibilidade entre `/{cnpj}/secoes/{secao_id}/sync` e `/{cnpj}/sync/{secao_id}`.
- [x] Criar testes automatizados para o POST de sync cobrindo o roteamento principal e o alias legado.
- [x] Ampliar os testes do POST de sync para cobrir erro de secao desconhecida e falha de persistencia.

### Prioridade Tecnica

- [x] Evoluir `dossie_extraction_service.py` para suportar multiplos `sql_ids` por secao quando houver composicao real de fontes.
- [x] Definir no retorno da sincronizacao informacoes mais auditaveis, como `sql_id` executado, `cache_key` e versao da consulta.
- [ ] Avaliar se secoes `mixed` devem continuar resumindo artefatos legados e canonicos no mesmo fluxo ou se o modelo precisa ser unificado.
- [x] Criar uma camada compartilhada de datasets Oracle canonicos para evitar reextracao do mesmo CNPJ/CPF em Dossie, estoque e outras analises.
- [x] Separar claramente extracao de dados do Oracle e composicao analitica em Polars.
- [x] Fazer `cache-first` por dataset/entity key antes de qualquer nova consulta Oracle.
- [x] Padronizar a base inicial de reuso de Parquet com metadata sidecar e leitura por `scan_parquet()` nos datasets compartilhados conhecidos.
- [x] Reaplicar a montagem resiliente de resultados Oracle tambem nos fluxos `Fisconforme`, evitando serializacao fragil de `fetchall()` com schema misto.
- [x] Reaplicar a montagem resiliente de resultados Oracle tambem na extracao eficiente em lotes, reduzindo diferencas de schema entre `fetchmany()` e os demais executores.
- [x] Reaplicar a montagem resiliente de resultados Oracle tambem na extração cadastral unitária do `Fisconforme`.
- [x] Substituir `pandas.read_sql()` por leitura incremental em lotes no `extrator_oracle.py`, reduzindo consumo de memoria e unificando o padrao de gravacao Parquet.
- [x] Reaproveitar `LazyFrame` no `processador_polars.py` para reduzir reabertura de Parquet dentro da mesma sessao analitica do `Fisconforme`.
- [x] Evitar releitura completa do cache cadastral do `Fisconforme` em exportacao e estatisticas quando uma copia direta ou consulta lazy forem suficientes.

## 4.1 TODO de Otimizacao de Extracoes e Reuso

Objetivo:
garantir que uma informacao extraida uma vez para um CNPJ, CPF ou chave de negocio nao seja extraida novamente sem necessidade, sendo reaproveitada por Dossie, estoque, ressarcimento e demais fluxos.

### Diretrizes

- Extrair uma vez, reutilizar muitas.
- Compor no Polars, nao no Oracle, sempre que a informacao ja existir materializada.
- Salvar datasets canonicos por entidade e assunto, nao por tela ou por caso de uso.

### TODO proposto

Observacao:
- os itens abaixo descrevem a evolucao desejada da arquitetura global de reuso do projeto.
- eles nao invalidam a camada `cache-first` e o reuso compartilhado ja implementados para o Dossie `contato`.

- [ ] Criar um catalogo de datasets compartilhados por entidade e dominio.
- [ ] Definir chaves canonicas de reuso:
  `cnpj`, `cnpj_raiz`, `cpf_cnpj`, `ie`, `chave_acesso`, `id_sefin` quando aplicavel.
- [ ] Criar uma camada de resolucao de datasets compartilhados antes do `SqlService.executar_sql()`.
- [ ] Fazer o Dossie consultar primeiro datasets existentes de cadastro, NFe, NFCe, contador, socios e contatos antes de abrir nova conexao Oracle.
- [ ] Fazer a secao `contato` priorizar joins Polars sobre datasets ja materializados.
- [ ] Mapear quais extrações Oracle do estoque ja podem alimentar o Dossie sem duplicacao.
- [ ] Mapear quais extrações do Dossie podem alimentar estoque, ressarcimento ou outras analises sem duplicacao.
- [ ] Padronizar a persistencia de datasets Oracle brutos ou semibrutos em uma arvore canonica.
- [ ] Persistir metadados de origem do dataset:
  SQL de origem, parametros, entidade, data da extração, versao e dependencias.
- [x] Implementar leitura inicial por `pl.scan_parquet()` para composicao lazy dos datasets compartilhados conhecidos.
- [ ] Evitar salvar novos Parquets redundantes quando o resultado puder ser representado como view logica ou composicao de datasets existentes.

### Casos prioritarios de reuso

Observacao:
- os casos ainda abertos abaixo sao oportunidades de ampliacao para outros modulos e para padronizacao final do catalogo global.
- o reaproveitamento minimo necessario para a secao `contato` ja esta ativo e validado.

- [ ] Reaproveitar `dados_cadastrais_<cnpj>.parquet` no Dossie antes de nova extracao cadastral.
- [ ] Reaproveitar `nfe_agr_<cnpj>.parquet`, `nfce_agr_<cnpj>.parquet` e bases correlatas em secoes do Dossie e em contatos.
- [x] Mapear `mov_estoque_<cnpj>.parquet`, `aba_mensal_<cnpj>.parquet`, `aba_anual_<cnpj>.parquet` e `ressarcimento_st_*_<cnpj>.parquet` no catalogo canonico de reuso.
- [ ] Reaproveitar `mov_estoque_<cnpj>.parquet`, `aba_mensal_<cnpj>.parquet` e `aba_anual_<cnpj>.parquet` nas analises derivadas sem reextracao Oracle.
- [ ] Reaproveitar datasets de contador, socio e pessoa quando a mesma entidade aparecer em mais de uma secao.

---

## 4. Riscos e Pontos de Atencao

- O motor atual executa somente o primeiro SQL priorizado por secao. Isso preserva simplicidade, mas pode ser insuficiente em secoes que exijam consolidacao de multiplas consultas.
- Os testes atuais ainda nao garantem o ciclo completo de extracao, persistencia e reconsulta da secao sincronizada.
- Sem uma camada global de reuso, o projeto corre risco de duplicar extrações Oracle para a mesma entidade em Dossie, contatos e fluxos analiticos.
- Salvar um novo Parquet por caso de uso, em vez de por dataset canonico, tende a aumentar custo de processamento, armazenamento e risco de inconsistência.

---

## 5. Criterio de Conclusao

O plano pode ser considerado concluido quando:

1. A sincronizacao puder ser disparada pela UI do Dossie.
2. O frontend refletir o novo estado da secao sem reload manual.
3. A documentacao estiver alinhada ao contrato real da rota.
4. Houver teste automatizado minimo para o POST de sync e para o fluxo de cache correspondente.
5. Os testes de qualidade do frontend e do backend estiverem executados sem erro.

Estado atual:
- os criterios tecnicos acima ja foram atendidos no estado atual do repositorio.
- os itens ainda abertos no restante deste documento refletem principalmente oportunidades de evolucao da arquitetura global de reuso e formalizacoes de contrato, nao bloqueios tecnicos do Dossie `contato` ja implementado.
