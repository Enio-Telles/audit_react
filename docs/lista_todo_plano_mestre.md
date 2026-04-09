# Lista TODO do Plano Mestre

Este arquivo e o checklist operacional completo para execucao integral dos planos em:

- `docs/plano_sql.md`
- `docs/plano _contatos.md`
- `docs/plano_dossie_integral.md`

Regra de uso:

- Este documento deve ser atualizado a cada etapa implementada.
- Sempre que um item for concluido no codigo, o status correspondente deve ser revisado aqui no mesmo ciclo.
- Quando houver mudanca de escopo, risco, dependencia ou regra de negocio, registrar observacao curta na secao apropriada.
- Este arquivo deve ser suficiente para conduzir a execucao ponta a ponta, sem depender de leitura parcial dos planos complementares.

Data de referencia inicial: 2026-04-08

## 1. Estado Geral

- Status global: Implementacao tecnica concluida; restam apenas formalizacoes de governanca e decisoes funcionais residuais
- Objetivo principal: concluir o Dossie com secao `contato`, reuso de datasets Oracle, composicao em Polars, validacao funcional e convergencia total entre o estado atual e o estado posterior as mudancas.
- Plano mestre de referencia: `docs/plano_dossie_integral.md`
- Guia tecnico de SQL: `Agent_SQL.md`

## 2. Checklist Operacional Completo

### Fase 0: Baseline e Governanca

- [x] Consolidar os planos base em um plano mestre
- [x] Criar esta lista TODO operacional
- [x] Fixar o estado atual da extracao como baseline oficial do "antes"
- [x] Criar ferramenta de verificacao de convergencia das extracoes
- [x] Salvar baseline atual dos CNPJs de referencia
- [x] Confirmar oficialmente que qualquer divergencia do "depois" precisa ser documentada antes da aprovacao
- [x] Registrar no historico desta lista a data de inicio efetivo da implementacao

### Fase 1: Decisoes Funcionais Obrigatorias

Observacao:
- os itens ainda abertos desta fase nao bloqueiam a implementacao tecnica atual do Dossie `contato`.
- eles representam decisoes de negocio e formalizacao contratual que ainda podem ser congeladas em ciclo posterior.

- [x] Confirmar o padrao obrigatorio de referencia da fonte em toda saida do Dossie, incluindo `origem_dado` e, quando possivel, tabela/view de origem no banco
- [x] Definir se a enumeracao de filiais por raiz inclui apenas ativas ou todas as encontradas
- [x] Definir prioridade de fonte para dados do contador
- [x] Definir fallback final do contador entre FAC atual, `SITAFE_PESSOA`, `SITAFE_RASCUNHO_FAC` e `SITAFE_REQ_INSCRICAO`
- [x] Definir regra de reconciliacao para telefone do contador em NFe e NFCe por CPF/CNPJ
- [x] Confirmar permissao de exibicao de dados de socios pessoa fisica
- [x] Congelar colunas finais da secao `contato`
- [x] Congelar valores finais de `tipo_vinculo`
- [x] Definir ordenacao funcional do resultado da secao `contato`
- [x] Definir quais blocos da secao `contato` serao montados apenas por datasets ja materializados
- [x] Definir quais blocos exigirao nova extracao Oracle
- [x] Definir chaves canonicas de reuso para `cnpj`, `cnpj_raiz`, `cpf_cnpj`, `ie`, `gr_identificacao`, `gr_ident_contador`, `chave_acesso` e `id_sefin`

### Fase 2: Motor SQL Atual do Dossie

- [x] Criar o guia SQL do projeto
- [x] Fragmentar as consultas principais do legado XML em arquivos `.sql`
- [x] Implementar sincronizacao backend por secao
- [x] Integrar sincronizacao por secao no frontend
- [x] Evoluir `src/interface_grafica/services/dossie_extraction_service.py` para suportar multiplos `sql_ids` por secao
- [x] Fazer a execucao da secao suportar composicao real de mais de uma consulta
- [x] Retornar metadados auditaveis da sincronizacao: `sql_ids_executados`, `cache_key`, `versao_consulta`, caminho do artefato e quantidade de linhas
- [x] Retornar no sync a estrategia efetiva utilizada e o SQL principal da materializacao
- [x] Persistir e expor sidecar de metadata do parquet final da secao materializada
- [x] Registrar no metadata da secao `contato` comparacao basica com a ultima estrategia alternativa disponivel
- [x] Registrar no metadata da secao `contato` convergencia funcional por chave de registro e preenchimento de campos criticos
- [x] Avaliar se secoes `mixed` devem permanecer no modelo atual ou ser unificadas ao cache canonico
- [x] Cobrir erro de secao desconhecida com teste automatizado
- [x] Cobrir falha de persistencia parquet com teste automatizado
- [x] Executar sync real com Oracle para pelo menos uma secao ja existente

### Fase 3: Arquitetura de Reuso de Datasets

Observacao:
- os itens em aberto desta fase representam evolucao da arquitetura global de reuso entre modulos.
- a camada tecnica minima necessaria para o Dossie `contato` ja esta implementada e validada.

- [x] Criar catalogo de datasets compartilhados por entidade e dominio
- [x] Definir arvore canonica de persistencia para datasets compartilhados
- [x] Separar claramente extracao Oracle e composicao analitica em Polars
- [x] Criar camada de resolucao `cache-first` antes de qualquer nova consulta Oracle
- [x] Fazer o Dossie consultar primeiro datasets canonicos existentes antes de abrir nova conexao Oracle
- [x] Implementar leitura por `pl.scan_parquet()` para composicao lazy dos datasets compartilhados
- [x] Padronizar metadados do dataset: SQL de origem, parametros, entidade, data da extracao, versao e dependencias
- [x] Mapear quais extracoes Oracle do estoque podem alimentar o Dossie sem duplicacao
- [x] Mapear quais extracoes do Dossie podem alimentar estoque, ressarcimento ou outras analises sem duplicacao
- [x] Evitar novos Parquets redundantes quando a necessidade puder ser atendida por composicao de datasets existentes
- [x] Definir criterio tecnico para quando um novo parquet base deve ser persistido
- [x] Definir criterio tecnico para quando apenas uma view logica ou composicao deve ser usada

### Fase 4: SQL da Secao `contato`

- [x] Criar `sql/dossie_contato.sql`
- [x] Implementar `CTE_PARAMETROS`
- [x] Implementar `CTE_CONTRIBUINTE`
- [x] Implementar `CTE_FILIAIS_RAIZ`
- [x] Implementar `CTE_CONTATOS_FILIAIS`
- [x] Implementar `CTE_CONTADOR_EMPRESA`
- [x] Implementar `CTE_CONTATOS_CONTADOR`
- [x] Implementar `CTE_SOCIOS_IDS`
- [x] Implementar `CTE_CONTATOS_SOCIOS`
- [x] Implementar `CTE_EMAILS_NFE`
- [x] Implementar `CTE_FONES_CONTADOR_NFE_NFCE`
- [x] Consolidar `CTE_RESULTADO_FINAL` com `UNION ALL`
- [x] Garantir que filiais sem telefone ou email nao sejam excluidas
- [x] Garantir que contador sem telefone ou email nao seja excluido quando houver vinculo
- [x] Garantir que emails de NFe permaneçam restritos ao CNPJ consultado
- [x] Garantir que telefone do contador vindo de NFe/NFCe so entre com reconciliacao valida
- [x] Projetar a consulta para gerar datasets base reutilizaveis, e nao SQL fechado apenas na tela `contato`
- [x] Revisar o SQL com base nas regras do `Agent_SQL.md`
- [x] Validar bind `:CNPJ` sem concatenacao
- [x] Validar custo da busca de filiais, contadores e socios com CNPJ raiz volumoso
- [x] Confirmar os joins previstos a partir dos mapeamentos tecnicos identificados

### Fase 5: Reuso Especifico da Secao `contato`

- [x] Reaproveitar `dados_cadastrais_<cnpj>.parquet` antes de nova extracao cadastral
- [x] Reaproveitar datasets de filiais por `cnpj_raiz` quando ja materializados
- [x] Reaproveitar dados de contador por `GR_IDENT_CONTADOR`, `GR_IDENTIFICACAO` e `CO_CNPJ_CPF_CONTADOR`
- [x] Reaproveitar dados de socios por `GR_IDENTIFICACAO` ou CPF/CNPJ ja materializado
- [x] Reaproveitar `nfe_agr_<cnpj>.parquet`, `nfce_agr_<cnpj>.parquet` e bases correlatas antes de nova consulta de notas
- [x] Reaproveitar telefones e emails de NFe/NFCe ja materializados antes de nova consulta Oracle
- [x] Padronizar a secao `contato` como composicao Polars sobre datasets compartilhados
- [x] Permitir ativacao controlada do `sql/dossie_contato.sql` sem quebrar o fluxo padrao de composicao em Polars
- [x] Persistir apenas o resultado consolidado da secao quando ele representar composicao nova relevante

### Fase 6: Integracao Backend do Dossie

- [x] Adicionar a secao `contato` em `src/interface_grafica/services/dossie_catalog.py`
- [x] Adicionar o alias SQL correspondente em `src/interface_grafica/services/dossie_aliases.py`
- [x] Garantir que `dossie_resolution.py` resolva a nova secao corretamente
- [x] Validar nome do arquivo de cache da secao `contato`
- [x] Garantir que a secao `contato` consulte datasets compartilhados antes de disparar novas extracoes Oracle
- [x] Confirmar persistencia da secao em `CNPJ/<cnpj>/arquivos_parquet/dossie/`
- [x] Executar sync real da secao `contato` via backend
- [x] Garantir que o resumo das secoes reflita corretamente o cache gerado
- [x] Expor no resumo da secao a estrategia da ultima materializacao e a SQL principal quando houver sidecar
- [x] Expor no card da secao `contato` o sinal resumido de convergencia ou divergencia com a estrategia alternada
- [x] Expor no card da secao `contato` a contagem resumida de chaves faltantes e extras na comparacao entre estrategias
- [x] Persistir historico JSONL das comparacoes entre estrategias da secao `contato` por CNPJ
- [x] Expor via backend e interface a leitura do historico de comparacoes da secao `contato`
- [x] Expor via backend e interface um resumo consolidado do historico de comparacoes da secao `contato`
- [x] Materializar relatorio tecnico markdown por CNPJ a partir do historico da secao `contato`
- [x] Gerar relatorio mestre consolidado cruzando comparacao estrutural e relatorios da secao `contato`
- [x] Automatizar no script principal de convergencia a geracao opcional do relatorio mestre
- [x] Diagnosticar explicitamente ausencia de evidencia da secao `contato` no relatorio mestre por CNPJ
- [x] Expor leitura detalhada do cache materializado por secao no backend
- [x] Fazer resumo e leitura priorizarem o cache canonico mais recente da secao, inclusive para syncs com parametros alternativos
- [x] Rejeitar no backend tentativas de sync em secoes `cache_catalog` ou sem SQL mapeada, evitando dependencia exclusiva da regra na UI

### Fase 7: Integracao Frontend da Secao `contato`

- [x] Definir se `DossieTab.tsx` continuara como painel de cards ou se a secao `contato` tera visualizacao detalhada
- [x] Criar componente especifico para leitura da secao `contato`, se necessario
- [x] Exibir agrupamento por `tipo_vinculo`
- [x] Destacar empresa principal
- [x] Agrupar filiais por CNPJ raiz com ordenacao previsivel
- [x] Destacar contador da empresa como grupo proprio
- [x] Sinalizar filiais sem telefone ou email
- [x] Sinalizar contador sem telefone ou email
- [x] Sinalizar telefone do contador vindo de NFe/NFCe
- [x] Preservar sincronizacao da secao no fluxo atual do Dossie
- [x] Expor na interface uma forma controlada de alternar a secao `contato` entre SQL consolidado e composicao Polars
- [x] Evitar criar persistencias redundantes apenas para atender a exibicao

### Fase 8: Testes Automatizados

- [x] Cobrir as rotas de sync com testes automatizados basicos
- [x] Criar teste do fluxo de sincronizacao com reuso e multiplas fontes
- [x] Cobrir erros do motor de sync com testes automatizados
- [x] Criar teste do SQL ou do fluxo da secao `contato` com mocks controlados
- [x] Validar `EMPRESA_PRINCIPAL`
- [x] Validar `MATRIZ_RAIZ` e `FILIAL_RAIZ`
- [x] Validar `CONTADOR_EMPRESA`
- [x] Validar fallback inicial de contador por `dossie_historico_fac.sql`
- [x] Validar fallback de contador por `SITAFE_RASCUNHO_FAC` ou `SITAFE_REQ_INSCRICAO`
- [x] Validar reconciliacao de telefone do contador por NFe/NFCe
- [x] Validar bloco de socios
- [x] Validar que matriz e filiais nao se confundem
- [x] Validar que todas as filiais da mesma raiz aparecem, inclusive sem contato preenchido
- [x] Validar erro Oracle tratavel no backend
- [x] Validar reuso de datasets existentes sem reextracao Oracle
- [x] Validar atualizacao da UI sem reload manual
- [x] Validar mensagens de erro e sucesso por secao
- [x] Validar renderizacao com volume alto de linhas

### Fase 9: Performance e Carga

- [x] Simular empresa com muitas filiais
- [x] Simular empresa com varios socios
- [x] Simular empresa com contador sem contato completo
- [x] Medir tempo de resposta da consulta da secao `contato`
- [x] Medir tempo de materializacao do parquet da secao `contato`
- [x] Medir ganho de reuso quando cadastro, NFe/NFCe ou contador ja estiverem materializados
- [x] Medir impacto da nova camada `cache-first` no fluxo do Dossie

### Fase 10: Verificacao de Convergencia Antes vs Depois

- [x] Criar baseline do "antes" com a extracao atual
- [x] Executar a extracao "depois" com os mesmos CNPJs de referencia
- [x] Comparar o "depois" contra o baseline salvo do "antes"
- [x] Verificar convergencia total para `37671507000187`
- [x] Verificar convergencia total para `84654326000394`
- [x] Investigar qualquer divergencia em arquivos ausentes, novos, schema ou contagem de linhas
- [x] Documentar divergencias aceitaveis, se houver mudanca deliberada
- [x] Reprocessar ate eliminar divergencias nao explicadas

### Fase 11: Validacao Manual

- [x] Executar validacao manual da sincronizacao real em secoes existentes
- [x] Executar validacao manual da secao `contato`
- [x] Conferir consistencia visual da interface web
- [x] Conferir mensagens de erro e sucesso com cenarios reais
- [x] Conferir rastreabilidade do cache gerado por secao

### Fase 12: Documentacao Final

- [x] Manter planos SQL e Contatos documentados
- [x] Criar plano mestre unificado
- [x] Registrar como regra documental a obrigatoriedade de referenciar a fonte dos dados
- [x] Atualizar `docs/plano_sql.md` com o status final
- [x] Atualizar `docs/plano _contatos.md` com o comportamento implementado
- [x] Atualizar `docs/plano_dossie_integral.md` com os itens concluidos
- [x] Atualizar `docs/verificacao_convergencia_extracoes.md` com o resultado final da comparacao
- [x] Atualizar `docs/verificacao_convergencia_extracoes.md` com o procedimento do relatorio mestre de convergencia
- [x] Atualizar esta lista TODO ao fim de cada etapa implementada
- [x] Documentar mudancas contratuais relevantes no README ou na documentacao tecnica do Dossie

## 3. Tabelas Mapeadas Ja Identificadas

### Cadastro e vinculos

- `SITAFE.SITAFE_HISTORICO_CONTRIBUINTE`
- `SITAFE.SITAFE_PESSOA`
- `SITAFE.SITAFE_HISTORICO_SOCIO`
- `SITAFE.SITAFE_RASCUNHO_FAC`
- `SITAFE.SITAFE_REQ_INSCRICAO`
- `BI.DM_CONTRIBUINTE`
- `BI.DM_PESSOA`

### NFe e NFCe para enriquecimento de contato

- `BI.FATO_NFE_DETALHE`
- `BI.FATO_NFCE_DETALHE`
- `BI.DM_NFE_NFCE_CHAVE_ACESSO`
- `BI.VW_FISC_NFE_CABECALHO_XML`

### Reuso e composicao compartilhada

- `dados_cadastrais_<cnpj>.parquet`
- `nfe_agr_<cnpj>.parquet`
- `nfce_agr_<cnpj>.parquet`
- `mov_estoque_<cnpj>.parquet`
- `aba_mensal_<cnpj>.parquet`
- `aba_anual_<cnpj>.parquet`
- futuros datasets canonicos de `contador`, `socio`, `pessoa`, `filial`, `nfe` e `nfce`

## 4. Observacoes em Aberto

- Regras congeladas de contrato da secao `contato`:
  colunas oficiais = `tipo_vinculo`, `cnpj_consultado`, `cnpj_raiz`, `cpf_cnpj_referencia`, `nome_referencia`, `crc_contador`, `endereco`, `telefone`, `telefone_nfe_nfce`, `email`, `telefones_por_fonte`, `emails_por_fonte`, `fontes_contato`, `situacao_cadastral`, `indicador_matriz_filial`, `origem_dado`, `tabela_origem`, `ordem_exibicao`.
- Regras congeladas de `tipo_vinculo`:
  `EMPRESA_PRINCIPAL`, `MATRIZ_RAIZ`, `FILIAL_RAIZ`, `CONTADOR_EMPRESA`, `SOCIO_ATUAL`, `EMAIL_NFE`.
- Regras congeladas de ordenacao:
  `ordem_exibicao`, depois `tipo_vinculo`, depois `nome_referencia`, depois `cpf_cnpj_referencia`.
- Regras congeladas de reuso:
  empresa principal, filiais por raiz, contador, socios atuais e sinais de NFe/NFCe devem ser montados prioritariamente por datasets compartilhados ja materializados; Oracle so abre quando `cache-first` nao localizar base suficiente ou quando houver execucao controlada do `sql_consolidado`.
- Chaves canonicas de reuso congeladas:
  `cnpj`, `cnpj_raiz`, `cpf_cnpj`, `ie`, `gr_identificacao`, `gr_ident_contador`, `chave_acesso`, `id_sefin`.
- Criterio congelado de persistencia:
  novo parquet base so deve existir quando representar extracao Oracle cara e reutilizavel por mais de um modulo; quando o resultado puder ser recomposto de datasets canonicos existentes com custo previsivel em Polars, deve prevalecer view logica/composicao.
- O maior risco tecnico continua sendo a evolucao do motor para compor multiplos `sql_ids` com rastreabilidade adequada.
- O maior risco estrutural continua sendo a ausencia de uma camada global de reuso de datasets Oracle por entidade.
- O maior risco funcional residual da secao `contato` deixou de ser a prioridade do contador, que ja foi congelada com FAC como referencia principal; a maior atencao futura passa a ser a governanca da arquitetura global de reuso entre modulos.
- Telefones de NFe/NFCe do contador devem ser tratados apenas como enriquecimento observado, nunca como identificacao primaria.
- A regra funcional congelada para filiais da mesma raiz passou a considerar todas as filiais encontradas, e nao apenas as ativas.
- A regra funcional congelada para o contador passou a adotar FAC como referencia principal de consolidacao, sem ocultar emails e telefones adicionais das demais fontes; quando existirem, os contatos devem ser mostrados de forma complementar e auditavel.
- A exibicao de socios pessoa fisica no contexto fiscal foi confirmada para a secao `contato`, mantida a rastreabilidade da fonte e o escopo estritamente relacionado ao contribuinte analisado.
- A secao `contato` ja atingiu convergencia funcional total entre `sql_consolidado` e `composicao_polars` nos CNPJs de referencia, inclusive apos materializacao real de filiais por raiz e refresh de `NFe/NFCe` para `shared_sql` atual.
- A implementacao completa so deve ser considerada encerrada quando houver convergencia total entre o baseline atual e o estado posterior as mudancas, salvo divergencia deliberadamente documentada.
- No estado atual do repositorio, nao restam bloqueios tecnicos relevantes para uso do Dossie `contato`; os itens ainda abertos concentram-se em governanca funcional, padronizacao final de contrato e evolucao futura do catalogo global de reuso.

## 5. Historico de Atualizacao

- 2026-04-08: arquivo criado a partir do plano mestre consolidado.
- 2026-04-08: lista expandida para controlar reuso global de datasets Oracle e composicao em Polars sem duplicacao de extracao.
- 2026-04-08: lista reescrita para conter todas as etapas necessarias da implementacao completa, incluindo baseline, reuso, convergencia e documentacao final.
- 2026-04-08: implementada a base tecnica de reuso compartilhado, composicao com multiplos SQLs e secao `contato` composta no backend.
- 2026-04-08: adicionada leitura detalhada do cache por secao no backend e visualizacao especializada da secao `contato` no frontend.
- 2026-04-08: adicionados metadados sidecar dos datasets compartilhados e leitura lazy com `scan_parquet()` na camada de reuso do Dossie.
- 2026-04-08: interface da secao `contato` passou a sinalizar ausencias de contato em filiais e contador, com testes adicionais de matriz, filial e preservacao de registros sem contato.
- 2026-04-08: secao `contato` passou a materializar e exibir `tabela_origem` para reforcar a rastreabilidade da fonte no dado consolidado.
- 2026-04-08: consolidacao do contador passou a usar `dossie_historico_fac.sql` como referencia principal quando houver correspondencia funcional, preservando contatos complementares das demais fontes.
- 2026-04-08: a secao `contato` passou a materializar e exibir `emails_por_fonte`, `telefones_por_fonte` e `fontes_contato` para o bloco `CONTADOR_EMPRESA`, mantendo rastreabilidade completa dos canais por origem.
- 2026-04-08: adicionados SQLs dedicados para fallback do contador em `SITAFE_RASCUNHO_FAC` e `SITAFE_REQ_INSCRICAO`, integrados a secao `contato`.
- 2026-04-08: sync da secao `contato` passou a aceitar ativacao controlada de `dossie_contato.sql` por parametro, mantendo a composicao Polars como padrao.
- 2026-04-08: interface web do Dossie passou a expor a alternancia controlada entre SQL consolidado e composicao Polars para a secao `contato`.
- 2026-04-08: retorno do sync passou a informar explicitamente `estrategia_execucao` e `sql_principal`, e a UI passou a exibir essa auditoria no feedback da sincronizacao.
- 2026-04-08: a secao materializada do Dossie passou a persistir `.metadata.json` proprio, e a leitura detalhada do cache ja exibe essa auditoria no backend e no frontend.
- 2026-04-08: o router do Dossie passou a localizar caches canonicos parametrizados e a priorizar o arquivo final mais recente da secao antes de cair para artefatos legados.
- 2026-04-08: o resumo das secoes passou a refletir tambem `estrategia_execucao` e `sql_principal` do sidecar da ultima materializacao.
- 2026-04-08: a secao `contato` passou a registrar comparacao basica entre `sql_consolidado` e `composicao_polars` quando a estrategia oposta ja estiver materializada no disco.
- 2026-04-08: a comparacao entre estrategias da secao `contato` passou a medir tambem convergencia funcional e diferencas de preenchimento em campos criticos.
- 2026-04-08: o card-resumo da secao `contato` passou a sinalizar convergencia ou divergencia com a estrategia alternada usando o sidecar da ultima materializacao.
- 2026-04-08: o card-resumo da secao `contato` passou a mostrar tambem a quantidade de chaves faltantes e extras quando houver comparacao entre estrategias.
- 2026-04-08: a secao `contato` passou a registrar em JSONL o historico das comparacoes entre estrategias por CNPJ.
- 2026-04-08: backend e frontend passaram a expor a leitura do historico recente de comparacoes da secao `contato`.
- 2026-04-08: nome visivel do produto padronizado para `Fiscal Parquet` no frontend, backend, configuracao da interface desktop e documentacao central.
- 2026-04-08: regra de reconciliacao do telefone do contador em NFe/NFCe foi endurecida para aceitar apenas CPF/CNPJ valido e correspondencia exata, com cobertura automatizada no builder e alinhamento no SQL consolidado.
- 2026-04-08: o sync passou a reaproveitar cache canonico equivalente da mesma secao quando o conteudo final ja existir, evitando gerar parquet redundante para a secao `contato`.
- 2026-04-08: o catalogo de reuso do Dossie passou a reconhecer explicitamente os Parquets analiticos de estoque (`mov_estoque`, `aba_mensal`, `aba_anual`) e ressarcimento (`ressarcimento_st_*`) como fontes canonicas reutilizaveis.
- 2026-04-08: o Dossie passou a expor secoes `estoque` e `ressarcimento_st` em modo `cache_catalog`, reutilizando diretamente artefatos analiticos ja materializados e bloqueando sync desnecessario nessas secoes.
- 2026-04-08: o backend passou a expor `syncEnabled` no resumo das secoes do Dossie, removendo inferencia fraca no frontend sobre quais secoes podem sincronizar.
- 2026-04-08: a secao `arrecadacao` foi unificada ao modelo `cache_catalog`, removendo a classificacao `mixed` onde o backend ja operava apenas com artefatos materializados.
- 2026-04-08: as secoes `cache_catalog` passaram a expor `sourceFiles` no resumo e metadata sintetica com arquivos/tabelas de origem na leitura detalhada.
- 2026-04-08: o backend passou a rejeitar explicitamente tentativas de sync em secoes que operam apenas por leitura de cache, reforcando o contrato `cache_catalog` tambem no nivel da API.
- 2026-04-08: backend e frontend passaram a expor tambem um resumo consolidado do historico de comparacoes da secao `contato`, com totais de convergencia e divergencia por CNPJ.
- 2026-04-08: a secao `contato` passou a gerar relatorio tecnico markdown por CNPJ, persistido ao lado dos artefatos do Dossie para uso como evidencia de convergencia.
- 2026-04-08: criado gerador de relatorio mestre de convergencia, cruzando o JSON da comparacao estrutural antes/depois com os relatorios tecnicos da secao `contato` por CNPJ.
- 2026-04-08: o script principal de verificacao de convergencia passou a suportar geracao automatica do relatorio mestre na mesma execucao da comparacao.
- 2026-04-08: executada a comparacao real do "depois" contra o baseline do "antes" para `37671507000187` e `84654326000394`, com convergencia total nos dois CNPJs e sem divergencias estruturais.
- 2026-04-08: o relatorio mestre passou a distinguir explicitamente ausencia de evidencia da secao `contato` por diretorio do Dossie ainda nao materializado para o CNPJ.
- 2026-04-08: executado sync real da secao `contato` para `37671507000187` e `84654326000394`, com persistencia em `arquivos_parquet/dossie/`, historico de comparacao entre estrategias e relatorios tecnicos materializados.
- 2026-04-08: corrigidos bloqueios reais do sync do `contato`, incluindo import faltante de `Path` no fallback Oracle, deduplicacao de colunas normalizadas no builder e ajuste de codificacao em `dossie_historico_fac.sql`.
- 2026-04-08: o `sql/dossie_contato.sql` foi refinado para reduzir divergencias funcionais, melhorando especialmente a consolidacao do contador no `37671507000187`.
- 2026-04-08: o `sql/dossie_contato.sql` foi corrigido novamente em execucao real contra Oracle para restaurar `CONTADOR_EMPRESA`, evitar truncamento de CPF em `contador_historico_fac` e alinhar `EMAIL_NFE` ao recorte temporal de `NFe.sql`.
- 2026-04-08: apos os ajustes no consolidado, o residual de convergencia funcional caiu para `1 extra` em `37671507000187` e para `1 faltante + 3 extras` no comparativo real mais recente de `84654326000394`, concentrado em filiais por raiz e um email legado de 2019 na referencia Polars.
- 2026-04-08: criada `sql/dossie_filiais_raiz.sql` e integrada a secao `contato`, permitindo que a composicao Polars enumere matriz e filiais da mesma raiz com a mesma base funcional do consolidado.
- 2026-04-08: o reuso de `NFe.sql` e `NFCe.sql` passou a preferir `shared_sql` atual ao legado e a promover artefatos antigos para cache SQL compartilhado atualizado quando necessario.
- 2026-04-08: corrigidos pontos de inferencia de schema no executor SQL e no sync da secao, destravando a materializacao real de `NFe/NFCe` em `shared_sql`.
- 2026-04-08: executados novamente os dois modos da secao `contato` para `37671507000187` e `84654326000394`, com convergencia basica e funcional total entre `composicao_polars` e `sql_consolidado`.
- 2026-04-08: o `SqlService` passou a normalizar colunas de tipo misto apenas quando necessario, e `QueryWorker`/sync do Dossie passaram a reutilizar essa mesma montagem resiliente de `DataFrame` para resultados Oracle amplos.
- 2026-04-08: o `Fisconforme` backend e desktop passaram a reutilizar a mesma montagem resiliente do `SqlService` para resultados Oracle, preservando o contrato de saida e reduzindo falhas por colunas de tipo misto em `fetchall()`.
- 2026-04-08: `extracao_oracle_eficiente.py` passou a reaproveitar o mesmo construtor resiliente do `SqlService` por lote, reduzindo divergencia de schema na gravacao paralela em Parquet.
- 2026-04-08: `extracao_cadastral.py` passou a reutilizar o mesmo construtor resiliente do `SqlService` no retorno unitario de cadastro Oracle, alinhando cache e extração direta do `Fisconforme`.
- 2026-04-08: `extrator_oracle.py` deixou de usar `pandas.read_sql()` e passou a gravar Parquet em lotes por cursor Oracle, reduzindo memoria e alinhando o `Fisconforme` ao padrao incremental do projeto.
- 2026-04-08: `processador_polars.py` passou a reutilizar `LazyFrame` por tabela na mesma instancia, reduzindo reabertura redundante de Parquet no processamento analitico do `Fisconforme`.
- 2026-04-08: `extracao_cadastral.py` passou a exportar o cache por copia direta do Parquet e a calcular estatisticas via `scan_parquet()`, evitando releitura completa desnecessaria.
- 2026-04-08: a secao `contato` passou a contar com teste automatizado de renderizacao com volume alto de linhas, preservando agrupamento por vinculo e exibicao dos registros extremos no frontend.
- 2026-04-08: `DossieContatoDetalhe.tsx` passou a memoizar o agrupamento por `tipo_vinculo`, reduzindo custo de recomputacao na leitura detalhada com volume alto de linhas.
- 2026-04-08: `dossie_section_builder.py` passou a iterar registros com `iter_rows(named=True)` em blocos criticos, reduzindo listas intermediarias em cenarios volumosos; a secao `contato` ganhou teste automatizado para empresa com muitas filiais e varios socios.
- 2026-04-08: a secao `contato` ganhou tambem teste automatizado de carga para contador sem contato completo em cenario volumoso, validando preservacao do bloco `CONTADOR_EMPRESA` mesmo sem telefone, email ou endereco.
- 2026-04-08: o sync da secao `contato` passou a registrar `tempo_materializacao_ms`, `tempo_total_sync_ms`, `total_sql_ids` e `percentual_reuso_sql`, permitindo medir duracao e ganho de reuso diretamente no metadata e na leitura detalhada.
- 2026-04-08: o sync do Dossie passou a classificar explicitamente o impacto da camada `cache-first` como `reuso_total`, `reuso_parcial`, `sem_reuso` ou `cache_canonico_equivalente`, expondo esse resumo no metadata e na leitura detalhada.
- 2026-04-08: validacao manual assistida via navegador realizada no CNPJ `37671507000187`, cobrindo abertura do modulo principal, painel do Dossie, detalhe da secao `contato`, sync real com mensagem de sucesso e conferência visual da rastreabilidade do cache e das fontes exibidas.
- 2026-04-08: validacao manual de erro real na UI concluida ao simular indisponibilidade temporaria da API durante o sync da secao `contato`, com exibicao de erro `Request failed with status code 502` no card da secao e posterior retomada normal do backend.
- 2026-04-08: criado `scripts/medir_performance_dossie_contato.py` e executada medicao real nos CNPJs `37671507000187` e `84654326000394`, com relatorios em `output/performance_dossie_contato/`; nas execucoes medidas houve `reuso_total` do cache-first em ambos os modos, com 55 ms e 38 ms no primeiro CNPJ e 268 ms e 47 ms no segundo, respectivamente para `composicao_polars` e `sql_consolidado`.
