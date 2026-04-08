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

- Status global: Em andamento
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
- [ ] Confirmar oficialmente que qualquer divergencia do "depois" precisa ser documentada antes da aprovacao
- [ ] Registrar no historico desta lista a data de inicio efetivo da implementacao

### Fase 1: Decisoes Funcionais Obrigatorias

- [ ] Definir se a enumeracao de filiais por raiz inclui apenas ativas ou todas as encontradas
- [ ] Definir prioridade de fonte para dados do contador
- [ ] Definir fallback final do contador entre FAC atual, `SITAFE_PESSOA`, `SITAFE_RASCUNHO_FAC` e `SITAFE_REQ_INSCRICAO`
- [ ] Definir regra de reconciliacao para telefone do contador em NFe e NFCe por CPF/CNPJ
- [ ] Confirmar permissao de exibicao de dados de socios pessoa fisica
- [ ] Congelar colunas finais da secao `contato`
- [ ] Congelar valores finais de `tipo_vinculo`
- [ ] Definir ordenacao funcional do resultado da secao `contato`
- [ ] Definir quais blocos da secao `contato` serao montados apenas por datasets ja materializados
- [ ] Definir quais blocos exigirao nova extracao Oracle
- [ ] Definir chaves canonicas de reuso para `cnpj`, `cnpj_raiz`, `cpf_cnpj`, `ie`, `gr_identificacao`, `gr_ident_contador`, `chave_acesso` e `id_sefin`

### Fase 2: Motor SQL Atual do Dossie

- [x] Criar o guia SQL do projeto
- [x] Fragmentar as consultas principais do legado XML em arquivos `.sql`
- [x] Implementar sincronizacao backend por secao
- [x] Integrar sincronizacao por secao no frontend
- [ ] Evoluir `src/interface_grafica/services/dossie_extraction_service.py` para suportar multiplos `sql_ids` por secao
- [ ] Fazer a execucao da secao suportar composicao real de mais de uma consulta
- [ ] Retornar metadados auditaveis da sincronizacao: `sql_ids_executados`, `cache_key`, `versao_consulta`, caminho do artefato e quantidade de linhas
- [ ] Avaliar se secoes `mixed` devem permanecer no modelo atual ou ser unificadas ao cache canonico
- [ ] Cobrir erro de secao desconhecida com teste automatizado
- [ ] Cobrir falha de persistencia parquet com teste automatizado
- [ ] Executar sync real com Oracle para pelo menos uma secao ja existente

### Fase 3: Arquitetura de Reuso de Datasets

- [ ] Criar catalogo de datasets compartilhados por entidade e dominio
- [ ] Definir arvore canonica de persistencia para datasets compartilhados
- [ ] Separar claramente extracao Oracle e composicao analitica em Polars
- [ ] Criar camada de resolucao `cache-first` antes de qualquer nova consulta Oracle
- [ ] Fazer o Dossie consultar primeiro datasets canonicos existentes antes de abrir nova conexao Oracle
- [ ] Implementar leitura por `pl.scan_parquet()` para composicao lazy dos datasets compartilhados
- [ ] Padronizar metadados do dataset: SQL de origem, parametros, entidade, data da extracao, versao e dependencias
- [ ] Mapear quais extracoes Oracle do estoque podem alimentar o Dossie sem duplicacao
- [ ] Mapear quais extracoes do Dossie podem alimentar estoque, ressarcimento ou outras analises sem duplicacao
- [ ] Evitar novos Parquets redundantes quando a necessidade puder ser atendida por composicao de datasets existentes
- [ ] Definir criterio tecnico para quando um novo parquet base deve ser persistido
- [ ] Definir criterio tecnico para quando apenas uma view logica ou composicao deve ser usada

### Fase 4: SQL da Secao `contato`

- [ ] Criar `sql/dossie_contato.sql`
- [ ] Implementar `CTE_PARAMETROS`
- [ ] Implementar `CTE_CONTRIBUINTE`
- [ ] Implementar `CTE_FILIAIS_RAIZ`
- [ ] Implementar `CTE_CONTATOS_FILIAIS`
- [ ] Implementar `CTE_CONTADOR_EMPRESA`
- [ ] Implementar `CTE_CONTATOS_CONTADOR`
- [ ] Implementar `CTE_SOCIOS_IDS`
- [ ] Implementar `CTE_CONTATOS_SOCIOS`
- [ ] Implementar `CTE_EMAILS_NFE`
- [ ] Implementar `CTE_FONES_CONTADOR_NFE_NFCE`
- [ ] Consolidar `CTE_RESULTADO_FINAL` com `UNION ALL`
- [ ] Garantir que filiais sem telefone ou email nao sejam excluidas
- [ ] Garantir que contador sem telefone ou email nao seja excluido quando houver vinculo
- [ ] Garantir que emails de NFe permaneçam restritos ao CNPJ consultado
- [ ] Garantir que telefone do contador vindo de NFe/NFCe so entre com reconciliacao valida
- [ ] Projetar a consulta para gerar datasets base reutilizaveis, e nao SQL fechado apenas na tela `contato`
- [ ] Revisar o SQL com base nas regras do `Agent_SQL.md`
- [ ] Validar bind `:CNPJ` sem concatenacao
- [ ] Validar custo da busca de filiais, contadores e socios com CNPJ raiz volumoso
- [ ] Confirmar os joins previstos a partir dos mapeamentos tecnicos identificados

### Fase 5: Reuso Especifico da Secao `contato`

- [ ] Reaproveitar `dados_cadastrais_<cnpj>.parquet` antes de nova extracao cadastral
- [ ] Reaproveitar datasets de filiais por `cnpj_raiz` quando ja materializados
- [ ] Reaproveitar dados de contador por `GR_IDENT_CONTADOR`, `GR_IDENTIFICACAO` e `CO_CNPJ_CPF_CONTADOR`
- [ ] Reaproveitar dados de socios por `GR_IDENTIFICACAO` ou CPF/CNPJ ja materializado
- [ ] Reaproveitar `nfe_agr_<cnpj>.parquet`, `nfce_agr_<cnpj>.parquet` e bases correlatas antes de nova consulta de notas
- [ ] Reaproveitar telefones e emails de NFe/NFCe ja materializados antes de nova consulta Oracle
- [ ] Padronizar a secao `contato` como composicao Polars sobre datasets compartilhados
- [ ] Persistir apenas o resultado consolidado da secao quando ele representar composicao nova relevante

### Fase 6: Integracao Backend do Dossie

- [ ] Adicionar a secao `contato` em `src/interface_grafica/services/dossie_catalog.py`
- [ ] Adicionar o alias SQL correspondente em `src/interface_grafica/services/dossie_aliases.py`
- [ ] Garantir que `dossie_resolution.py` resolva a nova secao corretamente
- [ ] Validar nome do arquivo de cache da secao `contato`
- [ ] Garantir que a secao `contato` consulte datasets compartilhados antes de disparar novas extracoes Oracle
- [ ] Confirmar persistencia da secao em `CNPJ/<cnpj>/arquivos_parquet/dossie/`
- [ ] Executar sync real da secao `contato` via backend
- [ ] Garantir que o resumo das secoes reflita corretamente o cache gerado

### Fase 7: Integracao Frontend da Secao `contato`

- [ ] Definir se `DossieTab.tsx` continuara como painel de cards ou se a secao `contato` tera visualizacao detalhada
- [ ] Criar componente especifico para leitura da secao `contato`, se necessario
- [ ] Exibir agrupamento por `tipo_vinculo`
- [ ] Destacar empresa principal
- [ ] Agrupar filiais por CNPJ raiz com ordenacao previsivel
- [ ] Destacar contador da empresa como grupo proprio
- [ ] Sinalizar filiais sem telefone ou email
- [ ] Sinalizar contador sem telefone ou email
- [ ] Sinalizar telefone do contador vindo de NFe/NFCe
- [ ] Preservar sincronizacao da secao no fluxo atual do Dossie
- [ ] Evitar criar persistencias redundantes apenas para atender a exibicao

### Fase 8: Testes Automatizados

- [x] Cobrir as rotas de sync com testes automatizados basicos
- [ ] Cobrir erros do motor de sync com testes automatizados
- [ ] Criar teste do SQL ou do fluxo da secao `contato` com mocks controlados
- [ ] Validar `EMPRESA_PRINCIPAL`
- [ ] Validar `MATRIZ_RAIZ` e `FILIAL_RAIZ`
- [ ] Validar `CONTADOR_EMPRESA`
- [ ] Validar fallback de contador por `SITAFE_RASCUNHO_FAC` ou `SITAFE_REQ_INSCRICAO`
- [ ] Validar reconciliacao de telefone do contador por NFe/NFCe
- [ ] Validar bloco de socios
- [ ] Validar que matriz e filiais nao se confundem
- [ ] Validar que todas as filiais da mesma raiz aparecem, inclusive sem contato preenchido
- [ ] Validar erro Oracle tratavel no backend
- [ ] Validar reuso de datasets existentes sem reextracao Oracle
- [ ] Validar atualizacao da UI sem reload manual
- [ ] Validar mensagens de erro e sucesso por secao
- [ ] Validar renderizacao com volume alto de linhas

### Fase 9: Performance e Carga

- [ ] Simular empresa com muitas filiais
- [ ] Simular empresa com varios socios
- [ ] Simular empresa com contador sem contato completo
- [ ] Medir tempo de resposta da consulta da secao `contato`
- [ ] Medir tempo de materializacao do parquet da secao `contato`
- [ ] Medir ganho de reuso quando cadastro, NFe/NFCe ou contador ja estiverem materializados
- [ ] Medir impacto da nova camada `cache-first` no fluxo do Dossie

### Fase 10: Verificacao de Convergencia Antes vs Depois

- [x] Criar baseline do "antes" com a extracao atual
- [ ] Executar a extracao "depois" com os mesmos CNPJs de referencia
- [ ] Comparar o "depois" contra o baseline salvo do "antes"
- [ ] Verificar convergencia total para `37671507000187`
- [ ] Verificar convergencia total para `84654326000394`
- [ ] Investigar qualquer divergencia em arquivos ausentes, novos, schema ou contagem de linhas
- [ ] Documentar divergencias aceitaveis, se houver mudanca deliberada
- [ ] Reprocessar ate eliminar divergencias nao explicadas

### Fase 11: Validacao Manual

- [ ] Executar validacao manual da sincronizacao real em secoes existentes
- [ ] Executar validacao manual da secao `contato`
- [ ] Conferir consistencia visual da interface web
- [ ] Conferir mensagens de erro e sucesso com cenarios reais
- [ ] Conferir rastreabilidade do cache gerado por secao

### Fase 12: Documentacao Final

- [x] Manter planos SQL e Contatos documentados
- [x] Criar plano mestre unificado
- [ ] Atualizar `docs/plano_sql.md` com o status final
- [ ] Atualizar `docs/plano _contatos.md` com o comportamento implementado
- [ ] Atualizar `docs/plano_dossie_integral.md` com os itens concluidos
- [ ] Atualizar `docs/verificacao_convergencia_extracoes.md` com o resultado final da comparacao
- [ ] Atualizar esta lista TODO ao fim de cada etapa implementada
- [ ] Documentar mudancas contratuais relevantes no README ou na documentacao tecnica do Dossie

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

- O maior risco tecnico continua sendo a evolucao do motor para compor multiplos `sql_ids` com rastreabilidade adequada.
- O maior risco estrutural continua sendo a ausencia de uma camada global de reuso de datasets Oracle por entidade.
- O maior risco funcional da secao `contato` continua sendo a definicao de prioridade entre FAC atual, cadastro de pessoa, rascunho, requerimento e sinais de NFe/NFCe.
- Telefones de NFe/NFCe do contador devem ser tratados apenas como enriquecimento observado, nunca como identificacao primaria.
- A implementacao completa so deve ser considerada encerrada quando houver convergencia total entre o baseline atual e o estado posterior as mudancas, salvo divergencia deliberadamente documentada.

## 5. Historico de Atualizacao

- 2026-04-08: arquivo criado a partir do plano mestre consolidado.
- 2026-04-08: lista expandida para controlar reuso global de datasets Oracle e composicao em Polars sem duplicacao de extracao.
- 2026-04-08: lista reescrita para conter todas as etapas necessarias da implementacao completa, incluindo baseline, reuso, convergencia e documentacao final.
