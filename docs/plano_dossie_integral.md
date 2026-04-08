# Plano Mestre de Execucao: Dossie SQL + Contatos

Este documento unifica os planos de `docs/plano_sql.md` e `docs/plano _contatos.md` em uma visao unica de execucao, com ordem recomendada, dependencias, status e checklist integral.

Objetivo: concluir o Dossie como modulo auditavel, navegavel e acionavel, cobrindo tanto a infraestrutura de sincronizacao SQL por secao quanto a nova secao de contatos com empresa principal, filiais por CNPJ raiz, contadores, socios atuais e sinais operacionais de NFe/NFCe.

Checklist operacional relacionado: `docs/lista_todo_plano_mestre.md`

## 1. Documentos de Referencia

- Plano base do motor SQL e da sincronizacao: `docs/plano_sql.md`
- Plano funcional da secao de contatos: `docs/plano _contatos.md`
- Guia tecnico de SQL Oracle: `Agent_SQL.md`

## 2. Estado Consolidado Atual

### Ja concluido

- Guia `Agent_SQL.md` criado.
- Fragmentacao das consultas principais do legado XML em arquivos `.sql`.
- Resolucao de secoes e cache canonico do Dossie implementados.
- Rotas de sincronizacao do Dossie implementadas no backend.
- Integracao React da sincronizacao por secao implementada no `DossieTab`.
- Testes cobrindo o roteamento principal e o alias legado de sync.

### Parcial

- Motor de extracao ainda executa apenas o primeiro `sql_id` da secao.
- O projeto ainda nao possui uma camada global de reuso de datasets Oracle por entidade para Dossie, estoque e demais analises.
- Validacao completa com Oracle real e teste manual/visual da UI ainda nao concluida.

### Nao iniciado

- Secao `contato` ainda nao foi implementada no catalogo do Dossie.
- `sql/dossie_contato.sql` ainda nao foi criado.
- Exibicao especializada de contatos no frontend ainda nao existe.

## 3. Principios de Execucao

- Preservar as regras de negocio ja existentes do Dossie.
- Tratar a secao `contato` como extensao do modelo atual de sincronizacao, sem criar um fluxo paralelo desnecessario.
- Reaproveitar primeiro as chaves e tabelas ja mapeadas no acervo de referencias antes de propor novas consultas.
- Implementar primeiro o contrato de dados e a persistencia antes da visualizacao rica no frontend.
- Manter nomes, comentarios e documentacao em portugues.
- Tratar extracoes Oracle como ativos compartilhados do projeto, e nao como artefatos isolados por tela.
- Priorizar composicao em Polars com `scan_parquet()` e joins sobre datasets ja materializados.
- Toda nova secao do Dossie deve entrar no mesmo circuito:
  resolucao -> execucao SQL -> persistencia parquet -> resumo backend -> sincronizacao frontend.

## 4. Ordem Recomendada de Implementacao

### Fase 1: Fechar pendencias do motor SQL atual

Objetivo: deixar a base do Dossie suficientemente solida antes de acoplar a nova secao `contato`.

- [ ] Evoluir `src/interface_grafica/services/dossie_extraction_service.py` para suportar composicao com multiplos `sql_ids` por secao.
- [ ] Incluir no retorno da sincronizacao metadados auditaveis: `sql_ids_executados`, `cache_key`, `versao_consulta` e caminho do artefato.
- [ ] Criar uma camada compartilhada de datasets canonicos para reuso de extracoes Oracle por entidade.
- [ ] Fazer o Dossie consultar primeiro datasets canonicos existentes antes de abrir nova conexao Oracle.
- [ ] Ampliar testes do POST de sync para erro de secao desconhecida.
- [ ] Ampliar testes do POST de sync para falha de persistencia em parquet.
- [ ] Executar validacao manual de sync real com uma secao ja existente do Dossie.

Dependencia:
- esta fase deve terminar antes da implementacao da secao `contato`, porque ela vai reutilizar o mesmo motor.

### Fase 2: Definir o contrato funcional da secao `contato`

Objetivo: congelar a estrutura de saida antes de escrever o SQL.

- [ ] Confirmar se a enumeracao de filiais por raiz inclui apenas ativas ou todas as filiais encontradas.
- [ ] Confirmar a prioridade de fonte para dados de contador: FAC atual, `SITAFE_PESSOA`, rascunho ou requerimento.
- [ ] Confirmar a regra de reconciliacao para telefone do contador em NFe e NFCe por CPF/CNPJ.
- [ ] Confirmar a permissao de exibicao de dados de socios pessoa fisica no contexto fiscal.
- [ ] Congelar a lista de colunas da secao `contato`.
- [ ] Congelar os valores oficiais de `tipo_vinculo`.
- [ ] Definir a ordenacao funcional da secao no resultado final.
- [ ] Definir quais blocos da secao `contato` serao montados por reuso de datasets existentes e quais exigem nova extracao.

Contrato recomendado:
- `EMPRESA_PRINCIPAL`
- `MATRIZ_RAIZ`
- `FILIAL_RAIZ`
- `CONTADOR_EMPRESA`
- `SOCIO_ATUAL`
- `EMAIL_NFE`

### Fase 3: Implementar o SQL de contatos

Objetivo: entregar a fonte de dados principal da nova secao.

- [ ] Criar `sql/dossie_contato.sql`.
- [ ] Implementar `CTE_PARAMETROS` para normalizar `:CNPJ` e derivar `:CNPJ_RAIZ`.
- [ ] Implementar `CTE_CONTRIBUINTE`.
- [ ] Implementar `CTE_FILIAIS_RAIZ`.
- [ ] Implementar `CTE_CONTATOS_FILIAIS`.
- [ ] Implementar `CTE_CONTADOR_EMPRESA`.
- [ ] Implementar `CTE_CONTATOS_CONTADOR`.
- [ ] Implementar `CTE_SOCIOS_IDS`.
- [ ] Implementar `CTE_CONTATOS_SOCIOS`.
- [ ] Implementar `CTE_EMAILS_NFE`.
- [ ] Implementar `CTE_FONES_CONTADOR_NFE_NFCE`.
- [ ] Consolidar o resultado final em `UNION ALL`.
- [ ] Garantir que filiais sem telefone ou email nao sejam excluidas do resultado.
- [ ] Garantir que contador sem telefone ou email nao seja excluido quando houver vinculo identificado.
- [ ] Garantir que emails de NFe sejam restritos ao CNPJ consultado.
- [ ] Garantir que telefone de NFe/NFCe so seja atribuido ao contador quando houver reconciliacao por CPF/CNPJ.
- [ ] Projetar a consulta para gerar datasets base reaproveitaveis por Dossie e outras analises, evitando SQL fechado apenas na tela `contato`.

Validacoes tecnicas desta fase:
- [ ] Revisar o SQL com base nas regras do `Agent_SQL.md`.
- [ ] Validar bind `:CNPJ` sem concatenacao de string.
- [ ] Validar custo da busca de filiais, contadores e socios com CNPJ raiz volumoso.
- [ ] Validar as chaves canonicas de reuso entre datasets:
  `cnpj`, `cnpj_raiz`, `cpf_cnpj`, `gr_identificacao`, `gr_ident_contador`, `chave_acesso`.
- [ ] Confirmar os joins previstos a partir dos mapeamentos:
  `SITAFE_HISTORICO_CONTRIBUINTE.GR_IDENT_CONTADOR`,
  `SITAFE_PESSOA.GR_IDENTIFICACAO`,
  `SITAFE_RASCUNHO_FAC.GR_IDENT_CONTADOR`,
  `SITAFE_REQ_INSCRICAO.GR_IDENTIFICACAO_CONTADOR`,
  `BI.DM_CONTRIBUINTE.CO_CNPJ_CPF_CONTADOR`,
  `BI.DM_PESSOA.CO_CNPJ_CPF_CONTADOR`,
  `BI.FATO_NFE_DETALHE.CO_EMITENTE`,
  `BI.FATO_NFE_DETALHE.CO_DESTINATARIO`,
  `BI.FATO_NFCE_DETALHE.CO_EMITENTE`,
  `BI.FATO_NFCE_DETALHE.CO_DESTINATARIO`,
  `BI.DM_NFE_NFCE_CHAVE_ACESSO.CO_EMITENTE`,
  `BI.DM_NFE_NFCE_CHAVE_ACESSO.CO_DESTINATARIO`.

### Fase 4: Integrar a secao `contato` no backend do Dossie

Objetivo: tornar a nova secao sincronizavel pelo mecanismo padrao.

- [ ] Adicionar a secao `contato` em `src/interface_grafica/services/dossie_catalog.py`.
- [ ] Adicionar o alias SQL correspondente em `src/interface_grafica/services/dossie_aliases.py`.
- [ ] Garantir que `dossie_resolution.py` resolva a nova secao corretamente.
- [ ] Validar o nome do arquivo de cache da secao `contato`.
- [ ] Garantir que a secao `contato` consulte datasets compartilhados antes de disparar novas extracoes Oracle.
- [ ] Executar sync real da secao `contato` via backend.
- [ ] Confirmar persistencia em `CNPJ/<cnpj>/arquivos_parquet/dossie/`.

### Fase 5: Ajustar o modelo de exibicao do frontend

Objetivo: exibir a nova secao de forma legivel e auditavel.

- [ ] Definir se `DossieTab.tsx` continuara sendo apenas um painel de cards ou se a secao `contato` abrira uma visualizacao detalhada.
- [ ] Criar componente especifico para leitura da secao `contato`, se necessario.
- [ ] Exibir agrupamento por `tipo_vinculo`.
- [ ] Dar destaque visual para a empresa principal consultada.
- [ ] Agrupar filiais por CNPJ raiz com ordenacao previsivel.
- [ ] Exibir contador da empresa como grupo proprio e distinguivel dos socios.
- [ ] Sinalizar claramente filiais sem telefone ou email.
- [ ] Sinalizar claramente contador sem telefone ou email.
- [ ] Sinalizar quando o telefone do contador vier de NFe ou NFCe, distinguindo cadastro formal de telefone observado.
- [ ] Preservar a acao de sincronizacao por secao no mesmo fluxo ja existente.
- [ ] Evitar criar visualizacoes que induzam a novas persistencias redundantes de datasets ja existentes.

### Fase 6: Testes integrados

Objetivo: validar corretude funcional, performance e estabilidade.

#### Backend

- [ ] Criar teste do SQL ou do fluxo da secao `contato` com mocks controlados.
- [ ] Validar que a empresa consultada sai como `EMPRESA_PRINCIPAL`.
- [ ] Validar que matriz e filiais nao se confundem.
- [ ] Validar que todas as filiais da mesma raiz aparecem, inclusive sem contato preenchido.
- [ ] Validar que o contador aparece como `CONTADOR_EMPRESA` quando houver `GR_IDENT_CONTADOR`.
- [ ] Validar fallback do contador para `SITAFE_RASCUNHO_FAC` ou `SITAFE_REQ_INSCRICAO` quando necessario.
- [ ] Validar que telefone vindo de NFe/NFCe so entra quando houver reconciliacao por CPF/CNPJ do contador.
- [ ] Validar que socio contribuinte aparece corretamente no bloco de socios.
- [ ] Validar que falha Oracle gera erro tratavel no backend.
- [ ] Validar que datasets ja materializados sao reaproveitados antes de nova consulta Oracle.

#### Frontend

- [ ] Validar que a sincronizacao da secao `contato` atualiza a UI sem reload manual.
- [ ] Validar mensagens de erro e sucesso por secao.
- [ ] Validar renderizacao com volume alto de linhas na secao `contato`.

#### Performance

- [ ] Simular empresa com muitas filiais na mesma raiz.
- [ ] Simular empresa com contador sem contato completo.
- [ ] Simular empresa com varios socios atuais.
- [ ] Medir tempo de resposta da consulta e da materializacao do parquet.
- [ ] Medir ganho de reuso quando cadastro, NFe/NFCe ou contador ja estiverem materializados.

### Fase 7: Documentacao final

Objetivo: fechar o ciclo sem deixar divergencia documental.

- [ ] Atualizar `docs/plano_sql.md` com o status final do motor SQL e da nova secao `contato`.
- [ ] Atualizar `docs/plano _contatos.md` com decisoes finais de contrato e implementacao real.
- [ ] Atualizar este plano mestre com os itens concluidos.
- [ ] Se houver mudanca relevante no contrato de dados, documentar tambem no README ou na documentacao tecnica do Dossie.

## 5. Checklist Integral Consolidado

### Base tecnica do Dossie

- [x] Criar o guia SQL do projeto.
- [x] Fragmentar as consultas principais do legado XML.
- [x] Implementar sincronizacao backend por secao.
- [x] Integrar sincronizacao por secao no frontend.
- [ ] Suportar multiplos `sql_ids` por secao.
- [ ] Retornar metadados auditaveis da sincronizacao.
- [ ] Criar camada global de reuso de datasets Oracle por entidade.
- [ ] Conectar datasets compartilhados em Polars antes de reconsultar o Oracle.
- [ ] Validar sync real com Oracle.

### Secao contato

- [ ] Definir regra final de filiais por raiz.
- [ ] Definir regra final de prioridade e fallback para dados do contador.
- [ ] Definir regra final para telefones do contador em NFe/NFCe.
- [ ] Definir regra final de exibicao de dados de socios.
- [ ] Criar `sql/dossie_contato.sql`.
- [ ] Integrar `contato` ao catalogo e aos aliases do Dossie.
- [ ] Persistir a secao `contato` no cache canonico.
- [ ] Reaproveitar datasets existentes de empresa, filial, contador, socio, NFe e NFCe antes de nova extracao.
- [ ] Exibir a secao `contato` com agrupamento por vinculo.

### Validacao

- [x] Cobrir as rotas de sync com testes automatizados basicos.
- [ ] Cobrir erros do motor de sync com testes automatizados.
- [ ] Cobrir o fluxo de `contato` com testes automatizados.
- [ ] Executar validacao manual/visual da interface.
- [ ] Executar testes de performance com empresas de alta cardinalidade.

### Documentacao

- [x] Manter planos SQL e Contatos documentados.
- [x] Criar plano mestre unificado.
- [ ] Atualizar a documentacao apos a implementacao real da secao `contato`.

## 6. Criterio de Conclusao Integral

O plano integral pode ser considerado concluido quando:

1. O motor SQL do Dossie suportar a secao `contato` sem excecoes arquiteturais.
2. A secao `contato` puder ser sincronizada via backend e pela interface web.
3. O sistema reaproveitar datasets ja materializados antes de abrir nova extracao Oracle sempre que houver chave canonica compativel.
4. O resultado incluir empresa principal, filiais por CNPJ raiz, contadores, socios atuais e emails de NFe conforme as regras definidas.
5. Os testes cobrirem fluxo feliz, erros relevantes, comportamento com alto volume e reuso sem reextracao desnecessaria.
6. A documentacao estiver alinhada ao comportamento real do sistema.

## 7. Observacoes Finais

- O maior risco tecnico atual nao esta no frontend, e sim na evolucao do motor de extracao para compor mais de um SQL por secao com rastreabilidade adequada.
- O maior risco tecnico transversal e a ausencia de uma camada global de reuso de datasets, o que pode duplicar extracoes Oracle e proliferar Parquets por caso de uso.
- O maior risco funcional da secao `contato` esta na definicao do que entra como filial por raiz, na prioridade de fonte para dados do contador, no tratamento de dados de socios pessoa fisica e na precedencia entre dado cadastral e dado reaproveitado.
- Se essas tres definicoes forem fechadas cedo, a execucao do restante do plano tende a ser linear e previsivel.
