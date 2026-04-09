# Plano Mestre de Execucao: Dossie SQL + Contatos

Este documento unifica os planos de `docs/plano_sql.md` e `docs/plano _contatos.md` em uma visao unica de execucao, com ordem recomendada, dependencias, status e checklist integral.

Objetivo: concluir o Dossie como modulo auditavel, navegavel e acionavel, cobrindo tanto a infraestrutura de sincronizacao SQL por secao quanto a nova secao de contatos com empresa principal, filiais por CNPJ raiz, contadores, socios atuais e sinais operacionais de NFe/NFCe.

Regra transversal obrigatoria:
toda informacao materializada, exibida ou documentada no Dossie deve manter rastreabilidade da fonte, idealmente com referencia a tabela ou view de origem no banco, alem do `sql_id`, dataset compartilhado ou parquet correspondente.

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
- Convergencia estrutural "antes vs depois" total nos CNPJs `37671507000187` e `84654326000394`.
- Convergencia funcional total da secao `contato` entre `composicao_polars` e `sql_consolidado` nos CNPJs de referencia.
- Endurecimento dos executores Oracle fora do Dossie ja aplicado em `Fisconforme`, extracao Oracle eficiente e cache cadastral.

### Parcial

- O projeto ja possui uma camada inicial de reuso de datasets Oracle por SQL e por artefatos canonicos conhecidos, incluindo metadados sidecar e leitura lazy por `scan_parquet()`, mas ainda falta ampliacao do catalogo compartilhado e criterio tecnico final para persistencia de novos datasets base.
- A interface web ja permite abrir o cache materializado da secao e possui visualizacao especializada para `contato`, mas ainda faltam validacao manual real e cobertura de carga.

### Nao iniciado

- Integracao de reuso entre Dossie, estoque e ressarcimento ainda nao foi consolidada em um catalogo unico.

### Implementado nesta etapa

- Secao `contato` adicionada ao catalogo do Dossie como composicao de datasets compartilhados.
- Reuso `cache-first` implementado para `dados_cadastrais.sql`, `NFe.sql`, `NFCe.sql` e datasets SQL compartilhados em `arquivos_parquet/shared_sql/`.
- Composicao Polars da secao `contato` implementada com empresa principal, filiais reaproveitadas por `cnpj_raiz`, contador, socios atuais e emails/telefones observados em documentos quando existirem.
- Backend passou a expor leitura detalhada do parquet materializado por secao sem reexecutar Oracle.
- `DossieTab` passou a abrir a secao selecionada e a renderizar `contato` com agrupamento por `tipo_vinculo`, destaque de empresa principal e indicacao de telefones observados em NFe/NFCe.
- Datasets compartilhados agora persistem metadata sidecar com SQL de origem, parametros, versao e timestamp de extracao, e a leitura de reuso passou a usar `scan_parquet()` como base.
- A secao `contato` agora materializa e exibe tambem `tabela_origem`, reforcando a rastreabilidade ate o nivel de tabela/view de banco.
- A consolidacao do contador agora usa `dossie_historico_fac.sql` como referencia principal quando houver correspondencia funcional, sem ocultar emails e telefones complementares de `SITAFE_PESSOA`, rascunho, requerimento ou sinais reconciliados de NFe/NFCe.
- A secao `contato` agora possui SQLs dedicados para fallback do contador em `SITAFE_RASCUNHO_FAC` e `SITAFE_REQ_INSCRICAO`, integrados ao fluxo padrao de reuso e composicao.
- `sql/dossie_contato.sql` foi criado como consulta consolidada inicial e o builder da secao ja sabe consumir esse contrato quando ele estiver disponivel.
- O sync da secao `contato` agora suporta ativacao controlada do caminho consolidado por parametro, sem substituir o fluxo padrao de composicao em Polars.
- A interface web do Dossie agora expoe esse modo controlado, permitindo alternar a sincronizacao da secao `contato` entre SQL consolidado e composicao Polars reutilizavel.
- O retorno do sync agora informa explicitamente `estrategia_execucao`, `sql_principal`, `sql_ids_executados` e `sql_ids_reutilizados`, reforcando a auditabilidade do caminho usado em cada materializacao.
- O parquet final de cada secao materializada agora pode carregar sidecar `.metadata.json`, e a leitura detalhada do cache ja expoe essa auditoria no backend e no frontend.
- O router do Dossie agora prioriza automaticamente o cache canonico mais recente da secao, inclusive quando ele foi materializado com parametros alternativos, evitando somar artefatos legados ao resumo da secao.
- O resumo das secoes agora tambem carrega do sidecar a estrategia da ultima materializacao e a SQL principal usada, permitindo auditoria basica ja no card da secao.
- A secao `contato` agora registra no metadata comparacao basica contra o ultimo cache disponivel da estrategia oposta, sem nova extracao Oracle, indicando convergencia ou divergencia de contrato essencial.
- A comparacao entre estrategias da secao `contato` agora inclui tambem convergencia funcional por chave de registro e resumo de preenchimento de campos criticos, como telefone, email, endereco e tabela de origem.
- O resumo visual das secoes agora tambem sinaliza, quando disponivel no sidecar, se a ultima materializacao de `contato` convergiu ou divergiu da estrategia alternada.
- O card-resumo de `contato` agora tambem pode exibir a magnitude da divergencia, com contagem de chaves faltantes e extras em relacao a estrategia alternada.
- A secao `contato` agora pode acumular historico JSONL das comparacoes entre estrategias por CNPJ, preparando a trilha de evidencias para validacoes reais com Oracle.
- O backend agora expoe esse historico de comparacoes do `contato` por rota dedicada, e a interface detalhada da secao ja consegue listar as ultimas comparacoes registradas.
- O backend agora tambem consolida esse historico em um resumo por CNPJ, com totais de convergencia e divergencia, reduzindo leitura manual de eventos para acompanhamento da convergencia.
- O Dossie agora tambem consegue materializar um relatorio tecnico markdown por CNPJ para a secao `contato`, transformando o historico de comparacoes em evidencia persistida para auditoria e acompanhamento do plano mestre.
- O projeto agora possui tambem um gerador de relatorio mestre de convergencia, que cruza a comparacao estrutural "antes vs depois" com os relatorios por CNPJ da secao `contato`.
- O script principal de verificacao de convergencia agora tambem pode disparar automaticamente a geracao desse relatorio mestre, reduzindo trabalho manual na etapa final do plano.
- O relatorio mestre agora diagnostica explicitamente quando a evidencia da secao `contato` ainda nao existe porque o diretorio do Dossie nao foi materializado para o CNPJ avaliado.
- O sync real da secao `contato` ja foi executado para os CNPJs de referencia, com persistencia em `arquivos_parquet/dossie/`, historico de comparacao entre estrategias e relatorios tecnicos por CNPJ.
- A convergencia estrutural do projeto ficou total nos CNPJs de referencia e a convergencia funcional da secao `contato` entre `composicao_polars` e `sql_consolidado` tambem ficou total apos os ajustes finais desta etapa.
- O `sql/dossie_contato.sql` foi corrigido para evitar perda total do bloco `CONTADOR_EMPRESA` no Oracle, removendo comparacoes com `''` incompatíveis com Oracle, eliminando truncamento indevido de CPF em `contador_historico_fac` e alinhando o recorte de `EMAIL_NFE` ao filtro de `NFe.sql`.
- A composicao Polars da secao `contato` agora tambem consome `dossie_filiais_raiz.sql`, deixando de depender apenas das pastas locais de CNPJ para enumeracao de matriz e filiais da mesma raiz.
- O reuso de `NFe.sql` e `NFCe.sql` agora prioriza `shared_sql` atual e promove automaticamente artefatos legados para cache SQL compartilhado quando necessario, eliminando contaminacao por referencias antigas fora do contrato atual.
- Os dois CNPJs de referencia (`37671507000187` e `84654326000394`) ja tiveram sync real nos dois modos apos esses ajustes e passaram a registrar `convergencia_basica = true` e `convergencia_funcional = true` nas comparacoes entre estrategias.
- O sync agora tambem consegue reaproveitar o cache canonico mais recente da propria secao quando o resultado final for equivalente, evitando gerar um novo parquet redundante so por repeticao de materializacao.
- A camada de reuso compartilhado do Dossie agora reconhece tambem os Parquets analiticos de estoque e ressarcimento (`mov_estoque`, `aba_mensal`, `aba_anual` e `ressarcimento_st_*`) como datasets canonicos conhecidos do workspace.
- O catalogo do Dossie agora expoe tambem secoes de leitura `estoque` e `ressarcimento_st`, operando em modo `cache_catalog`, isto e, apenas sobre artefatos ja materializados e sem sync Oracle proprio.
- O resumo das secoes agora explicita tambem `syncEnabled`, para que frontend e auditoria saibam quais secoes realmente possuem rota de sincronizacao util no contrato atual e quais operam apenas como leitura de cache.
- A secao `arrecadacao` deixou de ficar ambigua no catalogo e passou a ser tratada explicitamente como `cache_catalog`, alinhando o contrato com o comportamento real do backend.
- As secoes `cache_catalog` agora expoem tambem `sourceFiles` no resumo e metadata sintetica na leitura detalhada, registrando quais arquivos efetivamente sustentaram a visualizacao quando nao houver sidecar proprio.
- O backend agora rejeita explicitamente tentativas de sync em secoes `cache_catalog`, evitando que a restricao fique apenas na interface e reforcando o contrato de leitura pura dessas secoes.

## 3. Principios de Execucao

- Preservar as regras de negocio ja existentes do Dossie.
- Tratar a secao `contato` como extensao do modelo atual de sincronizacao, sem criar um fluxo paralelo desnecessario.
- Toda linha, secao ou dataset deve carregar referencia auditavel de origem do dado sempre que isso for tecnicamente viavel.
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

- [x] Evoluir `src/interface_grafica/services/dossie_extraction_service.py` para suportar composicao com multiplos `sql_ids` por secao.
- [x] Incluir no retorno da sincronizacao metadados auditaveis: `sql_ids_executados`, `cache_key`, `versao_consulta` e caminho do artefato.
- [x] Criar uma camada compartilhada de datasets canonicos para reuso de extracoes Oracle por entidade.
- [x] Fazer o Dossie consultar primeiro datasets canonicos existentes antes de abrir nova conexao Oracle.
- [x] Ampliar testes do POST de sync para erro de secao desconhecida.
- [x] Ampliar testes do POST de sync para falha de persistencia em parquet.
- [x] Executar validacao manual de sync real com uma secao ja existente do Dossie.

Dependencia:
- esta fase deve terminar antes da implementacao da secao `contato`, porque ela vai reutilizar o mesmo motor.

### Fase 2: Definir o contrato funcional da secao `contato`

Objetivo: congelar a estrutura de saida antes de escrever o SQL.

- [x] Confirmar se a enumeracao de filiais por raiz inclui apenas ativas ou todas as filiais encontradas.
- [x] Confirmar a prioridade de fonte para dados de contador: FAC atual, `SITAFE_PESSOA`, rascunho ou requerimento.
- [x] Confirmar a regra de reconciliacao para telefone do contador em NFe e NFCe por CPF/CNPJ.
- [x] Confirmar a permissao de exibicao de dados de socios pessoa fisica no contexto fiscal.
- [x] Congelar a lista de colunas da secao `contato`.
- [x] Congelar os valores oficiais de `tipo_vinculo`.
- [x] Definir a ordenacao funcional da secao no resultado final.
- [x] Definir quais blocos da secao `contato` serao montados por reuso de datasets existentes e quais exigem nova extracao.

Contrato recomendado:
- `EMPRESA_PRINCIPAL`
- `MATRIZ_RAIZ`
- `FILIAL_RAIZ`
- `CONTADOR_EMPRESA`
- `SOCIO_ATUAL`
- `EMAIL_NFE`

Decisoes funcionais ja confirmadas:
- a enumeracao por raiz deve incluir todas as filiais encontradas.
- a consolidacao do contador deve usar FAC como referencia principal, mas sem descartar emails e telefones complementares encontrados em `SITAFE_PESSOA`, rascunho, requerimento ou sinais operacionais validos.
- a exibicao de socios pessoa fisica esta autorizada no contexto fiscal deste Dossie.

Regras congeladas do contrato:
- colunas oficiais da secao `contato`: `tipo_vinculo`, `cnpj_consultado`, `cnpj_raiz`, `cpf_cnpj_referencia`, `nome_referencia`, `crc_contador`, `endereco`, `telefone`, `telefone_nfe_nfce`, `email`, `telefones_por_fonte`, `emails_por_fonte`, `fontes_contato`, `situacao_cadastral`, `indicador_matriz_filial`, `origem_dado`, `tabela_origem`, `ordem_exibicao`.
- valores oficiais de `tipo_vinculo`: `EMPRESA_PRINCIPAL`, `MATRIZ_RAIZ`, `FILIAL_RAIZ`, `CONTADOR_EMPRESA`, `SOCIO_ATUAL`, `EMAIL_NFE`.
- ordenacao funcional oficial: `ordem_exibicao ASC`, depois `tipo_vinculo ASC`, depois `nome_referencia ASC`, depois `cpf_cnpj_referencia ASC`.
- ordem funcional oficial por bloco: `EMPRESA_PRINCIPAL=10`, `MATRIZ_RAIZ=20`, `FILIAL_RAIZ=25`, `CONTADOR_EMPRESA=30`, `SOCIO_ATUAL=40`, `EMAIL_NFE(NFe)=50`, `EMAIL_NFE(NFCe)=55`.
- exibicao oficial de socios: somente socios atuais, sem historico societario completo, com permissao de exibicao tambem para pessoa fisica no contexto fiscal.
- caminho padrao oficial da secao: `composicao_polars` sobre datasets compartilhados; `sql_consolidado` permanece como caminho controlado de auditoria e comparacao.
- blocos montados prioritariamente por reuso de datasets materializados: empresa principal, filiais por raiz, contador, socios atuais, emails de NFe/NFCe e telefones reconciliados do contador.
- nova extracao Oracle so deve ocorrer quando o `cache-first` nao localizar dataset compartilhado suficiente para o bloco solicitado, ou quando o usuario/fluxo acionar explicitamente o modo `sql_consolidado`.

### Fase 3: Implementar o SQL de contatos

Objetivo: entregar a fonte de dados principal da nova secao.

- [x] Criar `sql/dossie_contato.sql`.
- [x] Implementar `CTE_PARAMETROS` para normalizar `:CNPJ` e derivar `:CNPJ_RAIZ`.
- [x] Implementar `CTE_CONTRIBUINTE`.
- [x] Implementar `CTE_FILIAIS_RAIZ`.
- [x] Implementar `CTE_CONTATOS_FILIAIS`.
- [x] Implementar `CTE_CONTADOR_EMPRESA`.
- [x] Implementar `CTE_CONTATOS_CONTADOR`.
- [x] Implementar `CTE_SOCIOS_IDS`.
- [x] Implementar `CTE_CONTATOS_SOCIOS`.
- [x] Implementar `CTE_EMAILS_NFE`.
- [x] Implementar `CTE_FONES_CONTADOR_NFE_NFCE`.
- [x] Consolidar o resultado final em `UNION ALL`.
- [x] Garantir que filiais sem telefone ou email nao sejam excluidas do resultado.
- [x] Garantir que contador sem telefone ou email nao seja excluido quando houver vinculo identificado.
- [x] Garantir que emails de NFe sejam restritos ao CNPJ consultado.
- [x] Garantir que telefone de NFe/NFCe so seja atribuido ao contador quando houver reconciliacao por CPF/CNPJ.
- [x] Projetar a consulta para gerar datasets base reaproveitaveis por Dossie e outras analises, evitando SQL fechado apenas na tela `contato`.

Validacoes tecnicas desta fase:
- [x] Revisar o SQL com base nas regras do `Agent_SQL.md`.
- [x] Validar bind `:CNPJ` sem concatenacao de string.
- [x] Validar custo da busca de filiais, contadores e socios com CNPJ raiz volumoso.
- [x] Validar as chaves canonicas de reuso entre datasets:
  `cnpj`, `cnpj_raiz`, `cpf_cnpj`, `gr_identificacao`, `gr_ident_contador`, `chave_acesso`.
- [x] Confirmar os joins previstos a partir dos mapeamentos:
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

- [x] Adicionar a secao `contato` em `src/interface_grafica/services/dossie_catalog.py`.
- [x] Adicionar o alias SQL correspondente em `src/interface_grafica/services/dossie_aliases.py`.
- [x] Garantir que `dossie_resolution.py` resolva a nova secao corretamente.
- [x] Validar o nome do arquivo de cache da secao `contato`.
- [x] Garantir que a secao `contato` consulte datasets compartilhados antes de disparar novas extracoes Oracle.
- [x] Executar sync real da secao `contato` via backend.
- [x] Confirmar persistencia em `CNPJ/<cnpj>/arquivos_parquet/dossie/`.

### Fase 5: Ajustar o modelo de exibicao do frontend

Objetivo: exibir a nova secao de forma legivel e auditavel.

- [x] Definir se `DossieTab.tsx` continuara sendo apenas um painel de cards ou se a secao `contato` abrira uma visualizacao detalhada.
- [x] Criar componente especifico para leitura da secao `contato`, se necessario.
- [x] Exibir agrupamento por `tipo_vinculo`.
- [x] Dar destaque visual para a empresa principal consultada.
- [x] Agrupar filiais por CNPJ raiz com ordenacao previsivel.
- [x] Exibir contador da empresa como grupo proprio e distinguivel dos socios.
- [x] Sinalizar claramente filiais sem telefone ou email.
- [x] Sinalizar claramente contador sem telefone ou email.
- [x] Sinalizar quando o telefone do contador vier de NFe ou NFCe, distinguindo cadastro formal de telefone observado.
- [x] Preservar a acao de sincronizacao por secao no mesmo fluxo ja existente.
- [x] Evitar criar visualizacoes que induzam a novas persistencias redundantes de datasets ja existentes.

### Fase 6: Testes integrados

Objetivo: validar corretude funcional, performance e estabilidade.

#### Backend

- [x] Criar teste do SQL ou do fluxo da secao `contato` com mocks controlados.
- [x] Validar que a empresa consultada sai como `EMPRESA_PRINCIPAL`.
- [x] Validar que matriz e filiais nao se confundem.
- [x] Validar que todas as filiais da mesma raiz aparecem, inclusive sem contato preenchido.
- [x] Validar que o contador aparece como `CONTADOR_EMPRESA` quando houver `GR_IDENT_CONTADOR`.
- [x] Validar fallback do contador para `SITAFE_RASCUNHO_FAC` ou `SITAFE_REQ_INSCRICAO` quando necessario.
- [x] Validar que telefone vindo de NFe/NFCe so entra quando houver reconciliacao por CPF/CNPJ do contador.
- [x] Validar que socio contribuinte aparece corretamente no bloco de socios.
- [x] Validar que falha Oracle gera erro tratavel no backend.
- [x] Validar que datasets ja materializados sao reaproveitados antes de nova consulta Oracle.

#### Frontend

- [x] Validar que a sincronizacao da secao `contato` atualiza a UI sem reload manual.
- [x] Validar mensagens de erro e sucesso por secao.
- [x] Validar renderizacao com volume alto de linhas na secao `contato`.

#### Performance

- [x] Simular empresa com muitas filiais na mesma raiz.
- [x] Simular empresa com contador sem contato completo.
- [x] Simular empresa com varios socios atuais.
- [x] Medir tempo de resposta da consulta e da materializacao do parquet.
- [x] Medir ganho de reuso quando cadastro, NFe/NFCe ou contador ja estiverem materializados.
- [x] Medir impacto da nova camada `cache-first` no fluxo do Dossie.

### Fase 7: Documentacao final

Objetivo: fechar o ciclo sem deixar divergencia documental.

- [x] Atualizar `docs/plano_sql.md` com o status final do motor SQL e da nova secao `contato`.
- [x] Atualizar `docs/plano _contatos.md` com decisoes finais de contrato e implementacao real.
- [x] Atualizar este plano mestre com os itens concluidos.
- [x] Se houver mudanca relevante no contrato de dados, documentar tambem no README ou na documentacao tecnica do Dossie.

## 5. Checklist Integral Consolidado

### Base tecnica do Dossie

- [x] Criar o guia SQL do projeto.
- [x] Fragmentar as consultas principais do legado XML.
- [x] Implementar sincronizacao backend por secao.
- [x] Integrar sincronizacao por secao no frontend.
- [x] Suportar multiplos `sql_ids` por secao.
- [x] Retornar metadados auditaveis da sincronizacao.
- [x] Criar camada global inicial de reuso de datasets Oracle por entidade.
- [x] Conectar datasets compartilhados em Polars antes de reconsultar o Oracle.
- [x] Validar sync real com Oracle.

### Secao contato

- [x] Definir regra final de filiais por raiz.
- [x] Definir regra final de prioridade e fallback para dados do contador.
- [x] Definir regra final para telefones do contador em NFe/NFCe.
- [x] Definir regra final de exibicao de dados de socios.
- [x] Criar `sql/dossie_contato.sql`.
- [x] Integrar `contato` ao catalogo e aos aliases do Dossie.
- [x] Persistir a secao `contato` no cache canonico.
- [x] Reaproveitar datasets existentes de empresa, filial, contador, socio, NFe e NFCe antes de nova extracao.
- [x] Exibir a secao `contato` com agrupamento por vinculo.

### Validacao

- [x] Cobrir as rotas de sync com testes automatizados basicos.
- [x] Cobrir erros do motor de sync com testes automatizados.
- [x] Cobrir o fluxo de `contato` com testes automatizados.
- [x] Executar validacao manual/visual da interface.
- [x] Executar testes de performance com empresas de alta cardinalidade.

Observacao da validacao manual:
- o fluxo real do `contato` foi conferido no CNPJ `37671507000187`, incluindo mensagem de sucesso apos sync real e mensagem de erro apos indisponibilidade temporaria da API, sem quebra da leitura detalhada ja materializada.
- a medicao operacional de performance foi executada sobre `37671507000187` e `84654326000394`, com relatorios em `output/performance_dossie_contato/` e `reuso_total` do cache-first em ambos os modos testados.

Observacao de fechamento:
- os itens ainda nao marcados neste plano deixaram de representar lacuna tecnica de implementacao e passaram a ser, neste momento, pendencias de governanca funcional, formalizacao de contrato ou decisao de negocio ainda nao congelada documentalmente.

Pendencias residuais desta etapa:
- decidir o alcance da proxima iteracao da arquitetura global de reuso entre Dossie, estoque, ressarcimento e demais modulos;
- manter a documentacao alinhada caso decisoes futuras alterem contrato, ordenacao funcional ou criterios de exibicao ja congelados.

### Documentacao

- [x] Manter planos SQL e Contatos documentados.
- [x] Criar plano mestre unificado.
- [x] Atualizar a documentacao apos a implementacao real da secao `contato`.

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
