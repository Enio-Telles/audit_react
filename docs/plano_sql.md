# Plano de Implementacao: Analise, Extracao e Evolucao do Dossie

Este documento consolida o plano do Dossie SQL e o status real da implementacao verificado no repositorio em 2026-04-08 apos a integracao do fluxo de sincronizacao entre backend e frontend.

Objetivo: transformar o Dossie de um leitor passivo de artefatos em cache para um fluxo capaz de resolver secoes, executar SQL no Oracle, persistir resultados em Parquet e expor esse ciclo para a API e para a interface web.

Plano mestre relacionado: `docs/plano_dossie_integral.md`

---

## 1. Status Verificado

| Item do plano | Status | Evidencias no codigo | Observacoes |
| --- | --- | --- | --- |
| Criar `Agent_SQL.md` | Concluido | `Agent_SQL.md` existe com regras de binds, isolamento de SQL e orientacoes para decomposicao do legado XML | O guia foi criado e cobre o objetivo principal do item. |
| Fragmentar consultas do `dossie_nif.xml` em `.sql` | Concluido | Existem `sql/dossie_enderecos.sql`, `sql/dossie_historico_situacao.sql`, `sql/dossie_regime_pagamento.sql`, `sql/dossie_atividades.sql`, `sql/dossie_contador.sql`, `sql/dossie_historico_fac.sql`, `sql/dossie_vistorias.sql` e `sql/dossie_historico_socios.sql` | A secao `cadastro` segue reutilizando `dados_cadastrais.sql`, o que preserva compatibilidade com o catalogo atual. |
| Criar motor de extracao em `dossie_extraction_service.py` | Parcial | `src/interface_grafica/services/dossie_extraction_service.py` resolve a secao, le o SQL, executa a consulta e salva o cache em Parquet | O servico hoje executa apenas o primeiro `sql_id` resolvido da secao. Ainda nao compoe multiplas consultas nem explicita versionamento de execucao no retorno. |
| Expor rota de sincronizacao no FastAPI | Concluido | `backend/routers/dossie.py` possui `POST /{cnpj}/secoes/{secao_id}/sync` e tambem o alias `POST /{cnpj}/sync/{secao_id}` | O backend agora atende o contrato atual e preserva compatibilidade com o contrato originalmente descrito no plano. |
| Atualizar `dossie_catalog.py` e `dossie_aliases.py` | Concluido | As secoes `enderecos`, `historico_situacao`, `regime_pagamento`, `atividades`, `contador`, `historico_fac`, `vistorias` e `socios` foram adicionadas e mapeadas | O catalogo e os aliases ja refletem a abertura das subsecoes originadas do XML. |
| Integrar sincronizacao no `frontend/src/api/client.ts` | Concluido | `frontend/src/api/client.ts` agora expone `dossieApi.syncSecao(cnpj, secaoId, parametros?)` | O client React passou a consumir o endpoint de sincronizacao do Dossie. |
| Integrar botoes e feedback de sync no `DossieTab.tsx` | Concluido | `frontend/src/features/dossie/components/DossieTab.tsx` agora usa `useMutation`, invalida cache e exibe feedback por secao | O fluxo de sincronizacao ja pode ser disparado pela interface do Dossie. |
| Executar verificacoes do plano | Parcial | Foram adicionados testes do roteador para os dois contratos de sync, `tsc --noEmit` passou e o lint terminou sem erros | Ainda falta validar o comportamento completo com consulta Oracle real e, idealmente, um teste visual ou manual da interface. |

---

## 2. Estado Atual Consolidado

### Backend

- O resumo das secoes continua conservador no `GET /api/dossie/{cnpj}/secoes`, lendo apenas artefatos ja materializados.
- Ja existe um caminho ativo de sincronizacao via `POST /api/dossie/{cnpj}/secoes/{secao_id}/sync`.
- O contrato legado `POST /api/dossie/{cnpj}/sync/{secao_id}` tambem foi mantido para aderencia ao plano original e evitar quebra de integracoes.
- O cache canonico do Dossie usa chave estavel derivada de CNPJ, secao, parametros normalizados e versao de consulta.

### Servicos

- `dossie_resolution.py` ja resolve secao, aliases SQL e nome de arquivo de cache.
- `dossie_extraction_service.py` ja salva o resultado em `CNPJ/<cnpj>/arquivos_parquet/dossie/`.
- `SqlService.build_binds()` ja filtra apenas bind variables presentes no SQL, reduzindo erro de execucao por parametro excedente.
- O fluxo atual do Dossie ainda executa Oracle diretamente por secao, sem uma camada global de reuso de datasets extraidos por entidade.

### Frontend

- O Dossie passou a operar como painel ativo para sincronizacao por secao.
- Os status visuais (`cached`, `loading`, `fresh`, `error`) agora sao usados tambem no fluxo de execucao e retorno da sincronizacao.

### Testes

- Existem testes para resumo de secoes e para chaves de cache.
- Existem testes cobrindo os dois contratos de rota POST de sincronizacao.
- Ainda nao ha teste ponta a ponta cobrindo execucao Oracle real, persistencia fisica e reflexo visual no frontend.

### Reuso e performance

- O projeto ja reaproveita bem os Parquets derivados no fluxo de estoque:
  `aba_mensal` e `aba_anual` leem `mov_estoque` em vez de reconstruir o fluxo do zero.
- O principal gap atual esta nas extracoes Oracle pontuais:
  o Dossie ainda nao verifica um cache compartilhado por entidade antes de disparar uma nova consulta.
- `SqlService.executar_sql()` ainda funciona como executor direto, sem uma camada de `cache-first` por dataset canonico.

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
- [ ] Ampliar os testes do POST de sync para cobrir erro de secao desconhecida e falha de persistencia.

### Prioridade Tecnica

- [ ] Evoluir `dossie_extraction_service.py` para suportar multiplos `sql_ids` por secao quando houver composicao real de fontes.
- [ ] Definir no retorno da sincronizacao informacoes mais auditaveis, como `sql_id` executado, `cache_key` e versao da consulta.
- [ ] Avaliar se secoes `mixed` devem continuar resumindo artefatos legados e canonicos no mesmo fluxo ou se o modelo precisa ser unificado.
- [ ] Criar uma camada compartilhada de datasets Oracle canonicos para evitar reextracao do mesmo CNPJ/CPF em Dossie, estoque e outras analises.
- [ ] Separar claramente extracao de dados do Oracle e composicao analitica em Polars.
- [ ] Fazer `cache-first` por dataset/entity key antes de qualquer nova consulta Oracle.
- [ ] Padronizar o reuso de Parquet para que consultas ja extraidas sejam conectadas por `scan_parquet()` e joins em Polars, nao por reconsulta ao Oracle.

## 4.1 TODO de Otimizacao de Extracoes e Reuso

Objetivo:
garantir que uma informacao extraida uma vez para um CNPJ, CPF ou chave de negocio nao seja extraida novamente sem necessidade, sendo reaproveitada por Dossie, estoque, ressarcimento e demais fluxos.

### Diretrizes

- Extrair uma vez, reutilizar muitas.
- Compor no Polars, nao no Oracle, sempre que a informacao ja existir materializada.
- Salvar datasets canonicos por entidade e assunto, nao por tela ou por caso de uso.

### TODO proposto

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
- [ ] Implementar leitura por `pl.scan_parquet()` para composicao lazy dos datasets compartilhados.
- [ ] Evitar salvar novos Parquets redundantes quando o resultado puder ser representado como view logica ou composicao de datasets existentes.

### Casos prioritarios de reuso

- [ ] Reaproveitar `dados_cadastrais_<cnpj>.parquet` no Dossie antes de nova extracao cadastral.
- [ ] Reaproveitar `nfe_agr_<cnpj>.parquet`, `nfce_agr_<cnpj>.parquet` e bases correlatas em secoes do Dossie e em contatos.
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
