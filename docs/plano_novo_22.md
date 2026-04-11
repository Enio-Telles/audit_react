# Plano Novo 22

## Objetivo

Definir a reorganização completa da arquitetura de extração, processamento e visualização analítica do projeto fiscal, com foco em:

- reduzir consultas desnecessárias ao Oracle;
- transformar SQLs pesadas em extrações atômicas e reutilizáveis;
- deslocar joins, agregações, deduplicações, enriquecimentos e indicadores para Polars/Parquet;
- separar claramente EFD, BI/XML, SITAFE e visões analíticas derivadas;
- unificar backend, pipeline e frontend em torno de datasets canônicos, rastreáveis e reaproveitáveis.

## Diretriz consolidada desta versão

A estrutura PySide deixou de ser a referência arquitetural do projeto.

Daqui em diante:

- backend FastAPI + frontend React são a superfície principal do sistema;
- serviços reutilizáveis devem viver em `src/utilitarios/`, `src/transformacao/` e `backend/services/`;
- `src/interface_grafica/` passa a ser tratado como legado em descontinuação controlada;
- novos fluxos, novos contratos e novas features não devem nascer acoplados ao PySide;
- toda remoção do legado deve ocorrer por fases, preservando corretude fiscal e estabilidade operacional.

## Verificação de migração para Tauri

### Situação encontrada no repositório

- já existe uma shell Tauri v2 em `frontend/src-tauri/`;
- o `frontend/package.json` já possui dependências e scripts de Tauri;
- o frontend React já está suficientemente separado da UI PySide para virar a superfície principal desktop;
- o backend FastAPI segue sendo necessário para servir `/api`, processamento e leitura dos datasets locais.

### Conclusão arquitetural

A migração para Tauri é viável e coerente com a direção do projeto, mas não é uma troca "frontend por desktop shell" apenas.

O ponto central é:

- Tauri pode substituir a shell PySide;
- FastAPI continua existindo como backend local do app;
- para empacotamento desktop real, o backend precisa rodar como processo local controlado pelo app ou como sidecar empacotado.

### Ajustes já implementados para essa trilha

- `frontend/src/api/client.ts` passou a resolver a `API_BASE_URL` com fallback adequado para shell desktop empacotada;
- `frontend/src-tauri/tauri.conf.json` foi ajustado para usar `app_react.py --no-browser` no fluxo de desenvolvimento da shell Tauri, subindo frontend e backend reais;
- a trilha atual favorece `React + Tauri` como substituto progressivo do PySide.

### Restrições observadas nesta máquina

- `node_modules` do frontend não estão instalados no ambiente atual;
- `cargo` e `rustc` não estão disponíveis nesta máquina neste momento;
- por isso, a shell Tauri não pôde ser executada nem compilada localmente nesta rodada.

### Direção recomendada

#### Fase de desenvolvimento

- usar Tauri como shell do frontend React;
- manter FastAPI local em `localhost`;
- usar `app_react.py --no-browser` como orquestrador de desenvolvimento.

#### Fase de empacotamento

- empacotar o backend Python como executável local;
- registrar esse backend como sidecar do app desktop;
- fazer o frontend Tauri consumir `http://127.0.0.1:<porta>/api` quando estiver fora de `http/https`.

#### Fase de desligamento do PySide

- migrar os últimos fluxos exclusivos do desktop legado;
- remover dependências de `PySide6` do entrypoint principal;
- manter a shell Tauri como desktop oficial.

## Verificação de observabilidade e versionamento de schemas

### Situação encontrada no repositório

- já existe uma rota de observabilidade em `backend/routers/observabilidade.py`;
- já existe catálogo de datasets e inspeção de materialização;
- já existe integração com OpenLineage em `src/observabilidade/openlineage.py`;
- já existe `SchemaRegistry` em `src/utilitarios/schema_registry.py`;
- o salvamento em Parquet e Delta já registra snapshots de schema automaticamente.

### Lacunas identificadas

- a observabilidade já existia, mas a exposição do versionamento de schema ainda era discreta;
- o catálogo de datasets ainda não mostrava de forma clara a versão de schema do artefato inspecionado;
- o plano ainda não tratava explicitamente a observabilidade operacional e a governança de schema como trilhas visíveis da arquitetura-alvo.

### Ajustes já implementados nesta rodada

- `backend/routers/observabilidade.py` passou a expor o resumo do `SchemaRegistry` no endpoint de status;
- foi criado o endpoint `/api/observabilidade/schema-registry`;
- `backend/routers/fiscal_catalog_inspector.py` passou a devolver metadados de schema por dataset inspecionado:
  - versão mais recente
  - hash do schema
  - campos registrados
  - schema atual detectado
  - diff entre schema atual e último snapshot
- os testes foram atualizados para cobrir:
  - resumo do registry
  - busca por `source_path`
  - exposição de schema no inspector

### Conclusão arquitetural

Observabilidade e versionamento de schemas já não são apenas intenção do plano; agora fazem parte da execução real do backend.

O próximo avanço é:

- ampliar manifests por dataset;
- expor lineage e schema na UI fiscal e no catálogo;
- transformar mudança de schema em evento operacional explícito do pipeline.

Este plano usa como base:

- o acervo de SQLs Oracle em `C:\Users\eniot\OneDrive - SECRETARIA DE ESTADO DE FINANCAS\GEFIS_ENIO\consultas_oracle_sql_developer\consultas_sql`;
- o mapeamento de tabelas em `C:\Users\eniot\OneDrive - SECRETARIA DE ESTADO DE FINANCAS\Desenvolvimento\mapeamento`;
- o estado real do repositório `c:\audit_react`;
- a direção já indicada no pacote "SQL mínimo denominador comum".

## Evolução já implementada nesta rodada

### Backend

- foi criada uma base compartilhada para exploração dos datasets analíticos atuais em `backend/routers/fiscal_analysis_support.py`;
- foram criados routers canônicos iniciais para:
  - `produto`
  - `conversao`
  - `estoque`
- o `backend/main.py` passou a expor:
  - `/api/fiscal/produto/*`
  - `/api/fiscal/conversao/*`
  - `/api/fiscal/estoque/*`
- a API passou a usar serviços desacoplados para:
  - registry
  - paginação e filtros de Parquet
  - leitura e execução de SQL
  - pipeline de extração e geração
  - agregação via módulo canônico compartilhado
  - inspeção de schema e observabilidade operacional
- parte dos imports críticos do backend e do pipeline foi movida de `interface_grafica.*` para `utilitarios.*` e `backend/services/*`
- foram criadas bridges temporárias em `backend/services/` para:
  - `dossie`
- os routers principais deixaram de importar `interface_grafica.*` diretamente
- `pipeline` já foi promovido para serviço real em `backend/services/pipeline_runtime.py`
- `aggregation` passou a apontar para o módulo canônico `src/utilitarios/servico_agregacao.py`
- os módulos de `dossie` foram promovidos para `src/utilitarios/dossie_*`, enquanto `src/interface_grafica/services/dossie_*` ficou restrito a wrappers de compatibilidade
- o router `backend/routers/dossie.py` passou a consumir diretamente `services.dossie_service.executar_sync_secao`, removendo o import dinâmico do caminho legado
- a observabilidade passou a expor resumo do `SchemaRegistry` e inspeção de schema por dataset

### Frontend

- foi criado um componente compartilhado de exploração de datasets:
  - `frontend/src/features/fiscal/shared/FiscalDatasetExplorer.tsx`
- foram criadas features canônicas iniciais para:
  - `Produto Master`
  - `Conversão`
  - `Estoque`
- o `App.tsx` passou a incluir abas próprias para esses domínios, sem remover as abas legadas;
- a documentação do módulo fiscal foi atualizada para refletir a nova organização.

### Situação após esta rodada

- o projeto já tem separação visível entre:
  - `EFD`
  - `Produto Master`
  - `Conversão`
  - `Estoque`
  - `Documentos Fiscais`
  - `Fiscalização`
  - `Catálogo`
- `Agregação`, `Conversão` e `Estoque` legados permanecem ativos como compatibilidade temporária;
- a aba `Análise Fiscal` continua existindo como visão consolidada de transição.
- o backend principal já não depende mais da antiga configuração PySide para `CNPJ_ROOT`;
- o movimento oficial passa a ser: extrair dependências reutilizáveis do legado, manter compatibilidade temporária e desligar a UI PySide por domínio.
- o acoplamento restante com o legado ficou concentrado em bridges temporárias e na própria árvore `src/interface_grafica/`, o que reduz o risco da próxima etapa de remoção.
- `pipeline` deixou de ser uma dessas bridges e passou a integrar a trilha principal do backend.
- `aggregation` também deixou de depender da implementação antiga como fonte primária; a compatibilidade da UI legada agora ocorre por reexport.

## Diagnóstico da abordagem atual

### Problemas principais

1. Há mistura de papéis entre SQL de extração e SQL analítica.
   Consultas como `c176_verificacoes_v5.sql`, `fronteira_completo.sql` e parte de `reg_0200.sql` já embutem enriquecimento, score, consolidação, agregações e regras de negócio que deveriam ocorrer fora do Oracle.

2. O Oracle ainda está sendo usado para trabalho que deve ser feito no lake.
   Isso aumenta custo de CPU, I/O, memória temporária, risco de spool e dificuldade de reuso.

3. O projeto já possui duas arquiteturas convivendo.
   Existe uma trilha legada baseada em artefatos como `c170_xml`, `c176_xml`, `mov_estoque`, `produtos_final` e `fatores_conversao`, mas já há também um embrião correto de atomização em `sql/arquivos_parquet/atomizadas/` e em `src/transformacao/atomizacao_pkg/`.

4. O domínio EFD ainda está subrepresentado no frontend.
   A aba nova de EFD mostra hoje só `C170` e `Bloco H`, enquanto o mapeamento e o acervo suportam expansão para `0000`, `0190`, `0200`, `0220`, `C100`, `C190`, `C176`, `C197`, `H005`, `H010`, `H020`, `K200` e demais registros necessários.

5. Os domínios ainda não estão formalmente separados.
   EFD, XML/BI, SITAFE, estoque, conversão, agregação e ressarcimento se cruzam de forma útil, mas ainda sem contratos suficientemente explícitos entre camadas.

6. As abas legadas de `conversao`, `agregacao` e `estoque` ainda dependem de artefatos finais, e não de uma malha de datasets canônicos por camada.

### Oportunidades já existentes

1. O acervo de mapeamento contém cobertura suficiente para formalizar contratos de schema.
2. O projeto já tem catálogo de datasets e localizador centralizado.
3. O pipeline atomizado de EFD já prova a direção correta: SQL mínima, tipagem e enriquecimento em Polars.
4. O frontend já iniciou a migração para módulos fiscais separados.

## Princípios arquiteturais

1. Oracle deve entregar dados base, granulares e auditáveis.
2. Polars deve concentrar tipagem, joins, deduplicação, enriquecimento, score e agregação.
3. Todo dataset derivado deve reaproveitar Parquet upstream antes de considerar nova consulta Oracle.
4. EFD deve ser um domínio estrito de SPED, sem misturar XML, BI ou SITAFE na camada base.
5. Regras fiscais e rastreabilidade prevalecem sobre conveniência de implementação.
6. Toda transformação deve ser explicável por contrato de entrada, chave de junção e regra aplicada.
7. Ajustes manuais não alteram `raw`; eles entram em uma camada de `edits` com precedência controlada.
8. O frontend deve consumir datasets canônicos, não arquivos ad hoc.
9. Todo payload de API deve carregar metadados de origem e rastreabilidade quando fizer sentido.
10. SQL de referência ou homologação pode existir, mas não deve ser confundida com SQL canônica de extração.

## Classificação dos SQLs

### 1. SQLs de extração base

São as consultas que devem permanecer no Oracle, com o menor grau possível de lógica derivada:

- `reg_0000`
- `reg_0190`
- `reg_0200`
- `reg_0220`
- `c100`
- `c170`
- `c176`
- `c190`
- `c197`
- `h005`
- `h010`
- `h020`
- `k200`
- bases BI/XML por documento e item
- bases SITAFE mínimas por documento, item, mercadoria e cálculo

### 2. SQLs intermediárias aceitas

São consultas que ainda podem existir, desde que o objetivo seja apenas:

- identificar última EFD por período;
- isolar retificadoras;
- reduzir o universo de leitura por `CNPJ`, período e arquivo válido;
- preservar chaves técnicas do domínio.

Exemplo aceitável:

- `01_reg0000_historico.sql`
- `02_reg0000_versionado.sql`
- `03_reg0000_ultimo_periodo.sql`

### 3. SQLs analíticas ou de mart

Essas não devem ser base primária de extração:

- `c176_verificacoes_v5.sql`
- `fronteira_completo.sql`
- `Ressarcimento_ST_calculo.sql`
- versões consolidadas de fronteira, score, batimento, conciliação e resumo mensal
- consultas que tragam `CASE` descritivo, `GROUP BY`, ranking de candidatos, score, match ou reconciliação documental completa

Esses SQLs devem ser reescritos como pipelines em Polars ou mantidos apenas como referência de homologação.

## Estratégia de extração Oracle

### Regra geral

Cada consulta Oracle deve representar uma entidade de origem ou um bloco mínimo reutilizável.

### O que a extração deve fazer

- filtrar por `CNPJ` o mais cedo possível;
- limitar por janela temporal explícita;
- preservar chaves técnicas e naturais;
- trazer apenas colunas necessárias;
- trazer metadados de período, entrega e versão do arquivo;
- evitar descrições derivadas e decodificações textuais;
- evitar agregação e união desnecessária;
- evitar `SELECT *`.

### O que a extração não deve fazer

- `CASE` descritivo para UX;
- classificação final de produto;
- score de aderência;
- ranking de candidatos;
- deduplicação por heurística de negócio;
- join com múltiplos domínios para produzir visão final;
- agrupamentos mensais/anuais;
- consolidação de ressarcimento;
- montagem de tabela operacional final.

### Padrão mínimo para cada SQL

Cada SQL canônica deve declarar:

- parâmetros esperados;
- granularidade da linha;
- chaves primárias ou técnicas;
- tabelas Oracle envolvidas;
- colunas fiscais obrigatórias;
- colunas de rastreabilidade;
- critério de versão/retificação, quando aplicável.

## Estratégia de processamento em Polars/Parquet

### Ordem de processamento

1. Ler Parquet com `scan_parquet`.
2. Filtrar por `cnpj`, período e partições.
3. Tipar colunas e normalizar chaves.
4. Aplicar versionamento e deduplicação técnica.
5. Fazer joins entre datasets base.
6. Aplicar regras fiscais derivadas.
7. Gerar datasets curated.
8. Gerar marts analíticos.
9. Materializar apenas ao final.

### Regras técnicas

- preferir `LazyFrame`;
- centralizar tipagem em funções pequenas por domínio;
- evitar `collect()` precoce;
- evitar `to_dicts()` fora da borda da API/UI;
- manter `schema_version`, `row_count`, `query_hash`, `extracted_at`, `dataset_id` e `upstream_datasets`;
- usar Parquet como padrão e Delta apenas onde houver merge incremental ou edição controlada.

## Camadas de dados

## `raw`

Extração quase literal do Oracle.

Características:

- o mais próximo possível da origem;
- sem regra de negócio final;
- com chaves técnicas preservadas;
- com nomes próximos aos campos originais, podendo aplicar apenas normalização mínima de casing e tipos.

Exemplos:

- `raw__efd__reg_0000`
- `raw__efd__reg_c170`
- `raw__bi_xml__nfe_item`
- `raw__sitafe__nfe_calculo_item`

## `base`

Tipagem, versionamento, deduplicação técnica e uniformização.

Características:

- arquivo válido por período;
- datas e números tipados;
- colunas de chave padronizadas;
- alias canônicos de colunas críticas;
- sem score ou indicador final.

Exemplos:

- `base__efd__reg_0000_ultimo_periodo`
- `base__efd__reg_c100`
- `base__efd__reg_c170`
- `base__efd__reg_c176`
- `base__produto__cadastro_0200`

## `curated`

Composição por domínio.

Características:

- joins entre bases do mesmo domínio ou entre domínios explicitamente autorizados;
- enriquecimento controlado;
- preparação para consumo analítico;
- ainda sem concentrar toda a lógica final de indicadores.

Exemplos:

- `cur__efd__documentos_itens`
- `cur__produto__master_base`
- `cur__conversao__candidatos`
- `cur__estoque__eventos`
- `cur__ressarcimento__c176_anchor`

## `marts`

Datasets de negócio e análise.

Características:

- score;
- match;
- conciliação;
- agregações mensais/anuais;
- divergências;
- indicadores;
- classificação final de produto.

Exemplos:

- `mart__produto__grupos`
- `mart__conversao__aplicada`
- `mart__estoque__saldo_mensal`
- `mart__estoque__saldo_anual`
- `mart__ressarcimento__batimento_item`
- `mart__ressarcimento__resumo_mensal`

## `views`

Camada de entrega para API e frontend.

Características:

- ordenação padrão;
- paginação;
- colunas amigáveis;
- suporte a filtro;
- metadados de rastreabilidade;
- nunca como fonte primária de processamento.

## Estratégia específica para EFD

### Escopo mínimo obrigatório

O domínio EFD deve contemplar, no mínimo:

- `reg_0000`
- `reg_0190`
- `reg_0200`
- `reg_0220`
- `c100`
- `c170`
- `c190`
- `c176`
- `c197`
- `h005`
- `h010`
- `h020`
- `k200`

### Regras do domínio EFD

1. A camada EFD deve mostrar estritamente dados da EFD.
2. Retificação e última entrega por período devem ser resolvidas no começo da cadeia.
3. `0200`, `0205` e `0220` devem existir como cadastro e histórico de produto da EFD.
4. `C100` é cabeçalho documental.
5. `C170` é item documental.
6. `C190` é analítico do documento, mas ainda pertence ao domínio EFD.
7. `C176` e `C197` permanecem na EFD, mesmo quando forem usados depois por ressarcimento.
8. `H005`, `H010`, `H020` e `K200` são EFD e devem ser visíveis no módulo EFD sem depender de estoques externos.

### Datasets propostos

- `raw__efd__reg_0000`
- `raw__efd__reg_0190`
- `raw__efd__reg_0200`
- `raw__efd__reg_0205`
- `raw__efd__reg_0220`
- `raw__efd__reg_c100`
- `raw__efd__reg_c170`
- `raw__efd__reg_c190`
- `raw__efd__reg_c176`
- `raw__efd__reg_c197`
- `raw__efd__reg_h005`
- `raw__efd__reg_h010`
- `raw__efd__reg_h020`
- `raw__efd__reg_k200`

- `base__efd__arquivos_validos`
- `base__efd__reg_0200_tipado`
- `base__efd__reg_c100_tipado`
- `base__efd__reg_c170_tipado`
- `base__efd__reg_c176_tipado`
- `base__efd__bloco_h_tipado`

- `cur__efd__documentos_itens`
- `cur__efd__inventario`
- `cur__efd__ajustes_documento`
- `cur__efd__comparativo_entregas`

### Saídas de frontend do módulo EFD

- visão por bloco;
- visão por registro;
- visão árvore `0000 -> C100 -> C170 -> C176`;
- inventário `H005/H010/H020`;
- visão de `K200`;
- dicionário de campos;
- comparação entre períodos e retificadoras;
- exportação do recorte filtrado;
- rastreabilidade até `dataset_id`, `sql_id` e `reg_*_id`.

## Estratégia específica para conversão

### Fontes prioritárias

1. `EFD 0220`
2. `EFD 0200`
3. `C170` para observar unidade praticada
4. bases auxiliares BI de fatores de conversão, quando necessário
5. ajustes manuais

### Objetivo

Separar:

- o que é fator declarado na EFD;
- o que é fator inferido por comportamento;
- o que é ajuste manual;
- o que é fator homologado para cálculo.

### Datasets propostos

- `raw__efd__reg_0220`
- `base__conversao__0220_tipado`
- `cur__conversao__candidatos`
- `cur__conversao__evidencias_uso_unidade`
- `mart__conversao__aplicada`
- `view__api__conversao__tabela_operacional`

### Regras

- nunca perder a distinção entre `unid_inv`, `unid_conv`, `unidade observada` e `unid_ref`;
- ajustes manuais devem ser sobreposição explícita;
- o fator final precisa indicar origem: `efd`, `inferido`, `manual`, `auxiliar_bi`.

## Estratégia específica para agregação

### Objetivo

Criar um produto master por contribuinte, sem misturar isso com a extração base.

### Fontes prioritárias

- `0200`
- `0205`
- `0220`
- `C170`
- GTIN
- NCM
- CEST
- descrições normalizadas

### Datasets propostos

- `cur__produto__cadastro_efd`
- `cur__produto__descricoes_praticadas`
- `cur__produto__candidatos_agregacao`
- `mart__produto__grupos`
- `mart__produto__conflitos`
- `view__api__produto__workbench_agregacao`

### Regras

- agregação não altera `raw` nem `base`;
- agrupamentos devem ser explicáveis por evidências;
- o grupo precisa guardar score, justificativa e histórico de merge;
- `GTIN`, `CEST`, `NCM`, unidade e descrição devem ser preservados separadamente;
- divergência entre identidade estrutural e prática documental deve virar sinal analítico, não ser apagada.

## Estratégia específica para estoque

### Objetivo

Construir visão de eventos, snapshots e saldos com base em EFD, usando XML/BI/SITAFE apenas como enriquecimento controlado.

### Fontes prioritárias

- `C170` como evento de item escriturado;
- `C100` para contexto documental;
- `H005/H010/H020` como inventário;
- `K200` como estoque declarado;
- `C176` como vínculo fiscal para ressarcimento;
- XML/BI para detalhe complementar;
- SITAFE para fronteira e cálculo quando necessário.

### Datasets propostos

- `cur__estoque__eventos`
- `cur__estoque__snapshots_inventario`
- `cur__estoque__k200`
- `mart__estoque__saldo_mensal`
- `mart__estoque__saldo_anual`
- `mart__estoque__divergencias`
- `view__api__estoque__workbench`

### Regras

- separar evento fiscal, snapshot e saldo calculado;
- nunca tratar `Bloco H` como se fosse movimentação;
- `K200` deve ser visão declarada para confronto, não substituto do saldo calculado;
- cálculos de saldo devem informar hipótese, corte temporal e fonte de cada ajuste.

## Estratégia específica para ressarcimento ST

### Diretriz central

Consultas como `c176_verificacoes_v5.sql` devem ser quebradas em blocos.

### Pipeline proposto

1. `base__efd__c176`
2. `cur__ressarcimento__saida_anchor`
3. `cur__ressarcimento__entrada_candidatos`
4. `cur__ressarcimento__entrada_score`
5. `cur__ressarcimento__fronteira`
6. `mart__ressarcimento__batimento_item`
7. `mart__ressarcimento__resumo_mensal`
8. `mart__ressarcimento__conciliacao`

### Regras

- `C176` continua pertencendo ao domínio EFD na origem;
- XML e SITAFE entram apenas na camada curated;
- score de aderência fica fora do Oracle;
- resumo mensal e conciliação ficam fora do Oracle;
- SQL final consolidada pode ser mantida apenas como referência de homologação.

## Regras para reaproveitamento e não repetição de consultas

1. Antes de criar nova SQL, verificar se a entidade já existe no catálogo.
2. Se o dado já existe em Parquet canônico, a transformação deve ler Parquet.
3. Cada `dataset_id` deve declarar `upstream_datasets`.
4. O backend não deve consultar Oracle diretamente para responder telas analíticas já cobertas por datasets materializados.
5. SQL analítica não pode virar dependência de extração base.
6. Toda nova demanda deve mapear:
   - qual dataset base já existe;
   - qual dataset curated falta;
   - qual mart realmente precisa ser criado.
7. Reprocessamento deve ser incremental por `cnpj + período + dataset`.
8. A mesma lógica fiscal não deve ser reimplementada em múltiplos routers ou componentes.

## Estrutura desejada do projeto

### Visão macro

O projeto desejado deve separar com clareza:

- extração Oracle;
- contratos de schema;
- processamento por camada;
- catálogo e manifests;
- API por domínio;
- frontend por feature analítica;
- armazenamento por camada e por CNPJ.

```text
docs/
  plano_novo_22.md

sql/
  shared/
    efd/
    common/
  raw/
    efd/
    bi_xml/
    sitafe/
    fiscalizacao/
  referencia/
    ressarcimento_st/
    homologacao/

src/
  extracao/
    oracle/
      efd/
      bi_xml/
      sitafe/
      fiscalizacao/
  contratos/
    efd/
    bi_xml/
    sitafe/
    produto/
    estoque/
    ressarcimento/
  datasets/
    registry.py
    manifests.py
    lineage.py
    schemas.py
  processamento/
    raw_to_base/
      efd/
      bi_xml/
      sitafe/
    curated/
      efd/
      produto/
      conversao/
      estoque/
      ressarcimento/
      fiscalizacao/
    marts/
      produto/
      estoque/
      ressarcimento/
      fiscalizacao/
  servicos/
    materializacao/
    reprocessamento/
    validacao/
  api/
    routers/
      fiscal_efd.py
      fiscal_produto.py
      fiscal_estoque.py
      fiscal_conversao.py
      fiscal_ressarcimento.py
      fiscal_catalogo.py
      fiscal_documentos.py
    shared/
      filtros.py
      paginacao.py
      rastreabilidade.py

frontend/src/features/fiscal/
  efd/
  produto_master/
  estoque/
  conversao/
  ressarcimento/
  documentos/
  fiscalizacao/
  catalogo/
  shared/
    api/
    components/
    hooks/
    tables/
    filters/
    provenance/
    compare/

frontend/src/app/
  shell/
  tabs/
  routes/

frontend/src/store/
  fiscalShellStore.ts
  compareStore.ts
  filtersStore.ts

dados/CNPJ/<cnpj>/
  raw/
  base/
  curated/
  marts/
  views/
  edits/
  manifests/
  lineage/
```

### Responsabilidade por área

#### `sql/`

- `shared/efd/`: blocos reutilizáveis de ancoragem, versionamento e arquivo válido;
- `raw/efd/`: SQL mínima por registro EFD;
- `raw/bi_xml/`: SQL mínima de XML e BI por documento e item;
- `raw/sitafe/`: SQL mínima de cálculo, mercadoria e produto;
- `referencia/`: consultas antigas usadas apenas para homologação.

#### `src/extracao/oracle/`

- carregar SQL;
- validar parâmetros;
- executar Oracle;
- registrar metadados de extração;
- materializar `raw`.

#### `src/contratos/`

- schemas por dataset;
- chaves técnicas;
- colunas obrigatórias;
- versões de schema;
- regras de validação estrutural.

#### `src/processamento/raw_to_base/`

- tipagem;
- versionamento;
- deduplicação técnica;
- normalização de colunas e chaves.

#### `src/processamento/curated/`

- joins entre datasets base;
- enriquecimento controlado;
- datasets prontos para consumo analítico.

#### `src/processamento/marts/`

- indicadores;
- saldos;
- score;
- classificação;
- conciliações;
- resumos de negócio.

#### `src/datasets/`

- catálogo canônico;
- resolução por alias;
- localização física;
- manifests;
- lineage;
- inspeção de schema.

#### `src/api/routers/`

- borda HTTP por domínio;
- filtros;
- paginação;
- ordenação;
- respostas com rastreabilidade.

#### `frontend/src/features/fiscal/`

- UI organizada por domínio analítico;
- cada módulo com sua API, tabelas, filtros, comparações e detalhes;
- sem dependência de arquivos legados pelo nome físico.

#### `dados/CNPJ/<cnpj>/`

- persistência por camada;
- recorte por contribuinte;
- trilha de edição e reprocessamento;
- manifests para inspeção e reaproveitamento.

### Fluxo alvo entre camadas

```text
Oracle SQL minima
  -> raw parquet
  -> base parquet
  -> curated parquet
  -> marts parquet
  -> views/api
  -> frontend analitico
```

### Estrutura desejada do frontend fiscal

```text
frontend/src/features/fiscal/
  efd/
    api.ts
    EfdPage.tsx
    components/
      EfdTree.tsx
      EfdDictionaryPanel.tsx
      EfdDatasetSwitcher.tsx
      EfdComparisonPanel.tsx
  produto_master/
    api.ts
    ProdutoMasterPage.tsx
    components/
      MergeWorkbench.tsx
      CandidateComparison.tsx
      ProductEvidencePanel.tsx
  conversao/
    api.ts
    ConversaoPage.tsx
    components/
      ConversaoTable.tsx
      FatorEditPanel.tsx
      ConversionImpactPanel.tsx
  estoque/
    api.ts
    EstoquePage.tsx
    components/
      EstoqueEventTable.tsx
      InventarioPanel.tsx
      SaldoTimeline.tsx
      DivergenciaPanel.tsx
  ressarcimento/
    api.ts
    RessarcimentoPage.tsx
    components/
      SaidaAnchorTable.tsx
      EntradaScorePanel.tsx
      BatimentoTable.tsx
      ResumoMensalPanel.tsx
  documentos/
  fiscalizacao/
  catalogo/
  shared/
    components/
      FiscalPageShell.tsx
      FiscalDataTable.tsx
      FiscalRowDetailPanel.tsx
      ProvenanceBadge.tsx
      ComparisonDrawer.tsx
      ExportMenu.tsx
    filters/
      AdvancedFilterBar.tsx
      SavedFilterMenu.tsx
    hooks/
      useFiscalPageState.ts
      useDatasetPage.ts
      useComparisonSelection.ts
    tables/
      columnPresets.ts
      formatters.ts
      highlightRules.ts
```

### Estrutura desejada do backend fiscal

```text
backend/
  routers/
    fiscal_efd.py
    fiscal_produto.py
    fiscal_conversao.py
    fiscal_estoque.py
    fiscal_ressarcimento.py
    fiscal_documentos.py
    fiscal_fiscalizacao.py
    fiscal_catalogo.py
  services/
    dataset_service.py
    page_query_service.py
    lineage_service.py
    export_service.py
    compare_service.py
  schemas/
    fiscal_common.py
    fiscal_efd.py
    fiscal_produto.py
    fiscal_estoque.py
```

## Convenções de nomes

### Datasets

- `raw__efd__reg_c170`
- `base__efd__reg_c170`
- `cur__produto__candidatos_agregacao`
- `mart__estoque__saldo_mensal`
- `view__api__efd__c170`

### SQL

Padrão sugerido:

- `NN_dominio_entidade_granularidade.sql`

Exemplos:

- `10_efd_reg0000_raw.sql`
- `20_efd_c100_raw.sql`
- `30_efd_c170_raw.sql`
- `40_efd_c176_raw.sql`
- `50_efd_reg0200_raw.sql`
- `60_efd_reg0220_raw.sql`
- `210_estoque_saldo_mensal_mart.sql`

### Campos de rastreabilidade

Todo dataset relevante deve carregar, quando aplicável:

- `dataset_id`
- `camada`
- `cnpj`
- `cnpj_raiz`
- `ano`
- `mes`
- `sql_id_origem`
- `tabela_origem`
- `extraido_em`
- `schema_version`
- `upstream_datasets`
- `query_hash`

## Estratégia de API

### Princípios

- routers devem expor datasets canônicos;
- paginação e ordenação pertencem à borda da API;
- filtros livres devem atuar sobre datasets materializados;
- respostas devem incluir metadados de rastreabilidade para inspeção;
- operação pesada deve ser assíncrona ou fora do event loop.

### Endpoints alvo

- `/api/fiscal/efd/*`
- `/api/fiscal/produto/*`
- `/api/fiscal/conversao/*`
- `/api/fiscal/estoque/*`
- `/api/fiscal/ressarcimento/*`
- `/api/fiscal/catalogo/*`

### Regra de transição

As abas legadas podem continuar existindo temporariamente, mas devem virar apenas atalhos ou wrappers para os datasets canônicos.

## Mudanças desejadas no frontend

### Objetivo da mudança

Sair de um frontend parcialmente acoplado a artefatos legados e chegar a um frontend orientado a domínio, com workbenches analíticos consistentes entre si.

### Mudanças estruturais

1. Separar as telas por feature fiscal real, e não por origem histórica da implementação.
2. Criar uma shell fiscal única com navegação, filtros globais, trilha de contexto e painel de rastreabilidade.
3. Fazer todos os módulos usarem componentes compartilhados de tabela, filtro, detalhe e comparação.
4. Centralizar estado de filtros, comparação e paginação em hooks e stores próprios do domínio fiscal.
5. Unificar contratos de resposta entre routers novos.
6. Refinar a interface gráfica progressivamente com chrome minimalista, priorizando clareza, densidade útil e baixa distração.
7. Concentrar a sofisticação da UX nas grades analíticas, e não no excesso de painéis decorativos.

### Mudanças de navegação

#### Navegação atual

- `EFD`
- `Documentos Fiscais`
- `Fiscalização`
- `Análise Fiscal`
- `Catálogo Datasets`
- `Agregação (legado)`
- `Conversão (legado)`
- `Estoque (legado)`

#### Navegação desejada

- `EFD`
- `Produto Master`
- `Conversão`
- `Estoque`
- `Ressarcimento`
- `Documentos`
- `Fiscalização`
- `Catálogo`
- `Legado` apenas durante a transição

### Mudanças de experiência

#### Para todas as páginas

- interface enxuta, com poucos blocos visuais e ações bem agrupadas;
- tabelas com o máximo de recursos possível, mantendo leitura limpa;
- barra de filtros avançados;
- presets de colunas;
- ordenação persistente;
- ocultação, reordenação e redimensionamento de colunas;
- filtros por coluna e busca textual combináveis;
- painel lateral de detalhe por linha;
- painel de comparação entre registros;
- exportação do recorte filtrado;
- badge de origem do dataset;
- link ou painel com lineage e upstream.

#### Para o módulo EFD

- adicionar navegador por bloco e registro;
- incluir `0000`, `0190`, `0200`, `0220`, `C100`, `C170`, `C190`, `C176`, `C197`, `H005`, `H010`, `H020`, `K200`;
- incluir comparação entre entregas e retificadoras;
- incluir árvore documental;
- incluir dicionário do registro selecionado.

#### Para o módulo Produto Master

- substituir a aba legada de agregação por um workbench de agrupamento;
- exibir evidências por GTIN, NCM, CEST, descrição, unidade e documento;
- permitir merge controlado com preview de impacto;
- separar claramente sugestão automática e decisão manual.

#### Para o módulo Conversão

- substituir a aba legada de conversão por uma página que diferencie:
  - fator EFD;
  - fator inferido;
  - fator auxiliar;
  - fator manual;
- adicionar painel de impacto antes da gravação;
- mostrar histórico de edição e origem do fator final.

#### Para o módulo Estoque

- substituir a aba legada por visão em camadas:
  - eventos;
  - snapshots de inventário;
  - estoque declarado;
  - saldo mensal;
  - saldo anual;
  - divergências;
- habilitar drill-down até documento, item e origem.

#### Para o módulo Ressarcimento

- criar um workbench próprio, em vez de esconder tudo em tabelas derivadas;
- separar saída anchor, entrada candidata, score, fronteira e resumo;
- permitir comparação entre cálculo atual e referência homologada.

### Mudanças técnicas no frontend

1. Criar uma camada `shared/api` para contratos e utilitários comuns.
2. Criar `useDatasetPage` para paginação, ordenação e filtro textual padronizados.
3. Criar `compareStore` para seleção cruzada entre datasets.
4. Criar `filtersStore` para filtros persistentes por módulo.
5. Adicionar `ProvenanceBadge` e `ComparisonDrawer` em todas as telas fiscais.
6. Migrar componentes legados de tabela para um `FiscalDataTable` compartilhado.
7. Manter wrappers temporários para `Agregação`, `Conversão` e `Estoque` enquanto o frontend novo assume o tráfego.
8. Convergir explorers simplificados para a tabela rica compartilhada, preservando uma interface visual enxuta.

### Matriz de migração de abas

| Aba atual | Destino desejado | Mudança principal |
|---|---|---|
| `EFD` | `EFD` expandida | sair de `C170 + Bloco H` para cobertura completa do domínio |
| `Agregação (legado)` | `Produto Master` | transformar merge pontual em workbench de identidade de produto |
| `Conversão (legado)` | `Conversão` | separar fator declarado, inferido e manual |
| `Estoque (legado)` | `Estoque` | separar evento, inventário, estoque declarado e saldo |
| `Análise Fiscal` | ser decomposta | redistribuir as visões para módulos próprios |
| `Catálogo Datasets` | `Catálogo` | manter e expandir como camada de inspeção |

## Proposta de frontend e UX

### Princípios

- chrome visual minimalista;
- densidade informacional alta nas tabelas;
- experiência de workbench analítico;
- filtros rápidos e avançados;
- drill-down por linha e por origem;
- comparação entre datasets;
- edição controlada apenas em camadas autorizadas;
- exportação do recorte filtrado;
- visibilidade explícita de origem e regra aplicada.

### Módulo EFD

- árvore `0000 -> C100 -> C170 -> C176`;
- tabela por registro;
- painel de detalhes por linha;
- comparação entre entrega original e retificadora;
- dicionário de campos com metadados do mapeamento;
- inventário `H005/H010/H020`;
- confronto com `K200`.

### Módulo Produto Master

- workbench de agrupamento;
- score de confiança;
- comparação lado a lado entre candidatos;
- merge controlado com justificativa;
- visualização de conflito entre GTIN, NCM, CEST, descrição e unidade.

### Módulo Conversão

- fatores declarados;
- fatores inferidos;
- fatores manuais;
- painel de impacto antes de aplicar;
- trilha de auditoria de edição.

### Módulo Estoque

- evento fiscal;
- snapshot de inventário;
- saldo mensal;
- saldo anual;
- divergências;
- drill-down até item, documento e origem.

### Módulo Ressarcimento

- saída anchor;
- entrada candidata;
- score de escolha;
- fronteira;
- batimento item a item;
- resumo mensal;
- conciliação.

## Performance

1. Particionar por `cnpj_raiz`, `ano`, `mes` e `dataset`.
2. Filtrar cedo em LazyFrame.
3. Materializar só o necessário para a tela.
4. Separar datasets largos de datasets operacionais.
5. Usar manifests para decidir se reextrai ou reaproveita.
6. Evitar leitura repetida de arquivos grandes apenas para contagem.
7. Padronizar schemas para reduzir casts redundantes.
8. Reaproveitar dimensões compartilhadas globais.

## Governança e manutenção

1. Cada dataset deve ter contrato e owner técnico.
2. Toda extração nova deve registrar tabela Oracle e `sql_id`.
3. Toda transformação nova deve declarar upstream.
4. Mudança de schema exige bump de versão.
5. Edição manual exige motivo, usuário e timestamp.
6. Toda tela deve informar de onde o dado veio.
7. SQL de referência deve ser claramente marcada como `referencia`, não `raw`.
8. Homologação fiscal deve comparar mart nova com SQL antiga de referência.

## Critérios de aceite do plano

O plano será considerado implementado com sucesso quando:

1. EFD possuir módulo próprio com datasets estritos e coverage mínima obrigatória.
2. `conversao`, `agregacao` e `estoque` deixarem de depender de artefatos não canônicos.
3. SQLs analíticas pesadas deixarem de ser base primária de extração.
4. Catálogo de datasets estiver centralizado e utilizado por backend e pipeline.
5. Reprocessamento incremental por período estiver disponível.
6. Frontend expuser rastreabilidade de origem e drill-down.
7. Ajustes manuais estiverem desacoplados da camada raw.

## Roadmap de implementação

## Fase 0 - Congelamento do inventário e nomenclatura

Objetivo:
Criar o mapa oficial do que existe hoje e do que passa a existir como padrão.

### Todo

- [ ] Levantar todos os SQLs atuais e classificá-los como `raw`, `intermediaria`, `mart` ou `referencia`.
- [ ] Congelar uma lista de `dataset_id` canônicos.
- [ ] Mapear aliases legados para datasets canônicos.
- [ ] Documentar granularidade de cada dataset.
- [ ] Definir convenção de nomes para SQL, Parquet, manifest e routers.
- [ ] Marcar explicitamente os SQLs que não podem mais ser usados como base de extração.

## Fase 1 - Contratos e catálogo de schemas

Objetivo:
Transformar o mapeamento existente em contratos formais de dado.

### Todo

- [ ] Gerar contratos para `0000`, `0190`, `0200`, `0220`, `C100`, `C170`, `C190`, `C176`, `C197`, `H005`, `H010`, `H020`, `K200`.
- [ ] Criar catálogo por domínio: `efd`, `bi_xml`, `sitafe`, `produto`.
- [ ] Registrar chaves primárias e chaves de junção.
- [ ] Definir colunas obrigatórias de rastreabilidade.
- [ ] Definir versões de schema por dataset.
- [ ] Criar manifests mínimos por dataset.

## Fase 2 - Camada raw EFD completa

Objetivo:
Ter todas as extrações EFD mínimas e reutilizáveis.

### Todo

- [ ] Formalizar `reg_0000_historico`, `versionado` e `ultimo_periodo`.
- [ ] Criar ou ajustar SQL raw para `0190`.
- [ ] Criar ou ajustar SQL raw para `0200`.
- [ ] Criar ou ajustar SQL raw para `0205`.
- [ ] Criar ou ajustar SQL raw para `0220`.
- [ ] Criar ou ajustar SQL raw para `C100`.
- [ ] Criar ou ajustar SQL raw para `C170`.
- [ ] Criar ou ajustar SQL raw para `C190`.
- [ ] Criar ou ajustar SQL raw para `C176`.
- [ ] Criar ou ajustar SQL raw para `C197`.
- [ ] Criar ou ajustar SQL raw para `H005`, `H010`, `H020`.
- [ ] Criar ou ajustar SQL raw para `K200`.
- [ ] Materializar partições por `cnpj`, `ano`, `mes`.

## Fase 3 - Camada raw BI/XML e SITAFE mínima

Objetivo:
Extrair apenas o que é realmente necessário para enriquecer os domínios posteriores.

### Todo

- [ ] Definir SQL raw canônica para `NFe` item.
- [ ] Definir SQL raw canônica para `NFe` cabeçalho.
- [ ] Definir SQL raw canônica para informações complementares.
- [ ] Definir SQL raw canônica para eventos/documentos auxiliares relevantes.
- [ ] Definir SQL raw canônica para `sitafe_nfe_calculo_item`.
- [ ] Definir SQL raw canônica para `sitafe_produto_sefin`.
- [ ] Definir SQL raw canônica para `sitafe_mercadoria`.
- [ ] Garantir que nenhuma extração BI/XML ou SITAFE já venha consolidada em nível de mart.

## Fase 4 - Camada base tipada e versionada

Objetivo:
Mover tipagem, versionamento e deduplicação técnica para Polars.

### Todo

- [ ] Consolidar funções lazy por entidade base.
- [ ] Padronizar datas, números e colunas-chave.
- [ ] Resolver retificadoras e última entrega na camada base.
- [ ] Padronizar chaves de produto por `cnpj|cod_item`.
- [ ] Criar datasets base para inventário e estoque declarado.
- [ ] Publicar `base__efd__*` no catálogo.

## Fase 5 - Curated de produto master e conversão

Objetivo:
Separar identidade de produto e conversão de unidade da extração bruta.

### Todo

- [ ] Montar `cur__produto__cadastro_efd`.
- [ ] Montar `cur__produto__descricoes_praticadas`.
- [ ] Montar `cur__produto__candidatos_agregacao`.
- [ ] Montar `cur__conversao__candidatos`.
- [ ] Montar `cur__conversao__evidencias_uso_unidade`.
- [ ] Criar camada de `edits` para agregação e conversão.
- [ ] Garantir precedência correta entre automático e manual.

## Fase 6 - Curated de estoque e inventário

Objetivo:
Criar a base canônica de eventos e snapshots.

### Todo

- [ ] Construir `cur__estoque__eventos` a partir de `C100/C170`.
- [ ] Construir `cur__estoque__snapshots_inventario` a partir de `H005/H010/H020`.
- [ ] Construir `cur__estoque__k200`.
- [ ] Separar claramente evento, snapshot e saldo.
- [ ] Incluir enriquecimento opcional com BI/XML e SITAFE sem contaminar a camada base.
- [ ] Garantir trilha de origem por linha.

## Fase 7 - Marts analíticos

Objetivo:
Recriar as visões analíticas fora do Oracle.

### Todo

- [ ] Implementar `mart__produto__grupos`.
- [ ] Implementar `mart__produto__conflitos`.
- [ ] Implementar `mart__conversao__aplicada`.
- [ ] Implementar `mart__estoque__saldo_mensal`.
- [ ] Implementar `mart__estoque__saldo_anual`.
- [ ] Implementar `mart__estoque__divergencias`.
- [ ] Implementar `mart__ressarcimento__batimento_item`.
- [ ] Implementar `mart__ressarcimento__resumo_mensal`.
- [ ] Implementar `mart__ressarcimento__conciliacao`.
- [ ] Validar resultados com SQLs antigas de referência.

## Fase 8 - Refatoração do backend

Objetivo:
Fazer a API servir datasets canônicos em vez de arquivos legados ad hoc.

### Todo

- [x] Criar routers canônicos iniciais por domínio para `produto`, `conversao` e `estoque`.
- [x] Mover lógica repetida de paginação/filtro para base compartilhada no backend.
- [x] Criar serviço de Parquet desacoplado da estrutura PySide.
- [x] Criar serviço de SQL reutilizável em `src/utilitarios/sql_service.py`.
- [x] Desacoplar `CNPJ_ROOT` e paths críticos do backend da configuração da interface PySide.
- [x] Promover o pipeline para serviço real do backend.
- [x] Promover a agregação para módulo canônico compartilhado em `src/utilitarios/servico_agregacao.py`.
- [x] Isolar `dossie` atrás de bridge explícita em `backend/services/`.
- [x] Substituir a implementação de `dossie` por módulos canônicos em `src/utilitarios/dossie_*`, mantendo wrappers legados apenas para compatibilidade.
- [x] Fazer endpoints retornarem metadados de rastreabilidade.
- [ ] Desacoplar API nova das convenções legadas de arquivo.
- [ ] Manter compatibilidade temporária por alias.
- [ ] Criar endpoints de catálogo, inspeção e lineage.
- [x] Substituir a bridge temporária restante de `dossie` por implementação definitiva fora de `src/interface_grafica/`.

## Fase 9 - Refatoração do frontend

Objetivo:
Substituir a leitura de artefatos legados por módulos analíticos canônicos.

### Todo

- [x] Criar uma shell fiscal compartilhada inicial para os módulos novos.
- [x] Expandir a aba EFD para o escopo mínimo obrigatório.
- [x] Criar módulo `produto_master`.
- [x] Criar módulo canônico de `conversao`.
- [x] Criar módulo canônico de `estoque`.
- [ ] Criar módulo canônico de `ressarcimento`.
- [x] Convergir o `FiscalDatasetExplorer` para a tabela compartilhada rica, mantendo a interface visual enxuta.
- [x] Criar `FiscalDataTable` compartilhada com presets, destaques e exportação.
- [x] Criar `AdvancedFilterBar` compartilhada.
- [x] Criar `ComparisonDrawer` compartilhado.
- [x] Criar `ProvenanceBadge` compartilhado.
- [ ] Padronizar hooks de paginação, ordenação e comparação.
- [x] Refinar os módulos fiscais com interface minimalista e grade analítica rica como padrão transversal.
- [x] Transformar `agregacao`, `conversao` e `estoque` legados em convivência temporária com os módulos novos.
- [ ] Redistribuir as visões da atual `Análise Fiscal` para módulos próprios.
- [ ] Adicionar drill-down, comparação e exportação.
- [ ] Adicionar painel fixo de rastreabilidade.
- [ ] Criar plano de desligamento gradual das abas legadas.

## Fase 10 - Governança operacional

Objetivo:
Garantir manutenção contínua, performance e auditabilidade.

### Todo

- [x] Expor resumo do `SchemaRegistry` na camada de observabilidade.
- [x] Expor inspeção de versão/hash/diff de schema no catálogo de datasets.
- [ ] Criar rotina operacional de verificação automática de schema por dataset.
- [ ] Criar rotina de comparação entre extração nova e SQL de referência.
- [ ] Criar rotina de auditoria de `edits`.
- [ ] Criar métricas de reuso de datasets.
- [ ] Criar métricas de economia de consulta Oracle.
- [ ] Criar regra para reprocessamento incremental por `cnpj + período + dataset`.
- [ ] Criar documentação operacional para novas demandas.

## Fase 12 - Migração desktop para Tauri

Objetivo:
Substituir progressivamente a shell PySide pela shell Tauri mantendo FastAPI como backend local do app.

### Todo

- [x] Verificar a existência e a aderência da base `frontend/src-tauri/`.
- [x] Ajustar o frontend para resolver `API_BASE_URL` em shell desktop empacotada.
- [x] Ajustar o fluxo de desenvolvimento da shell Tauri para subir o app real.
- [ ] Instalar e validar toolchain local de Tauri nesta máquina:
  - Node dependencies
  - Rust (`cargo`/`rustc`)
  - pré-requisitos Windows do Tauri
- [ ] Executar `tauri dev` com backend e frontend reais.
- [ ] Definir estratégia oficial de backend local para desktop:
  - sidecar Python empacotado
  - porta local controlada
  - healthcheck e retry na inicialização
- [ ] Adaptar o app Tauri para inicializar e monitorar o backend local.
- [ ] Padronizar logs, crash handling e shutdown coordenado entre shell e backend.
- [ ] Migrar fluxos restantes da experiência desktop para o frontend React.
- [ ] Descontinuar o entrypoint PySide após a equivalência funcional mínima.

## Fase 11 - Deprecação controlada do legado

Objetivo:
Retirar dependências antigas sem ruptura.

### Todo

- [x] Mapear os principais pontos do backend que ainda dependem de `src/interface_grafica`.
- [ ] Mapear exaustivamente quais telas e serviços ainda leem arquivos legados diretamente.
- [ ] Desligar dependências antigas por domínio, começando por EFD.
- [ ] Descontinuar formalmente a interface PySide como entrypoint do projeto.
- [x] Remover a dependência funcional do backend em relação ao `dossie` legado, mantendo wrappers apenas para compatibilidade.
- [ ] Manter apenas SQLs de referência de homologação em pasta separada.
- [ ] Remover ou congelar pipelines duplicadas.
- [ ] Encerrar uso de SQL analítica como fonte primária.

## Prioridades executivas

### Prioridade 1

- Fase 0
- Fase 1
- Fase 2

### Prioridade 2

- Fase 3
- Fase 4
- Fase 5
- Fase 6

### Prioridade 3

- Fase 7
- Fase 8
- Fase 9

### Prioridade 4

- Fase 10
- Fase 11

## Resultado esperado

Ao final deste plano, o projeto deve operar com:

- extrações Oracle mínimas e reutilizáveis;
- domínios bem separados;
- EFD estrita e completa;
- produto master, conversão e estoque apoiados em datasets canônicos;
- marts analíticos em Polars/Parquet;
- frontend orientado a exploração analítica;
- rastreabilidade ponta a ponta;
- menos carga no Oracle e mais reaproveitamento local.
