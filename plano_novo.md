# Plano Novo - incorporacao de `C:\funcoes - Copia` no `audit_react`

Data base: 2026-03-30

Status deste documento:

- atualizado para o estado real ja implementado no repositorio;
- registra o que foi concluido no core fiscal por CNPJ;
- registra o que foi homologado localmente;
- registra a trilha ST ja incorporada ao backend;
- redefine a proxima fase para expansao fiscal controlada apos ST.

## 1. Objetivo consolidado

Objetivo do projeto:

- incorporar ao `audit_react` as extracoes, analises e consultas fiscais do acervo `C:\funcoes - Copia`;
- preservar a arquitetura alvo `FastAPI + React + Parquet + Polars`;
- evitar regressao para SQLs monoliticas;
- manter rastreabilidade fiscal por CNPJ;
- substituir stubs e simplificacoes por fluxo real auditavel.

Decisoes estruturais que permanecem validas:

- o `audit_react` continua sendo a aplicacao-alvo;
- o projeto externo continua sendo referencia funcional e documental, nao base de acoplamento estrutural;
- o isolamento operacional continua em `storage/CNPJ/{cnpj}`;
- a fronteira publica continua em FastAPI e React;
- nomes publicos das tabelas gold foram preservados.

## 2. Estado atual implementado

### 2.1 Camadas por CNPJ

Implementado:

- `extraidos/` como bronze operacional por CNPJ;
- `silver/` como camada intermediaria recomponivel em Polars;
- `parquets/` como camada gold publica;
- manifesto operacional por CNPJ com contagem, schema, timestamps e versao de regra.

Camadas efetivamente suportadas pela API:

- `GET /api/tabelas/{cnpj}?camada=extraidos|silver|parquets`
- `GET /api/tabelas/{cnpj}/{nome}?camada=extraidos|silver|parquets`
- `GET /api/storage/{cnpj}/manifesto`

Compatibilidade:

- `parquets` continua sendo a camada padrao quando `camada` nao e informada.

### 2.2 Extracao Oracle e rastreabilidade SQL

Implementado:

- correcao do `mapeamento_sql_oracle.py` para analisar SQL ja renderizada com `{{FONTE_*}}` resolvidas;
- inclusao de `reg0005` e `reg0190` no conjunto essencial de extracao;
- inclusao de `c176`, `nfe_dados_st` e `e111` no catalogo versionado de consultas Oracle;
- expansao do catalogo de fontes Oracle e do mapeamento estrutural para `REG_C176`, `REG_E111`, `DM_EFD_AJUSTES` e `NFE_XML`;
- manutencao da regra `data_limite=None` como ausencia total de corte temporal;
- SQL versionado mantido como base da extracao do projeto atual.

Resultado:

- o mapa estrutural Oracle deixou de subestimar fontes reais usadas pelas SQLs embutidas.

### 2.3 Pipeline fiscal gold

Implementado com geracao real em Polars:

- `produtos_unidades`
- `produtos`
- `produtos_agrupados`
- `id_agrupados`
- `fatores_conversao`
- `produtos_final`
- `nfe_entrada`
- `mov_estoque`
- `aba_mensal`
- `aba_anual`
- `produtos_selecionados`
- `ajustes_e111`
- `st_itens`

Estado da logica:

- `produtos_unidades` passou a derivar da silver;
- `produtos` usa `id_produto` como identificador publico canonico;
- `produtos_agrupados` aplica agrupamento automatico e override manual;
- `fatores_conversao` usa precedencia `manual > reg0220 > inferencia > fallback`;
- `mov_estoque` executa trilha cronologica real;
- `aba_mensal` e `aba_anual` foram refatoradas para o padrao do novo pipeline e derivam exclusivamente da trilha materializada.
- `ajustes_e111` materializa a trilha gold de ajustes E111 por competencia;
- `st_itens` consolida XML de ST/FCP e C176 por documento/item sem alterar o contrato das 11 tabelas do core.

### 2.4 Trilha ST incorporada

Implementado:

- `server/python/consultas/c176.sql`
- `server/python/consultas/nfe_dados_st.sql`
- `server/python/consultas/e111.sql`
- `silver/c176_xml.parquet` alimentado por fonte real quando a extracao local existir;
- `silver/nfe_dados_st.parquet`
- `silver/e111_ajustes.parquet`
- `parquets/ajustes_e111.parquet`
- `parquets/st_itens.parquet`
- enriquecimento aditivo de `silver/c176_xml` com campos diretos do `C176` e dados de entrada XML obtidos de `extraidos/nfe.parquet` por `chave+item`;
- projecao canonica de homologacao no comparador ST para avaliar paridade sem empobrecer o bronze local.

Decisao aplicada:

- a trilha ST foi adicionada como expansao complementar do pipeline;
- o contrato das 11 tabelas gold do core foi preservado;
- `E111` passou a ser tratado como trilha propria de apuracao, nao como simples atributo documental;
- a conciliacao de ST reutiliza o mapeamento de produtos do core via `id_agrupado`.
- o bronze ST local continua mais rico que o externo; a homologacao passou a usar `shape_bruto_local` e `shape_canonico_local` como visoes distintas.

### 2.5 Contratos e compatibilidades fechadas

Contrato canonico atual:

- `descricao_padrao` e o nome publico canonico para descricao em fatores e grupos;
- `id_produto` e o identificador publico canonico da tabela `produtos`.

Compatibilidade residual aceita:

- leitura defensiva de `descr_padrao` apenas na borda do frontend;
- agregacao aceita `id_item` ou `id_produto` ao validar tabelas antigas ja materializadas;
- desagregacao tolera `descricao_padrao` ou `descr_padrao` em parquets legados.

Compatibilidade que nao deve ser expandida:

- nenhum novo modulo deve voltar a depender de `id_item` como shape principal;
- nenhum novo contrato deve reintroduzir `descr_padrao` como nome publico.

### 2.6 Operacionalizacao do ambiente local

Implementado:

- `server/python/scripts/preparar_ambiente_backend.py`
- `server/python/scripts/verificar_prontidao_backend.py`
- atualizacao de `requirements.txt` com `python-multipart`

Resultado:

- o backend agora possui bootstrap reproduzivel para testes e E2E local;
- `test_api.py` deixou de depender de preparacao manual fora do repositorio;
- o verificador retorna `pronto_core=true` quando o ambiente esta apto a rodar a suite do core.

### 2.7 E2E local

Implementado em `server/python/scripts/executar_e2e_local.py`:

- uso de `id_produto` como identificador publico principal;
- consulta de `extraidos`, `silver` e `parquets`;
- leitura de manifesto por CNPJ;
- validacao de agregacao, conversao, recalculo, exportacao e integridade do pipeline;
- leitura de `ajustes_e111` e `st_itens` na mesma trilha de smoke;
- restauracao automatica de `edicoes/` e `exportacoes/` ao final da validacao.

Resultado:

- a homologacao E2E local nao deixa lixo operacional no CNPJ de referencia, salvo quando executada com `--manter-alteracoes`.

## 3. Homologacao local concluida

### 3.1 Suite automatizada do core

Validado localmente no ambiente ativo:

- `tests/test_extrair_oracle.py`
- `tests/test_pipeline_tabelas.py`
- `tests/test_trilha_st.py`
- `tests/test_resumos_estoque.py`
- `tests/test_api.py`

Resultado consolidado:

- `39 passed`

### 3.2 CNPJ piloto homologado

CNPJ piloto utilizado:

- `37671507000187`

Validacoes executadas:

- execucao direta do orquestrador;
- execucao do E2E local;
- leitura de manifesto;
- validacao de camadas `extraidos`, `silver` e `parquets`;
- validacao da cadeia `produtos -> fatores -> estoque`;
- validacao da materializacao das tabelas complementares `ajustes_e111` e `st_itens`;
- reprocessamento apos agregacao e fator manual;
- restauracao final do storage operacional.

Estado persistido apos restauracao final do piloto:

- `edicoes/` vazio;
- `exportacoes/` com relatorios operacionais da paridade ST;
- `extraidos` com 11 tabelas;
- `silver` com 9 tabelas;
- `parquets` com 13 tabelas.

Contagens persistidas no fechamento final:

- `ajustes_e111.parquet`: 78 registros
- `st_itens.parquet`: 1947 registros
- `mov_estoque.parquet`: 5489 registros
- `aba_mensal.parquet`: 2433 registros
- `aba_anual.parquet`: 1114 registros

Observacao do piloto:

- o CNPJ homologado localmente ja possui as novas extracoes ST materializadas no `audit_react`;
- a cadeia complementar da ST foi preenchida com dados reais, incluindo `c176_xml`, `nfe_dados_st`, `e111_ajustes`, `ajustes_e111` e `st_itens`;
- `silver/c176_xml` foi enriquecida no piloto e passou a materializar 43 colunas sem quebrar `st_itens`;
- o gap remanescente deixou de ser de extracao local e de divergencia estrutural ampla; agora ele ficou concentrado em divergencias residuais de contagem frente ao projeto externo.

Schemas validados:

- `mov_estoque`: `id_agrupado`, `descricao`, `tipo`, `data`, `quantidade`, `valor_unitario`, `valor_total`, `saldo`, `custo_medio`, `cfop`, `origem`
- `aba_mensal`: `id_agrupado`, `descricao`, `mes`, `saldo_inicial`, `entradas`, `saidas`, `saldo_final`, `custo_medio`, `valor_estoque`, `qtd_movimentos`, `omissao`
- `aba_anual`: `id_agrupado`, `descricao`, `ano`, `saldo_inicial_ano`, `total_entradas`, `total_saidas`, `saldo_final_ano`, `custo_medio_anual`, `valor_estoque_final`, `meses_com_omissao`, `total_omissao`

## 4. Pendencias tecnicas remanescentes

Pendencias remanescentes que ainda existem, mas nao bloqueiam a fase atual:

- `weasyprint` nao esta instalado no ambiente local ativo;
- `reportlab` nao esta instalado no ambiente local ativo.
- a paridade externa da trilha ST ainda nao foi fechada no `audit_react`, apesar de o piloto ja estar reextraido com `c176`, `nfe_dados_st` e `e111`.

Impacto:

- isso nao bloqueia o core fiscal, `test_api.py` ou o E2E local;
- isso afeta apenas renderizacao PDF em runtime dos relatorios.

Acao recomendada quando for tratar relatorios PDF:

- executar `python scripts/preparar_ambiente_backend.py --escopo completo`

Acao recomendada para fechar a paridade ST:

- extracao Oracle local do piloto para `c176`, `nfe_dados_st` e `e111`: concluida
- enriquecimento de `silver/c176_xml` e projecao canonica do comparador: concluidos
- proxima acao: investigar divergencias residuais de contagem apontadas pelo comparador e rerodar `python scripts/comparar_paridade_externa.py 37671507000187`.

## 5. Comparacao com `C:\funcoes - Copia`

O repositorio agora possui um comparador operacional em:

- `server/python/scripts/comparar_paridade_externa.py`

Escopo atual do comparador:

- `extraidos/c176`
- `extraidos/nfe_dados_st`
- `extraidos/e111`
- `silver/c176_xml`
- cadeia local complementar:
  - `silver/nfe_dados_st`
  - `silver/e111_ajustes`
  - `parquets/ajustes_e111`
  - `parquets/st_itens`

Resultado atual no CNPJ piloto:

- o projeto externo possui dados ST ja materializados;
- o `audit_react` agora possui os extraidos `c176`, `nfe_dados_st` e `e111` materializados localmente no piloto;
- a cadeia local ST ficou completa, com `silver/c176_xml`, `silver/nfe_dados_st`, `silver/e111_ajustes`, `parquets/ajustes_e111` e `parquets/st_itens` preenchidos;
- o status atual do comparador passou para `divergente`;
- o comparador agora gera, por artefato:
  - `shape_bruto_local`
  - `shape_canonico_local`
  - `externo`
  - `shape_canonico_externo`
  - `paridade_shape_bruto`
  - `paridade_shape_canonico`
  - `divergencia_residual_justificada`
- na visao canonica atual do piloto:
  - `extraidos/c176`: `schema_igual=true`, `colunas_iguais=true`, `diff_registros=16`
  - `extraidos/nfe_dados_st`: `schema_igual=true`, `colunas_iguais=true`, `diff_registros=19`
  - `extraidos/e111`: `schema_igual=true`, `colunas_iguais=true`, `diff_registros=2`
  - `silver/c176_xml`: `schema_igual=true`, `colunas_iguais=true`, `diff_registros=16`
- as divergencias atuais seguem classificadas por camada:
  - bronze: `extraidos/c176`, `extraidos/nfe_dados_st`, `extraidos/e111`
  - silver: `silver/c176_xml`
- o comparador gera resumo por camada e relatorio em Markdown para auditoria operacional, e a divergencia residual atual da ST esta concentrada em contagem de registros apos projecao canonica.

## 6. Proxima fase recomendada

Com o core fiscal fechado operacionalmente e a trilha ST incorporada no codigo, a proxima fase recomendada passa a ser concluir a paridade externa da ST e, em seguida, expandir a cobertura fiscal sem romper o baseline homologado.

Prioridade recomendada da proxima fase:

1. reduzir as divergencias residuais de contagem da trilha ST:
   investigar os `diff_registros` remanescentes do bronze e da `silver/c176_xml` apos projecao canonica
2. rerodar homologacao da ST ate classificar o resultado como equivalente ou divergente residual aceita
3. trilha documental complementar:
   `cte`, `nfe_evento`
4. trilha de monitoramento fiscal:
   `omissoes`, `fisconforme`, `fronteira`, `difal`, `verif_cnpjs`

Regra de entrada da proxima fase:

- nenhuma expansao nova deve quebrar:
  - as 11 tabelas gold do core;
  - as 2 tabelas gold complementares da trilha ST;
  - o contrato publico por CNPJ;
  - o bootstrap do backend;
  - a suite automatizada do core;
  - a homologacao do CNPJ piloto.

## 7. Decisao operacional a partir daqui

O plano original de incorporacao do acervo externo continua valido, mas a etapa "substituir stubs e fechar o core operacional" deve ser considerada concluida.

Estado da migracao:

- Fase 0 - catalogacao e compatibilidade: concluida para o core atual
- Fase 1 - extracao bronze essencial: concluida para o conjunto atual
- Fase 2 - silver do core: concluida
- Fase 3 - gold do core: concluida
- Fase 4 - API e operacionalizacao do core: concluida
- Fase 5 - homologacao local do core: concluida
- Fase 6 - implementacao da trilha ST no codigo: concluida
- Fase 7 - extracao real e homologacao inicial da ST: concluida
- Fase 8 - reducao estrutural de divergencias da ST por projecao canonica e enriquecimento da `silver/c176_xml`: concluida
- Fase 9 - investigacao das divergencias residuais de contagem da ST: pendente
- Proxima etapa real: investigacao das divergencias residuais de contagem da ST, seguida de expansao fiscal incremental sobre baseline homologado

Conclusao:

- `plano_novo.md` deixa de ser um plano de implantacao do core e passa a ser o registro oficial de que:
  - o core fiscal por CNPJ com Parquet + Polars foi incorporado ao `audit_react` e esta operacionalmente homologado no ambiente local;
  - a trilha ST foi implementada no backend, extraida no piloto e materializada sem quebra do core;
  - a reducao estrutural de divergencia da ST por projecao canonica ja foi implementada;
  - a proxima etapa obrigatoria e investigar as divergencias residuais de contagem da paridade externa da trilha ST.
