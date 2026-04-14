# Plano de Arquitetura Fiscal

Este diretório consolida a proposta e o estado atual da reestruturação do módulo fiscal do projeto.

## Princípio operacional

1. extrair dados dos bancos com eficiência;
2. consolidar e reaproveitar em datasets canônicos e Parquet;
3. exibir para visualização, auditoria e análise.

## Domínios fiscais oficiais

- EFD
- Documentos Fiscais
- Fiscalização
- Cruzamentos / Verificações / Classificação dos Produtos

## Situação atual de implementação

A arquitetura proposta já saiu do plano e entrou em operação inicial dentro do repositório:

- **EFD**: resumo real + tabela operacional para `C170` e `Bloco H` + filtro textual + filtro por coluna + ordenação + detalhe de registro;
- **Documentos Fiscais**: resumo real + tabelas operacionais para `NF-e`, `NFC-e`, `CT-e`, `informações complementares` e `contatos` + filtro textual + filtro por coluna + ordenação + detalhe de registro;
- **Fiscalização**: resumo real + painel de cadastro + tabela de malhas + lista de DSFs relacionadas + filtro textual + filtro por coluna + ordenação nas malhas + detalhe da malha selecionada;
- **Análise Fiscal**: resumo real + tabelas operacionais para estoque, agregação, conversão e produtos-base + filtro textual + filtro por coluna + ordenação + detalhe de registro.

## Trilha de infraestrutura já endereçada

- cache L1/L2 para leitura de SQL (`src/utilitarios/sql_cache.py` + `ler_sql.py`)
- catálogo SQL com índice reutilizável e invalidação (`src/utilitarios/sql_catalog.py`)
- observabilidade básica reutilizável com logging JSON e métricas opcionais (`src/observabilidade/`)
- registro versionado de schemas em `workspace/app_state/schema_registry.json`
- shell Tauri v2 inicial em `frontend/src-tauri/`
- helper Delta Lake com chaveamento por ambiente em `src/utilitarios/delta_lake.py`
- backend fiscal novo inteiro preparado para ler Parquet ou Delta conforme o dataset materializado
- `ParquetService` preparado para listar, ler e salvar Parquet ou Delta
- catálogo canônico de datasets com aliases e resolução Delta/Parquet em `src/utilitarios/dataset_registry.py`
- pontos reais do pipeline já ligados ao catálogo canônico (`tb_documentos`, `mov_estoque`, `aba_mensal`)
- wrappers de materialização canônica para `c170_xml`, `c176_xml`, `fatores_conversao`, `produtos_agrupados` e `produtos_final`
- cache do Fisconforme adaptado para a trilha canônica (`dados_cadastrais`, `malhas`)
- wrapper de materialização canônica para `bloco_h`
- resolução centralizada de datasets nos routers fiscais principais (`EFD`, `Análise`, `Documentos Fiscais` e `Fiscalização`)
- catálogo expandido para datasets documentais auxiliares
- inspector central de catálogo no backend/observabilidade
- tela operacional de catálogo no frontend
- extração no frontend alinhada à realidade atual do pipeline
- sugestão automática de abordagem baseada no catálogo
- datasets ausentes e famílias SQL sugeridas no frontend
- reprocessamento seletivo por domínio no modo de processamento
- stack local de Prometheus/Grafana/Marquez em `infra/observability/`

## Migração das abas atuais

- Estoque -> Cruzamentos
- Agregação -> Verificações
- Conversão -> Verificações

## Documento consolidado

Use `00_CONSOLIDADO_MODULO_FISCAL.md` como visão concatenada do estado atual, da arquitetura alvo, dos contratos e da migração.

## Arquivos deste diretório

- `00_CONSOLIDADO_MODULO_FISCAL.md`
- `01_MAPA_ESTRUTURA_FISCAL.md`
- `02_CATALOGO_DATASETS_PARQUET.md`
- `03_CONTRATO_ENDPOINTS_FISCAL.md`
- `04_ESTRUTURA_PASTAS_FRONTEND_BACKEND.md`
- `05_MIGRACAO_ABAS_ATUAIS.md`
- `06_PLANO_COMPLETO_IMPLEMENTACAO.md`
- `07_BACKLOG_DETALHADO_POR_ETAPA.md`
- `08_CRITERIOS_ACEITE_TESTES_RISCOS.md`
- `09_SEQUENCIA_EXECUCAO.md`
- `10_SQL_CACHE_OBSERVABILIDADE_SCHEMAS.md`
- `11_TAURI_DELTA_OPENLINEAGE_OBSERVABILIDADE_MOBILE.md`
- `12_VALIDACAO_LOCAL_DELTA_LINEAGE_OBSERVABILIDADE.md`
- `13_LEITURA_ADAPTATIVA_PARQUET_DELTA.md`
- `14_COBERTURA_DELTA_NOS_DOMINIOS_FISCAIS.md`
- `15_CATALOGO_CANONICO_DATASETS.md`
- `16_MATERIALIZACAO_CANONICA_PIPELINE.md`
- `17_WRAPPERS_MATERIALIZACAO_CANONICA.md`
- `18_FISCONFORME_BLOCO_H_CANONICOS.md`
- `19_RESOLUCAO_CENTRALIZADA_ROUTERS.md`
- `20_DOCUMENTOS_FISCAIS_LOCALIZADOR.md`
- `21_CATALOGO_DOCUMENTAL_E_FISCALIZACAO.md`
- `22_INSPECAO_CATALOGO_BACKEND.md`
- `23_TELA_OPERACIONAL_CATALOGO.md`
- `24_EXTRACAO_FRONTEND_REALIDADE_ATUAL.md`
- `25_SUGESTAO_AUTOMATICA_ABORDAGEM.md`
- `26_DATASETS_AUSENTES_E_FAMILIAS_SQL.md`
- `27_REPROCESSAMENTO_SELETIVO_DOMINIO.md`
- `AGENTS_NOVO.md`
- `AGENTS_SQL_NOVO.md`
