# audit_react

Sistema de auditoria fiscal da SEFIN com frontend React e backend Python (FastAPI + Polars), organizado por CNPJ e com pipeline de tabelas em Parquet.

Documentacao funcional do pipeline, cobrindo tabelas gold e camada silver:

- [docs/README.md](./docs/README.md)

## Arquitetura

- `client/`: interface React para extracao, consulta, agregacao, conversao e estoque.
- `server/index.ts`: gateway HTTP para servir frontend e proxy `/api` para FastAPI.
- `server/python/api.py`: API principal do backend.
- `server/python/audit_engine/`: contratos, geradores e orquestrador do pipeline.
- `server/python/consultas/`: consultas SQL versionadas e parametrizadas.
- `storage/CNPJ/{cnpj}/silver/`: camada intermediaria recomponivel em Polars, gerada automaticamente antes das tabelas gold.
- `storage/CNPJ/{cnpj}/parquets/`: camada gold consumida pela UI e pelos endpoints publicos.
- As consultas versionadas foram adaptadas tomando como referencia o acervo local em `C:\funcoes - Copia\sql`.

## Pipeline

Ordem topologica atual:

1. `produtos_unidades`
2. `produtos`
3. `produtos_agrupados`
4. `fatores_conversao`
5. `produtos_final`
6. `id_agrupados`
7. `nfe_entrada`
8. `mov_estoque`
9. `aba_mensal`
10. `aba_anual`
11. `produtos_selecionados`
12. `ajustes_e111`
13. `st_itens`

## Configuracao segura

1. Copie `.env.example` para `.env`.
2. Preencha credenciais Oracle e caminhos locais.
3. Nunca versionar segredos reais no repositorio.

## Execucao local

### 1) Backend Python

```bash
cd server/python
python scripts/preparar_ambiente_backend.py --escopo core
python scripts/verificar_prontidao_backend.py
uvicorn api:app --reload --port 8000
```

Se quiser instalar o backend completo, incluindo renderizacao HTML -> PDF:

```bash
cd server/python
python scripts/preparar_ambiente_backend.py --escopo completo
```

Dependencias minimas para testes do core:

- `fastapi`, `polars`, `pypdf`, `python-docx` e demais pacotes da base do backend
- `reportlab` e `weasyprint` nao sao mais obrigatorios para a suite de `test_api.py`, mas continuam sendo dependencias validas do runtime de relatorios

Antes de rodar `pytest` ou o E2E local, o passo de prontidao do backend deve retornar `pronto_core=true`.

### 2) Frontend + gateway

```bash
pnpm install
pnpm dev
```

Frontend: `http://localhost:3000`

## Runbook MVP

### Fluxo operacional

1. Selecionar CNPJ ativo no cabecalho.
2. Ir para `Extracao`, escolher consultas SQL e executar pipeline.
   Deixe `Data limite` em branco para extrair todo o historico disponivel do CNPJ.
3. Validar tabelas em `Consulta`.
4. Registrar ajustes em `Agregacao` e `Conversao`.
5. Reprocessar em cascata (automatico nos endpoints de edicao).
6. Conferir resultados em `Estoque`.
7. Exportar CSV/XLSX/Parquet por `Consulta`.

### Ajuste Oracle (dicionario real)

1. Use `GET /api/oracle/conexao` para validar credenciais.
2. Abra `Configuracoes` e valide os aliases Oracle pela interface.
3. Use a busca integrada ou `GET /api/oracle/fontes?termo=...` para localizar views/tabelas candidatas.
4. Inspecione colunas pelo botao da tela ou `GET /api/oracle/colunas/{objeto}?owner=...`.
5. Salve os aliases corretos na UI; os overrides ficam persistidos em `storage/CNPJ/_sistema/fontes_oracle.json`.
6. Rode a extracao novamente pela tela `Extracao` ou endpoint `/api/pipeline/executar`.

### Estrutura por CNPJ

```text
storage/CNPJ/{cnpj}/
  extraidos/
  silver/
  parquets/
  edicoes/
    agregacao.json
    fatores.json
  exportacoes/
```

### Manifesto operacional

Cada execucao gera um manifesto logico do CNPJ com contagem, schema e timestamps das camadas:

- `extraidos`
- `silver`
- `parquets`

O manifesto pode ser consultado em `GET /api/storage/{cnpj}/manifesto`.

### Consulta por camada

Os endpoints de leitura de tabelas agora aceitam `camada=parquets|silver|extraidos`:

- `GET /api/tabelas/{cnpj}?camada=silver`
- `GET /api/tabelas/{cnpj}/{nome}?camada=extraidos`

Sem informar `camada`, a API continua usando `parquets` para preservar compatibilidade com o frontend existente.

## Testes

```bash
cd server/python
python scripts/verificar_prontidao_backend.py
python -m pytest -q tests
```

Para executar apenas a suite principal de API:

```bash
cd server/python
python scripts/verificar_prontidao_backend.py
python -m pytest -q tests/test_api.py
```

Dependencias extras para renderizacao real de PDF:

- `weasyprint`: renderizador HTML -> PDF preferencial
- `reportlab`: fallback textual de PDF usado pelo runtime quando `weasyprint` nao estiver disponivel

## E2E local (fluxo completo)

```bash
cd server/python
python scripts/verificar_prontidao_backend.py
python scripts/executar_e2e_local.py 37671507000187
```

O E2E restaura automaticamente `edicoes/` e `exportacoes/` ao final da validacao. Para manter as alteracoes produzidas durante o teste:

```bash
python scripts/executar_e2e_local.py 37671507000187 --manter-alteracoes
```

Com Oracle real:

```bash
python scripts/verificar_prontidao_backend.py
python scripts/executar_e2e_local.py 37671507000187 --executar-extracao
```

## Comparacao com `C:\funcoes - Copia`

O backend agora possui um comparador local de paridade para a trilha ST:

```bash
cd server/python
python scripts/comparar_paridade_externa.py 37671507000187
```

O script compara contagem, colunas e schema entre os artefatos do `audit_react` e os parquets equivalentes do projeto externo para:

- `extraidos/c176`
- `extraidos/nfe_dados_st`
- `extraidos/e111`
- `silver/c176_xml`

No piloto `37671507000187`, a extracao local da trilha ST ja foi executada com Oracle real. O status atual do comparador e `divergente`, com cadeia local completa e divergencias remanescentes classificadas por camada.

Nesta fase, o comparador passou a avaliar cada artefato em duas visoes:

- `shape_bruto_local`: parquet operacional real do `audit_react`
- `shape_canonico_local`: projecao de homologacao usada para medir paridade externa

No estado atual do piloto, a divergencia estrutural ampla foi reduzida. Para `extraidos/c176`, `extraidos/nfe_dados_st`, `extraidos/e111` e `silver/c176_xml`, a visao canonica ja converge em schema e colunas com o projeto externo; a divergencia remanescente ficou concentrada em contagem de registros.

Tambem resume a cadeia local completa da ST, cobrindo:

- `silver/nfe_dados_st`
- `silver/e111_ajustes`
- `parquets/ajustes_e111`
- `parquets/st_itens`

Para salvar um relatorio auditavel em Markdown:

```bash
cd server/python
python scripts/comparar_paridade_externa.py 37671507000187 --saida-markdown storage/CNPJ/37671507000187/exportacoes/paridade_st.md
```

Para salvar tambem o JSON operacional no mesmo ciclo:

```bash
cd server/python
python scripts/comparar_paridade_externa.py 37671507000187 --saida storage/CNPJ/37671507000187/exportacoes/paridade_st.json --saida-markdown storage/CNPJ/37671507000187/exportacoes/paridade_st.md
```

## Observacoes

- Endpoints de agregacao/conversao/exportacao estao implementados sem stubs.
- O pipeline materializa automaticamente a camada `silver` com `tb_documentos`, `item_unidades`, `itens`, `descricao_produtos`, `fontes_produtos`, `c170_xml`, `c176_xml`, `nfe_dados_st` e `e111_ajustes`.
- `silver/c176_xml` agora foi enriquecida com campos diretos do `C176` e com dados de entrada XML obtidos de `extraidos/nfe.parquet` por `chave+item`, sem criar dependencia persistida de `silver -> gold`.
- Reprocessamento usa tabela editada + dependentes transitivos.
- Extracao Oracle utiliza SQL versionado em `server/python/consultas`.
- O conjunto essencial de extracao agora inclui `reg0005`, `reg0190`, `c176`, `nfe_dados_st` e `e111`, alem das consultas fiscais ja existentes.
- A conexao Oracle agora ajusta `NLS_NUMERIC_CHARACTERS = '.,'` na sessao para suportar a extracao numerica de XML fiscal da trilha ST.
- O contrato publico de `fatores_conversao.parquet` usa `descricao_padrao` como campo canonico; compatibilidade com `descr_padrao` fica restrita a leitura defensiva no frontend.
- O fluxo E2E local valida camadas (`extraidos`, `silver`, `parquets`), manifesto por CNPJ, a cadeia `produtos -> fatores -> estoque` e as tabelas complementares `ajustes_e111` e `st_itens`.
- O relatorio de paridade ST em `exportacoes/paridade_st.json` e `exportacoes/paridade_st.md` agora distingue explicitamente o shape bruto do shape canonico de homologacao.
- SQLs aceitam placeholders `{{FONTE_*}}`, resolvidos por override persistido, variavel `ORACLE_FONTE_*` ou valor padrao.
- O mapeamento raiz Oracle analisa a SQL ja renderizada com os placeholders resolvidos, reduzindo falso mapeamento de fontes.
- `data_limite` vazia significa ausencia total de filtro temporal nas consultas Oracle; quando preenchida, funciona como data maxima de processamento.
