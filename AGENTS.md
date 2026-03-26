# AGENTS.md — Instruções para agentes no repositório audit_react

Este arquivo define como agentes de IA devem atuar no projeto `audit_react`. Leia-o integralmente antes de qualquer alteração no código.

---

## Missão

Você está trabalhando em um sistema de auditoria fiscal para a SEFIN que combina:

- interface web analítica em React;
- API e motor de processamento em Python;
- pipeline modular de geração de tabelas analíticas em Parquet.

Seu objetivo não é apenas "fazer a aplicação funcionar visualmente". Seu objetivo é **reduzir a distância entre a arquitetura proposta e a execução real**, com foco em:

- confiabilidade operacional;
- rastreabilidade fiscal;
- integridade do pipeline;
- clareza de manutenção;
- mínima divergência entre frontend, backend e contratos de tabelas.

---

## Regra central

**Não implemente funcionalidades "bonitas porém falsas".**

Evite:

- telas com comportamento meramente demonstrativo quando existe fluxo real para integrar;
- endpoints que retornam sucesso fictício;
- tabelas vazias gravadas silenciosamente para simular pipeline concluído;
- dados mockados permanentes em páginas que deveriam refletir o backend.

Se algo ainda não estiver implementado, deixe isso explícito no código, no retorno da API e na descrição do commit. Use `# TODO:` com contexto claro.

---

## Prioridades do projeto

Quando houver dúvida sobre o que melhorar primeiro, siga esta ordem:

1. Fechar integração real entre frontend e backend.
2. Substituir mocks e placeholders por fluxo funcional mínimo.
3. Fortalecer contratos e rastreabilidade das tabelas.
4. Melhorar regras fiscais e consistência analítica.
5. Refinar UX e acabamento visual.

---

## Visão geral da arquitetura

### Frontend — `client/`

| Tecnologia | Uso |
|---|---|
| React 19 + TypeScript | Componentes e lógica |
| Tailwind CSS 4 + shadcn/ui | Estilização e componentes UI |
| Wouter | Roteamento client-side |
| Recharts | Gráficos e visualizações |
| Lucide React | Ícones |

Responsável por fluxo de navegação, telas de extração, consulta, agregação, conversão e estoque, visualização de tabelas e interação com a API.

### Gateway / servidor web — `server/index.ts`

Responsável por servir o frontend compilado e atuar como ponto de integração HTTP. Não trate este arquivo como local de lógica fiscal. Se precisar integrar frontend e FastAPI, faça isso de forma explícita e consistente.

### Backend analítico — `server/python/`

| Tecnologia | Uso |
|---|---|
| FastAPI | API REST |
| Polars | Processamento de DataFrames |
| Parquet | Armazenamento de tabelas |
| Oracle DB (oracledb) | Extração de dados fiscais |

Responsável por endpoints da API, orquestração do pipeline, leitura/escrita de parquets, processamento de dados fiscais e geração de tabelas derivadas.

---

## Estrutura de diretórios — uma pasta por tabela

O `audit_engine` segue o princípio de **uma pasta por tabela**. Cada tabela do pipeline possui seu próprio diretório contendo o módulo gerador, o contrato de schema e os testes correspondentes. Essa estrutura garante isolamento, rastreabilidade e facilidade de manutenção.

### Estrutura-alvo

```
server/python/audit_engine/
├── __init__.py                      # Registra todos os módulos
├── pipeline/
│   ├── __init__.py
│   └── orquestrador.py              # Orquestração e ordem topológica
├── contratos/
│   ├── __init__.py
│   └── base.py                      # Classes base: ContratoTabela, ColunaSchema, TipoColuna
├── tabelas/
│   ├── __init__.py                  # Importa e registra todas as tabelas
│   ├── produtos_unidades/
│   │   ├── __init__.py
│   │   ├── contrato.py              # ContratoTabela com schema e dependências
│   │   ├── gerador.py               # Função geradora com @registrar_gerador
│   │   └── tests/
│   │       └── test_produtos_unidades.py
│   ├── produtos/
│   │   ├── __init__.py
│   │   ├── contrato.py
│   │   ├── gerador.py
│   │   └── tests/
│   │       └── test_produtos.py
│   ├── produtos_agrupados/
│   │   ├── __init__.py
│   │   ├── contrato.py
│   │   ├── gerador.py
│   │   └── tests/
│   │       └── test_produtos_agrupados.py
│   ├── id_agrupados/
│   │   ├── __init__.py
│   │   ├── contrato.py
│   │   ├── gerador.py
│   │   └── tests/
│   │       └── test_id_agrupados.py
│   ├── fatores_conversao/
│   │   ├── __init__.py
│   │   ├── contrato.py
│   │   ├── gerador.py
│   │   └── tests/
│   │       └── test_fatores_conversao.py
│   ├── produtos_final/
│   │   ├── __init__.py
│   │   ├── contrato.py
│   │   ├── gerador.py
│   │   └── tests/
│   │       └── test_produtos_final.py
│   ├── nfe_entrada/
│   │   ├── __init__.py
│   │   ├── contrato.py
│   │   ├── gerador.py
│   │   └── tests/
│   │       └── test_nfe_entrada.py
│   ├── mov_estoque/
│   │   ├── __init__.py
│   │   ├── contrato.py
│   │   ├── gerador.py
│   │   └── tests/
│   │       └── test_mov_estoque.py
│   ├── aba_mensal/
│   │   ├── __init__.py
│   │   ├── contrato.py
│   │   ├── gerador.py
│   │   └── tests/
│   │       └── test_aba_mensal.py
│   ├── aba_anual/
│   │   ├── __init__.py
│   │   ├── contrato.py
│   │   ├── gerador.py
│   │   └── tests/
│   │       └── test_aba_anual.py
│   └── produtos_selecionados/
│       ├── __init__.py
│       ├── contrato.py
│       ├── gerador.py
│       └── tests/
│           └── test_produtos_selecionados.py
├── utils/
│   ├── __init__.py
│   └── parquet_io.py                # Leitura, escrita, validação e exportação
└── api.py                           # Endpoints FastAPI
```

### Regras da estrutura por tabela

Cada pasta de tabela **deve** conter:

| Arquivo | Responsabilidade |
|---|---|
| `contrato.py` | Instância de `ContratoTabela` com nome, schema (colunas + tipos), dependências, módulo, função e arquivo de saída. Deve chamar `registrar_contrato()` ao ser importado. |
| `gerador.py` | Função decorada com `@registrar_gerador("nome_tabela")`. Recebe `diretorio_cnpj`, `diretorio_parquets`, `arquivo_saida` e `contrato`. Retorna número de registros gerados. |
| `tests/` | Testes unitários que validam o gerador com dados sintéticos mínimos. |

Ao criar uma nova tabela, siga exatamente esse padrão. Ao modificar uma tabela existente, mantenha a alteração contida dentro da pasta correspondente. Nunca espalhe lógica de uma tabela por múltiplos diretórios.

---

## Pipeline de tabelas — ordem topológica

O pipeline gera 11 tabelas analíticas. O orquestrador resolve a ordem de execução automaticamente via dependências, mas a sequência esperada é:

```
produtos_unidades ──► produtos ──► produtos_agrupados ──► fatores_conversao
                                         │                       │
                                         ▼                       ▼
                                   id_agrupados            produtos_final
                                                                  │
                                                                  ▼
                                                            nfe_entrada
                                                                  │
                                                                  ▼
                                                            mov_estoque
                                                                  │
                                                                  ▼
                                                            aba_mensal
                                                                  │
                                                                  ▼
                                                            aba_anual

                                   produtos_selecionados (derivado de produtos_final)
```

### Referência rápida de dependências

| Tabela | Depende de | Gera |
|---|---|---|
| `produtos_unidades` | (nenhuma — dados extraídos do Oracle) | `produtos_unidades.parquet` |
| `produtos` | `produtos_unidades` | `produtos.parquet` |
| `produtos_agrupados` | `produtos` | `produtos_agrupados.parquet` |
| `id_agrupados` | `produtos_agrupados` | `id_agrupados.parquet` |
| `fatores_conversao` | `produtos_agrupados` | `fatores_conversao.parquet` |
| `produtos_final` | `produtos_agrupados`, `fatores_conversao` | `produtos_final.parquet` |
| `nfe_entrada` | `produtos_final` | `nfe_entrada.parquet` |
| `mov_estoque` | `nfe_entrada`, `produtos_final` | `mov_estoque.parquet` |
| `aba_mensal` | `mov_estoque` | `aba_mensal.parquet` |
| `aba_anual` | `aba_mensal` | `aba_anual.parquet` |
| `produtos_selecionados` | `produtos_final` | `produtos_selecionados.parquet` |

### Reprocessamento em cascata

Quando o usuário edita manualmente um fator de conversão ou uma agregação, todas as tabelas dependentes devem ser reprocessadas. O orquestrador expõe `reprocessar_a_partir_de(tabela_editada)` que resolve os dependentes transitivos e os reexecuta na ordem correta. Nunca reprocesse tabelas isoladamente sem considerar o grafo de dependências.

---

## Fluxo de dados por CNPJ

Os dados são organizados por CNPJ em diretórios independentes:

```
/storage/CNPJ/
└── {cnpj_limpo}/
    ├── extraidos/              # Dados brutos extraídos do Oracle
    │   ├── nfe_compra.parquet
    │   ├── nfe_venda.parquet
    │   ├── reg0200.parquet
    │   ├── reg0220.parquet
    │   └── bloco_h.parquet
    ├── parquets/               # Tabelas geradas pelo pipeline
    │   ├── produtos_unidades.parquet
    │   ├── produtos.parquet
    │   ├── ...
    │   └── aba_anual.parquet
    ├── edicoes/                # Edições manuais do auditor
    │   ├── agregacao.json      # Mapeamento manual De/Para
    │   └── fatores.json        # Fatores editados manualmente
    └── exportacoes/            # Arquivos exportados (xlsx, csv)
```

Cada CNPJ é uma unidade isolada. Nunca misture dados entre CNPJs. As análises devem focar em um CNPJ por vez, mas o sistema deve permitir navegar entre CNPJs previamente auditados.

---

## Endpoints da API

A API em `server/python/api.py` expõe os seguintes grupos de endpoints:

| Grupo | Método | Endpoint | Descrição |
|---|---|---|---|
| Sistema | GET | `/api/health` | Health check |
| Sistema | GET | `/api/contratos` | Lista contratos de tabelas |
| Sistema | GET | `/api/contratos/ordem` | Ordem topológica |
| Pipeline | POST | `/api/pipeline/executar` | Executa pipeline completo ou parcial |
| Pipeline | POST | `/api/pipeline/reprocessar` | Reprocessa dependentes de tabela editada |
| Pipeline | GET | `/api/pipeline/status/{cnpj}` | Verifica integridade dos parquets |
| Tabelas | GET | `/api/tabelas/{cnpj}` | Lista parquets disponíveis |
| Tabelas | GET | `/api/tabelas/{cnpj}/{nome}` | Lê tabela com paginação e filtros |
| Agregação | POST | `/api/agregacao/agregar` | Agrupa produtos |
| Agregação | POST | `/api/agregacao/desagregar` | Remove grupo |
| Conversão | PUT | `/api/conversao/fator` | Edita fator de conversão |
| Conversão | POST | `/api/conversao/recalcular` | Recalcula tabelas derivadas |
| Exportação | GET | `/api/exportar/{cnpj}/{nome}` | Exporta tabela (xlsx/csv/parquet) |

Ao adicionar novos endpoints, siga o padrão existente: prefixo `/api/`, parâmetro `cnpj` quando aplicável, retorno JSON com campo `status`.

---

## Rotas do frontend

| Rota | Componente | Descrição |
|---|---|---|
| `/` | `Dashboard.tsx` | Visão geral, KPIs, atalhos |
| `/extracao` | `Extracao.tsx` | Seleção de CNPJ, consultas SQL, pipeline |
| `/consulta` | `Consulta.tsx` | Browser de tabelas Parquet |
| `/agregacao` | `Agregacao.tsx` | Agrupamento De/Para |
| `/conversao` | `Conversao.tsx` | Fatores de conversão |
| `/estoque` | `Estoque.tsx` | Movimentação, mensal, anual |
| `/configuracoes` | `Configuracoes.tsx` | Conexão Oracle, preferências |

---

## Filosofia de design da interface

Tema: **Institutional Precision / Swiss Design Fiscal**

### Layout

Usar grade rígida e alinhamento matemático. Sidebar escura fixa à esquerda (`#0f172a`), workspace limpo e claro (`#f8fafc`). Tabelas devem priorizar largura útil e leitura rápida. Evitar layouts centralizados genéricos.

### Cores

Usar cor de forma funcional, nunca decorativa. Azul institucional (`#1e40af`) para ação e seleção. Verde para sucesso. Âmbar para pendência ou atenção. Vermelho para erro. Nunca usar gradientes decorativos ou roxo.

### Tipografia

Títulos e interface: **DM Sans**. Valores numéricos, identificadores, CNPJ, NCM, CEST, nomes técnicos e códigos: **JetBrains Mono**. Manter hierarquia tipográfica consistente com peso e tamanho, não com cor.

### Interação

Transições rápidas e discretas (150-200ms). Evitar animações chamativas, elásticas ou decorativas. Feedback deve ser funcional: toast para confirmações, badge para status, skeleton para carregamento.

### Densidade

Priorizar alta densidade informacional com legibilidade. Telas fiscais devem mostrar contexto, status e filtros com objetividade. Espaço em branco é ferramenta de organização, não de decoração.

---

## Regras para backend Python

### Geradores de tabelas

Cada gerador deve:

1. Receber `diretorio_cnpj`, `diretorio_parquets`, `arquivo_saida` e `contrato` como parâmetros.
2. Verificar existência das dependências antes de processar.
3. Usar Polars para todo processamento de DataFrames — nunca Pandas em código novo.
4. Escrever o resultado em Parquet com compressão `zstd`.
5. Retornar o número de registros gerados (inteiro).
6. Quando não houver dados de entrada, criar DataFrame vazio com o schema do contrato — nunca omitir o arquivo de saída.
7. Logar com `logger.info` o nome da tabela e a contagem de registros.
8. Decompor a lógica em **funções auxiliares com nomes em português autoexplicativos** (ver seção abaixo).
9. Cada operação Polars relevante deve ter um **comentário explicativo** em português descrevendo o que está sendo feito e por quê.

### Convenções Polars — funções em português com comentários explicativos

Toda lógica de construção de tabelas Polars — tanto nos geradores do pipeline quanto em qualquer tabela analítica derivada — deve ser decomposta em funções com **nomes em português que descrevam exatamente o que fazem**. O objetivo é que um auditor fiscal ou desenvolvedor que leia o código entenda a intenção de cada etapa sem precisar interpretar lógica Polars crua.

**Regras obrigatórias:**

1. Cada transformação significativa em um DataFrame deve ser extraída para uma função nomeada em português.
2. O nome da função deve ser um verbo no infinitivo que descreva a ação: `filtrar_`, `calcular_`, `agrupar_`, `consolidar_`, `classificar_`, `cruzar_`, `enriquecer_`, `validar_`, `totalizar_`, `separar_`.
3. Cada função deve ter uma docstring de uma linha explicando o propósito fiscal ou analítico.
4. Dentro da função, cada operação Polars (`.filter`, `.group_by`, `.join`, `.with_columns`, `.sort`, `.select`, `.rename`) deve ter um comentário inline explicando **o que** está sendo feito e **por que** é necessário no contexto fiscal.
5. Nunca encadear mais de 3 operações Polars sem um comentário intermediário.
6. Variáveis intermediárias devem ter nomes descritivos em português: `df_nfe_filtradas`, `df_com_fatores`, `df_estoque_consolidado`.

**Prefixos padrão para funções:**

| Prefixo | Uso | Exemplo |
|---|---|---|
| `filtrar_` | Remover linhas por critério | `filtrar_nfe_por_periodo` |
| `calcular_` | Derivar valores numéricos | `calcular_quantidade_na_unidade_referencia` |
| `agrupar_` | Agregar linhas por chave | `agrupar_produtos_por_ncm` |
| `consolidar_` | Unir múltiplas fontes | `consolidar_entradas_e_saidas` |
| `classificar_` | Atribuir categorias | `classificar_omissao_por_tipo` |
| `cruzar_` | Join entre tabelas | `cruzar_nfe_com_fatores` |
| `enriquecer_` | Adicionar colunas derivadas | `enriquecer_com_descricao_produto` |
| `validar_` | Verificar integridade | `validar_schema_contra_contrato` |
| `totalizar_` | Somar/acumular valores | `totalizar_estoque_mensal` |
| `separar_` | Dividir DataFrame | `separar_compras_e_vendas` |

**Exemplo correto de gerador:**

```python
def gerar_mov_estoque(
    diretorio_cnpj: str,
    diretorio_parquets: str,
    arquivo_saida: str,
    contrato: ContratoTabela,
) -> int:
    """Gera tabela de movimentação de estoque consolidando entradas, saídas e inventário.

    Etapas:
    1. Carregar NFe de entrada e produtos_final
    2. Filtrar período de auditoria
    3. Calcular quantidades na unidade de referência
    4. Consolidar entradas e saídas por produto/mês
    5. Classificar omissões
    """
    # 1. Carregar dependências
    df_nfe = carregar_dependencia(diretorio_parquets, "nfe_entrada")
    df_produtos = carregar_dependencia(diretorio_parquets, "produtos_final")

    # 2. Filtrar e transformar
    df_nfe_periodo = filtrar_nfe_por_periodo(df_nfe, data_inicio, data_fim)
    df_com_quantidades = calcular_quantidade_na_unidade_referencia(df_nfe_periodo, df_produtos)

    # 3. Consolidar
    df_consolidado = consolidar_entradas_e_saidas(df_com_quantidades)
    df_final = classificar_omissao_por_tipo(df_consolidado)

    # 4. Gravar resultado
    escrever_parquet(df_final, arquivo_saida)
    return len(df_final)


def filtrar_nfe_por_periodo(
    df_nfe: pl.DataFrame,
    data_inicio: str,
    data_fim: str,
) -> pl.DataFrame:
    """Filtra NFe de entrada dentro do período de auditoria."""
    return df_nfe.filter(
        # Manter apenas NFe com data de emissão dentro do período fiscalizado
        (pl.col("data_emissao") >= data_inicio)
        & (pl.col("data_emissao") <= data_fim)
    )


def calcular_quantidade_na_unidade_referencia(
    df_nfe: pl.DataFrame,
    df_produtos: pl.DataFrame,
) -> pl.DataFrame:
    """Converte quantidades de compra para a unidade de referência usando fatores."""
    # Cruzar NFe com fatores de conversão pelo id_agrupado
    df_cruzado = df_nfe.join(
        df_produtos.select(["id_agrupado", "fator_compra_ref", "unid_ref"]),
        on="id_agrupado",
        how="left",
    )
    # Aplicar fator: quantidade_original * fator = quantidade na unidade de referência
    return df_cruzado.with_columns(
        (pl.col("quantidade") * pl.col("fator_compra_ref")).alias("qtd_ref")
    )


def consolidar_entradas_e_saidas(
    df: pl.DataFrame,
) -> pl.DataFrame:
    """Agrupa movimentações por produto e mês, totalizando entradas e saídas."""
    return df.group_by(["id_agrupado", "ano_mes"]).agg(
        # Somar quantidades de entrada no período
        pl.col("qtd_ref").filter(pl.col("tipo") == "entrada").sum().alias("total_entrada"),
        # Somar quantidades de saída no período
        pl.col("qtd_ref").filter(pl.col("tipo") == "saida").sum().alias("total_saida"),
    )
```

**Exemplo incorreto (não aceitar):**

```python
# ERRADO: funções em inglês, sem comentários, lógica encadeada
def process(df):
    return df.filter(pl.col("x") > 0).join(df2, on="k").group_by("g").agg(
        pl.col("v").sum()
    ).sort("g").with_columns((pl.col("v") * 1.1).alias("adj"))
```

### Edições manuais

As edições do auditor (agregação, fatores) são salvas em JSON dentro de `{cnpj}/edicoes/`. Os geradores devem carregar e aplicar essas edições durante o processamento. Edições manuais sempre têm prioridade sobre cálculos automáticos.

### Tratamento de erros

Nunca silenciar exceções. Erros devem propagar até o orquestrador, que os registra no `ResultadoPipeline`. O pipeline continua com as próximas tabelas que não dependem da tabela com erro.

---

## Regras para frontend React

### Componentes

Usar shadcn/ui como base. Importar de `@/components/ui/*`. Não duplicar componentes que já existem no template. Compor com Tailwind utilities, evitar CSS customizado.

### Hooks de API

Toda comunicação com o backend deve passar pelos hooks em `client/src/hooks/useAuditApi.ts`. Nunca fazer `fetch` direto nos componentes de página. Os hooks gerenciam estado de loading, error e data.

### Tipos

Os tipos compartilhados estão em `client/src/types/audit.ts`. Manter sincronizados com os contratos Python. Quando um contrato mudar no backend, o tipo correspondente no frontend deve ser atualizado no mesmo commit.

### Estado

Não usar estado global complexo. Cada página gerencia seu próprio estado via hooks. O CNPJ ativo é passado via contexto ou parâmetro de rota.

---

## Convenções de código

### Nomeação

| Elemento | Convenção | Exemplo |
|---|---|---|
| Tabela do pipeline | snake_case | `produtos_agrupados` |
| Arquivo Parquet | snake_case + `.parquet` | `fatores_conversao.parquet` |
| Pasta de tabela | snake_case | `tabelas/mov_estoque/` |
| Endpoint API | snake_case com `/api/` | `/api/pipeline/executar` |
| Componente React | PascalCase | `Agregacao.tsx` |
| Hook React | camelCase com `use` | `useAuditApi` |
| Variável Python | snake_case em português | `diretorio_cnpj`, `df_nfe_filtradas` |
| Função Python (gerador) | verbo_infinitivo + contexto em português | `filtrar_nfe_por_periodo`, `calcular_quantidade_na_unidade_referencia` |
| Variável TypeScript | camelCase | `filtroColuna` |

### Commits

Usar prefixos convencionais: `feat:`, `fix:`, `refactor:`, `docs:`, `test:`. Incluir o nome da tabela ou módulo afetado quando aplicável. Exemplo: `feat(fatores_conversao): implementar leitura do Reg0220`.

### Documentação

Todo gerador deve ter docstring explicando o processo em etapas numeradas. Todo endpoint deve ter docstring de uma linha. Comentários `# TODO:` devem incluir contexto suficiente para outro desenvolvedor entender o que falta.

### Idioma do código Python

Todo código Python do `audit_engine` deve usar **português** para nomes de funções, variáveis, parâmetros, docstrings e comentários. A única exceção são nomes de métodos Polars (`.filter`, `.join`, `.group_by`, etc.) e palavras reservadas do Python. Essa regra se aplica a geradores, funções auxiliares, utilitários e testes.

---

## O que NÃO fazer

### No backend

- Nunca usar Pandas quando Polars está disponível.
- Nunca gravar parquet sem respeitar o schema do contrato.
- Nunca pular a verificação de dependências no gerador.
- Nunca misturar dados de CNPJs diferentes no mesmo processamento.
- Nunca retornar `{"status": "ok"}` de um endpoint que não executou nada.
- Nunca colocar lógica de tabela fora da pasta correspondente em `tabelas/`.
- Nunca nomear funções ou variáveis em inglês — todo código Python deve usar português.
- Nunca encadear mais de 3 operações Polars sem comentário explicativo.
- Nunca criar funções genéricas como `process()`, `transform()` ou `run()` — usar nomes descritivos em português.

### No frontend

- Nunca fazer `fetch` direto em componentes — usar hooks de `useAuditApi.ts`.
- Nunca criar estado global para dados que são específicos de uma página.
- Nunca usar cores fora da paleta definida (azul, verde, âmbar, vermelho).
- Nunca usar animações elásticas ou decorativas.
- Nunca mostrar dados mockados como se fossem reais sem indicação visual clara.

### Na estrutura

- Nunca criar arquivos de tabela fora da pasta `tabelas/{nome_tabela}/`.
- Nunca registrar um contrato em local diferente de `tabelas/{nome}/contrato.py`.
- Nunca registrar um gerador em local diferente de `tabelas/{nome}/gerador.py`.
- Nunca alterar a ordem topológica manualmente — ela é derivada das dependências.

---

## Arquivos-chave por tipo de tarefa

| Tarefa | Arquivos a consultar |
|---|---|
| Adicionar nova tabela ao pipeline | `contratos/base.py`, qualquer `tabelas/*/contrato.py` como referência, `pipeline/orquestrador.py`, `tabelas/__init__.py` |
| Modificar schema de tabela existente | `tabelas/{nome}/contrato.py`, `tabelas/{nome}/gerador.py`, `client/src/types/audit.ts` |
| Adicionar endpoint à API | `server/python/api.py`, `client/src/hooks/useAuditApi.ts` |
| Alterar página do frontend | `client/src/pages/{Pagina}.tsx`, `client/src/components/layout/DashboardLayout.tsx` |
| Corrigir lógica de processamento | `tabelas/{nome}/gerador.py`, `utils/parquet_io.py` |
| Alterar fluxo do pipeline | `pipeline/orquestrador.py`, contratos das tabelas envolvidas |
| Adicionar rota no frontend | `client/src/App.tsx`, `client/src/components/layout/DashboardLayout.tsx` |

---

## Estado atual e pendências

A estrutura atual em `server/python/audit_engine/modulos/` ainda agrupa múltiplas tabelas por arquivo (`produtos.py`, `agregacao.py`, `conversao.py`, `estoque.py`). A migração para a estrutura `tabelas/{nome}/` descrita acima é uma tarefa prioritária. Ao realizar essa migração:

1. Criar a pasta `tabelas/` com subpastas para cada uma das 11 tabelas.
2. Extrair cada `@registrar_gerador` para o `gerador.py` correspondente.
3. Extrair cada `registrar_contrato()` de `contratos/tabelas.py` para o `contrato.py` correspondente.
4. Manter `contratos/base.py` apenas com as classes base (`ContratoTabela`, `ColunaSchema`, `TipoColuna`, funções de registro e ordem topológica).
5. Atualizar `tabelas/__init__.py` para importar todos os contratos e geradores.
6. Verificar que o orquestrador continua funcionando sem alterações.
7. Criar testes mínimos para cada gerador.

Geradores que ainda precisam de implementação real (atualmente retornam DataFrame vazio):

| Gerador | Status | O que falta |
|---|---|---|
| `produtos_unidades` | Stub | Leitura de `extraidos/nfe_compra.parquet` + `nfe_venda.parquet` + `reg0200.parquet` |
| `nfe_entrada` | Stub | Leitura de NFe extraídas + aplicação de fatores de conversão |
| `mov_estoque` | Stub | Consolidação de entradas + saídas + inventário (Bloco H) |
| Endpoints de agregação | Stub | Integração real com `agregacao.json` e reprocessamento |
| Endpoints de conversão | Stub | Integração real com `fatores.json` e reprocessamento |
| Endpoints de exportação | Stub | Geração de Excel/CSV formatado |