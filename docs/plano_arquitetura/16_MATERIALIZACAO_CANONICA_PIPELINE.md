# Materialização canônica do pipeline

## Objetivo

Este passo liga o catálogo canônico de datasets aos pontos reais de materialização do pipeline, para que a produção dos artefatos mais importantes da primeira onda deixe de depender apenas de nomes de arquivo espalhados.

---

## O que foi ligado

### 1. `tb_documentos`

Arquivo:

- `src/transformacao/tabelas_base/tabela_documentos.py`

Mudança:

- deixou de salvar só por nome de arquivo local;
- passou a registrar explicitamente o dataset canônico `tb_documentos`.

### 2. `mov_estoque`

Arquivo:

- `src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py`

Mudança:

- leitura de dependências via catálogo canônico (`produtos_final`, `fatores_conversao`);
- gravação explícita do dataset canônico `mov_estoque`.

### 3. `aba_mensal`

Arquivo:

- `src/transformacao/calculos_mensais_pkg/calculos_mensais.py`

Mudança:

- leitura do dataset `mov_estoque` via catálogo;
- gravação explícita do dataset canônico `aba_mensal`.

---

## Ganho prático

Com isso, a primeira onda passou a ter uma trilha mais consistente:

- o pipeline produz datasets com nome lógico conhecido;
- o catálogo decide o caminho real;
- o formato Parquet/Delta é aplicado na camada central;
- metadata sidecar acompanha a materialização.

---

## Testes adicionados

Arquivo:

- `tests/test_dataset_registry_pipeline_paths.py`

Cobertura:

- caminho canônico de `tb_documentos`;
- caminho canônico de `aba_mensal`;
- caminho canônico de `malhas`.

---

## Próximo passo recomendado

Agora o próximo passo de maior valor é continuar a mesma integração para os demais artefatos relevantes da primeira onda e dos domínios fiscais auxiliares, especialmente:

- `dados_cadastrais`
- `malhas`
- `bloco_h`
- `c170_xml`
- `produtos_final`
- `fatores_conversao`

Isso fecha o ciclo entre extração, transformação, catálogo canônico e consumo no backend novo.
