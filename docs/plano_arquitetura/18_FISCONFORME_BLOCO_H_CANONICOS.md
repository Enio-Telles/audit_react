# Fisconforme e Bloco H na trilha canônica

## Objetivo

Este passo fecha dois pontos fora do pipeline analítico central:

- o cache materializado do Fisconforme (`dados_cadastrais` e `malhas`)
- a materialização canônica do artefato legado `bloco_h`

---

## O que foi implementado

### 1. Fisconforme com leitura e escrita adaptativas

Arquivo:

- `backend/routers/fisconforme.py`

Mudanças:

- `dados_cadastrais` e `malhas` passam a usar o catálogo canônico para resolver caminho;
- leitura do cache agora aceita Parquet ou Delta;
- gravação do cache passa a registrar metadata e usar `registrar_dataset()`;
- `cache/stats` agora informa também o formato materializado;
- `DELETE /cache/{cnpj}` remove arquivo Parquet, diretório Delta e metadata sidecar.

### 2. Wrapper para `bloco_h`

Arquivo:

- `src/transformacao/bloco_h.py`

Mudanças:

- localiza o artefato legado em caminhos conhecidos;
- registra o dataset canônico `bloco_h` com metadata.

---

## Ganho prático

Com isso, a trilha canônica agora avança também sobre:

- cache de fiscalização/Fisconforme
- inventário fiscal legado consumido por EFD e Análise

---

## Teste adicionado

Arquivo:

- `tests/test_bloco_h_materialization.py`

Cobertura:

- valida que a materialização de `bloco_h` registra o dataset canônico esperado.

---

## Próximo passo recomendado

Agora o próximo passo de maior valor é começar a trocar pontos de leitura específicos ainda presos em nomes legados por chamadas mais diretas ao `dataset_registry`, reduzindo duplicação de resolução de caminho dentro dos routers.
