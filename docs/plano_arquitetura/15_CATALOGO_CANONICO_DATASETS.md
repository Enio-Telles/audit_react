# Catálogo canônico de datasets

## Objetivo

Este passo cria uma camada de nomes canônicos para reduzir divergência entre:

- nomes legados;
- nomes usados no backend fiscal novo;
- nomes da primeira onda Delta;
- caminhos materializados em Parquet ou Delta.

---

## O que foi implementado

### 1. Alias -> nome canônico

O `dataset_registry` agora normaliza aliases como:

- `cadastral` -> `dados_cadastrais`
- `movimentacao_estoque` -> `mov_estoque`
- `efd_bloco_h` -> `bloco_h`

Isso permite que componentes antigos e novos apontem para o mesmo dataset lógico.

### 2. Catálogo expandido da primeira onda

Entraram no catálogo canônico datasets materializados como:

- `tb_documentos`
- `dados_cadastrais`
- `malhas`
- `c170_xml`
- `c176_xml`
- `bloco_h`
- `mov_estoque`
- `aba_mensal`
- `aba_anual`
- `fatores_conversao`
- `produtos_agrupados`
- `produtos_final`

### 3. Resolução Parquet ou Delta

O catálogo agora:

- gera candidatos canônicos;
- expande automaticamente variantes `.parquet` e diretório Delta;
- faz fallback para caminhos legados conhecidos;
- registra metadata sidecar para arquivos ou diretórios.

### 4. Observabilidade do catálogo

O endpoint `/api/observabilidade/status` agora expõe um resumo do catálogo de datasets, incluindo aliases e datasets materializados.

---

## Ganho prático

Com isso, o projeto passa a ter uma camada central para tratar nomes como equivalentes sem espalhar regras em cada router ou serviço.

Isso reduz o risco de divergência entre:

- `dados_cadastrais` vs `cadastral`
- `bloco_h` vs `efd_bloco_h`
- `mov_estoque` vs `movimentacao_estoque`

---

## Próximo passo recomendado

Agora que a resolução lógica dos nomes ficou centralizada, o próximo passo mais valioso é integrar esse catálogo diretamente aos pontos de materialização do pipeline, para que cada etapa registre explicitamente o dataset canônico que produziu.
