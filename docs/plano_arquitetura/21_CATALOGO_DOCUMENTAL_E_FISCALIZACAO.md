# Catálogo documental expandido e Fiscalização no localizador

## Objetivo

Este passo fecha duas lacunas que ainda restavam:

- parte do domínio documental usava `dataset_id` ainda não registrados de fato no catálogo;
- o router de Fiscalização ainda resolvia caminhos manualmente para `dados_cadastrais` e `malhas`.

---

## O que foi implementado

### Catálogo expandido

Arquivo:

- `src/utilitarios/dataset_registry.py`

Entradas e aliases adicionados:

- `cte_base`
- `docs_info_complementar`
- `docs_contatos`
- aliases como `cte`, `info_complementar` e `email_nfe`

Além disso, os caminhos legados desses datasets passaram a fazer parte do fallback oficial do catálogo.

### Fiscalização com localizador central

Arquivo:

- `backend/routers/fiscal_fiscalizacao.py`

Mudança:

- `_dados_cadastrais_path()` agora usa `locate_dataset(cnpj, "dados_cadastrais")`
- `_malhas_path()` agora usa `locate_dataset(cnpj, "malhas")`

---

## Ganho prático

Com isso:

- o domínio documental passa a depender menos de fallback cego;
- Fiscalização entra no mesmo fluxo de resolução central já usado em EFD, Análise e Documentos;
- o catálogo canônico ganha aderência real aos datasets auxiliares que o backend já tenta consumir.

---

## Testes adicionados

- `tests/test_dataset_registry_documental_aliases.py`
- `tests/test_fiscalizacao_locator_integration.py`

Cobertura:

- aliases documentais normalizados para nomes canônicos;
- presença das definições documentais no catálogo;
- Fiscalização priorizando o localizador central.

---

## Próximo passo recomendado

Agora o próximo passo de maior valor é revisar os últimos pontos auxiliares do backend e consolidar também diagnósticos e inspeções genéricas em cima do catálogo, reduzindo ainda mais uso direto de paths manuais.
