# Documentos Fiscais com resolução centralizada

## Objetivo

Este passo leva a mesma estratégia de resolução centralizada para o domínio de Documentos Fiscais.

---

## O que foi implementado

Arquivo principal:

- `backend/routers/fiscal_documentos.py`

Mudança estrutural:

- a função `_find_document_path()` passou a consultar primeiro o helper `locate_dataset()`;
- só depois disso mantém a busca por padrões e caminhos legados.

Datasets documentais impactados:

- `nfe_base`
- `nfce_base`
- `cte_base`
- `docs_info_complementar`
- `docs_contatos`

---

## Ganho prático

Isso faz o domínio documental se alinhar ao mesmo fluxo usado em EFD e Análise:

1. tenta nome lógico/catálogo;
2. cai para fallback manual apenas quando necessário.

Com isso, a duplicação de regras de caminho diminui e a migração gradual para datasets canônicos fica mais consistente.

---

## Teste adicionado

Arquivo:

- `tests/test_fiscal_documentos_locator_integration.py`

Cobertura:

- valida que a busca de NF-e prioriza o localizador central.

---

## Próximo passo recomendado

Agora o próximo passo mais valioso é revisar pontos auxiliares restantes do backend e concentrar a localização manual remanescente em helpers únicos, reduzindo ainda mais regras espalhadas.
