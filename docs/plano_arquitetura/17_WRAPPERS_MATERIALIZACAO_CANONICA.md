# Wrappers de materialização canônica

## Objetivo

Este passo aproveita os módulos legados já consolidados e transforma os wrappers de compatibilidade em pontos de materialização canônica.

---

## O que foi implementado

### Wrappers atualizados

- `src/transformacao/c170_xml.py`
- `src/transformacao/c176_xml.py`
- `src/transformacao/fatores_conversao.py`
- `src/transformacao/produtos_final_v2.py`

### Estratégia

Cada wrapper agora segue este fluxo:

1. chama a implementação legada real;
2. lê o artefato legado gerado;
3. registra esse resultado no catálogo canônico com metadata;
4. deixa a escolha Parquet ou Delta na camada central do `dataset_registry`.

---

## Ganho prático

Isso reduz risco porque evita refatoração profunda nos módulos grandes, mas já entrega consistência de saída para datasets importantes como:

- `c170_xml`
- `c176_xml`
- `fatores_conversao`
- `produtos_agrupados`
- `produtos_final`

---

## Testes adicionados

Arquivo:

- `tests/test_canonical_materialization_wrappers.py`

Cobertura:

- wrapper de `c170_xml` registra o dataset canônico esperado;
- wrapper de `produtos_final_v2` registra `produtos_agrupados` e `produtos_final`.

---

## Próximo passo recomendado

Agora o próximo passo de maior valor é aplicar a mesma abordagem a pontos fora do pipeline analítico direto, especialmente:

- cache/materialização de `dados_cadastrais`
- cache/materialização de `malhas`
- artefatos auxiliares de EFD como `bloco_h`

Isso fecha ainda mais a trilha entre extração, cache, catálogo canônico e leitura pelo backend novo.
