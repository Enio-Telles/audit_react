# Resolução centralizada de datasets nos routers

## Objetivo

Este passo começa a retirar lógica duplicada de localização de arquivos diretamente dos routers fiscais e a empurrar essa responsabilidade para uma camada única apoiada no catálogo canônico.

---

## O que foi implementado

### Helper central

Arquivo:

- `backend/routers/fiscal_dataset_locator.py`

Função principal:

- `locate_dataset(cnpj, dataset_id, *fallbacks)`

Fluxo:

1. tenta resolver pelo `dataset_registry`;
2. se não achar, usa os fallbacks explícitos;
3. continua aceitando Parquet ou Delta.

### Routers atualizados

- `backend/routers/fiscal_efd.py`
- `backend/routers/fiscal_analise_v2.py`

Datasets já migrados para a resolução centralizada:

- `c170_xml`
- `c176_xml`
- `bloco_h`
- `mov_estoque`
- `aba_mensal`
- `aba_anual`
- `fatores_conversao`
- `produtos_agrupados`
- `produtos_final`

---

## Ganho prático

Isso reduz:

- duplicação de regras de caminho;
- divergência entre nome lógico e localização física;
- acoplamento dos routers a estruturas legadas de pasta.

Ao mesmo tempo, mantém fallback seguro para não quebrar o comportamento atual.

---

## Teste adicionado

Arquivo:

- `tests/test_fiscal_dataset_locator.py`

Cobertura:

- priorização do catálogo;
- uso de fallback quando o catálogo ainda não localiza o dataset.

---

## Próximo passo recomendado

Continuar a mesma estratégia nos routers restantes, principalmente:

- `fiscal_documentos.py`
- pontos auxiliares do backend que ainda resolvem caminhos manualmente

Depois disso, a maior parte da navegação fiscal nova passa a depender de resolução central única.
