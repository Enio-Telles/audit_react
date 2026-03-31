# Importação e Implementação de Abatimento ST na Aba Anual

O objetivo deste plano é importar a matriz de referências Sefin e integrá-la ao cálculo da `aba_anual` para reconhecer e aplicar as isenções ("abatimento") do ICMS sobre saídas desacobertadas para produtos sujeitos a Substituição Tributária (ST). 

A instrução orienta que "o mock isenta essa validação por hora"; ou seja, se a matriz oficial não estiver disponível/populada, o pipeline deve continuar validando o cálculo sem quebrar, tratando tudo como não-ST até a importação real. Adicionalmente, criaremos uma cobertura rigorosa no `pytest` para simular os cenários fiscais e prevenir regressões ("blindagem tributária").

## Proposta de Implementação

### 1. Importação da Matriz de Referências
A matriz `sitafe_produto_sefin_aux.parquet` será copiada do repositório legado para o diretório de dados estáticos do `audit_engine`:
- Destino sugerido: `server/python/audit_engine/dados/referencias/sitafe_produto_sefin_aux.parquet` (ou em `storage/referencias` dependendo da configuração root do projeto).

### 2. Modificação da Lógica na `aba_anual`
O processo de fechamento do ICMS na `aba_anual` (localizado em `server/python/audit_engine/tabelas/aba_anual/gerador.py`) será refatorado para suportar o cruzamento:

1. **Tentativa de Carga e "Mocking" Automático:** O orquestrador vai tentar carregar `sitafe_produto_sefin_aux.parquet`. Se o arquivo não existir ou se comportar diferente, o sistema irá aplicar um "mock de isenção", atribuindo ST = Falso para todos. Isso permite liberar o cômputo na `aba_anual` sem quebrar o pipeline por falta da base do sistema central.
2. **Mapeamento de Vigência Anual:** A função filtrará os períodos de vigência do ano analisado na matriz (usando `it_da_inicio` e `it_da_final`), agrupando e flaggando se `it_co_sefin` (`id_agrupado`) esteve sob a regra ST no respectivo ano.
3. **Cruzamento (`Left Join`):** Será efetuado um `left join` da matriz cruzada sobre a `aba_anual` e consolidaremos a coluna bool `tem_st_ano`.
4. **Aplicação do Abatimento:** Para as linhas onde `tem_st_ano == True`, a base imponível ou o imposto resultante de `ICMS_saidas_desac` será forçado a `0.0`. O `ICMS_estoque_desac` não será afetado pelo ST (em conformidade com o legado técnico).

### 3. Blindagem Tributária no Pytest (`tabelas/aba_anual/tests/test_aba_anual.py`)
A suíte automatizada do Pytest será expandida/criada e conterá cenários determinísticos populados com `DataFrames` manuais para assegurar que não haja regressions:
- **Cenário 1: Abatimento Real ST.** Um produto que bate com a cruzada da matriz tem o cálculo zerado.
- **Cenário 2: Sem Abatimento.** Produto que não existe na matriz ou seu status não-ST mantém o imposto `ICMS_saidas_desac`, aplicando a alíquota interna sobre base `pms` ou `pme`.
- **Cenário 3: Mock/Fallback (Referência indisponível).** Teste isolado subindo uma base onde a matriz reference de ST não é fornecida e a API se preserva isentando ST.

---

## User Review Required

> [!WARNING]
> Posicionamento da `sitafe_produto_sefin_aux.parquet`
> Atualmente, estou planejando posicionar essa tabela em `C:\audit_react\server\python\audit_engine\dados\referencias\sitafe_produto_sefin_aux.parquet` para ser um mock referêncial disponível a todas as extrações CNPJ's. Você prefere que ela seja copiada para a pasta padrão apontada pelas env vars (ex: `STORAGE_BASE_DIR/referencias/`), ou o caminho interno `audit_engine/dados/...` atende melhor para este mock?

## Open Questions
- Ao carregar os dados anuais para ST, os campos na `mov_estoque` que designam o ID do produto para o join na matriz de ST é exclusivamente pelo `id_agrupado`, ou ainda usamos a nomenclatura legada `co_sefin_agr` nesses Parquets gerados?

## Passo-a-passo (Verification Plan)
1. Rodar `pytest` focado no teste automatizado isolado para `test_aba_anual.py` validando todos os cenários.
2. Conferir localmente os valores após o join e a zera do ST na pipeline inteira.
