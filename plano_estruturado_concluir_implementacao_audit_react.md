# Plano Estruturado para Concluir a Implementação do `audit_react`

## 1. Objetivo

Concluir a implementação do sistema de auditoria fiscal com foco em:

- integração real entre frontend, gateway Node e backend Python;
- execução funcional do pipeline do `audit_engine`;
- geração modular das tabelas analíticas;
- rastreabilidade dos produtos entre fontes fiscais;
- estabilidade operacional da aplicação;
- preparação da `main` para evolução contínua sem retrabalho estrutural.

---

## 2. Situação atual resumida

O projeto já possui parte da base estrutural, mas ainda há lacunas que impedem o fluxo ponta a ponta.

### 2.1 Pontos que já estão encaminhados
- estrutura React no cliente;
- servidor Node/Express;
- backend Python/FastAPI;
- pasta `audit_engine` com contratos, orquestração e geradores iniciais;
- utilitário `polars_utils.py` já presente;
- definição geral de que cada tabela deve ser gerada de forma modular.

### 2.2 Pontos que ainda impedem a conclusão
- proxy `/api` e integração frontend/backend ainda precisam estar definitivamente consolidados na versão final;
- tela de Extração ainda precisa operar integralmente sobre o pipeline real;
- backend Python precisa estar totalmente preparado para bootstrap do `audit_engine`, persistência e execução por CNPJ;
- há geradores de tabelas ainda incompletos ou frágeis;
- a tabela `produtos_unidades` ainda precisa ser implementada de forma funcional;
- faltam testes mínimos de integração e validação do pipeline;
- faltam critérios claros de aceite por etapa.

---

## 3. Resultado final esperado

Ao final, o sistema deve permitir:

1. informar um CNPJ e parâmetros de execução no frontend;
2. acionar a API real pelo frontend;
3. iniciar e acompanhar a execução do pipeline no backend;
4. gerar tabelas analíticas em arquivos dedicados;
5. salvar saídas por CNPJ em estrutura organizada;
6. manter rastreabilidade dos produtos entre `NFe`, `NFCe`, `C170` e `Bloco H`;
7. expor status de processamento, erros e artefatos gerados;
8. permitir expansão segura para novas tabelas sem quebrar a arquitetura.

---

## 4. Princípios de implementação

### 4.1 Arquitetura
- **1 tabela = 1 pasta específica = 1 gerador principal**
- funções compartilhadas devem ficar em `auxiliares` ou `utils`
- contratos de schema devem existir por tabela
- leitura, transformação, validação e escrita devem ser separadas

### 4.2 Dados
- usar `Polars` preferencialmente
- priorizar `LazyFrame` quando possível
- padronizar tipos e nomes de colunas
- manter campos de origem para rastreabilidade

### 4.3 Pipeline
- o orquestrador deve chamar etapas independentes
- cada etapa deve registrar:
  - início
  - fim
  - quantidade de linhas
  - arquivos lidos
  - arquivos gerados
  - erros

### 4.4 Interface
- frontend nunca deve operar em modo mock como versão final
- status deve refletir execução real
- mensagens de erro devem ser úteis e objetivas

---

## 5. Escopo técnico a concluir

## 5.1 Camada 1 — Integração de execução ponta a ponta

### Objetivo
Garantir que o usuário consiga acionar a execução real do pipeline pela interface.

### Entregas
- ajustar `server/index.ts` para atuar como gateway `/api`;
- consolidar `vite.config.ts` com proxy de desenvolvimento;
- conectar `client/src/pages/Extracao.tsx` ao hook/serviço real;
- revisar tipagem de resposta do pipeline no frontend;
- validar tratamento de loading, sucesso e erro.

### Critérios de aceite
- ao iniciar extração, o frontend chama a API real;
- o backend responde sem erro de rota;
- a interface mostra progresso/status real;
- erros do backend chegam ao frontend com mensagem útil.

---

## 5.2 Camada 2 — Backend Python operacional

### Objetivo
Fazer o backend Python ser a camada estável de execução do pipeline.

### Entregas
- consolidar `server/python/api.py`;
- garantir import seguro do `audit_engine`;
- organizar diretórios por CNPJ;
- parametrizar diretórios-base;
- expor endpoints mínimos:
  - healthcheck
  - iniciar pipeline
  - consultar status
  - listar arquivos gerados

### Critérios de aceite
- backend sobe sem erro de import;
- pipeline é executado via endpoint;
- diretórios são criados automaticamente;
- arquivos de saída são persistidos corretamente.

---

## 5.3 Camada 3 — Orquestração do `audit_engine`

### Objetivo
Fazer o motor de auditoria executar de forma previsível e modular.

### Entregas
- revisar `pipeline/orquestrador.py`;
- definir sequência clara das tabelas;
- padronizar retorno de cada etapa;
- adicionar log estruturado por etapa;
- interromper execução apenas em erro crítico;
- registrar warnings para falhas não críticas.

### Critérios de aceite
- orquestrador executa etapas na ordem correta;
- cada etapa registra resultado;
- falhas localizadas são rastreáveis;
- execução final devolve resumo consolidado.

---

## 5.4 Camada 4 — Implementação real das tabelas

### Objetivo
Concluir os geradores das tabelas centrais do projeto.

### Prioridade de implementação
1. `produtos`
2. `produtos_unidades`
3. `produtos_agrupados`
4. `fatores_conversao`
5. demais tabelas derivadas dependentes

---

## 6. Implementação da tabela `produtos_unidades`

## 6.1 Objetivo funcional

Gerar a tabela `produtos_unidades.py` com os campos:

- `codigo`
- `descricao`
- `descr_compl`
- `tipo_item`
- `ncm`
- `cest`
- `gtin`
- `unid`
- `compras`
- `vendas`

Com base em:

- `NFe`
- `NFCe`
- `C170`
- `Bloco H`

---

## 6.2 Regras de negócio mínimas

### Compras
Identificar em `C170` quando:
- `ind_oper = 0`
- `cfop = co_cfop` da referência
- `operacao_mercantil = 'X'` em `referencias/cfop/cfop_bi.parquet`

Preço/base de compras:
- `vl_item`

### Vendas
Identificar em `NFe` e `NFCe` quando:
- `co_emitente = cnpj`
- `tipo_operacao = '1 - saida'`
- `co_cfop = co_cfop` da referência
- `operacao_mercantil = 'X'`

Preço/base de vendas:
- `prod_vprod + prod_vfrete + prod_vseg + prod_voutro - prod_vdesc`

---

## 6.3 Estrutura recomendada da pasta

```text
server/python/audit_engine/tabelas/produtos_unidades/
├── __init__.py
├── contrato.py
├── gerador.py
├── extratores.py
├── normalizacao.py
├── consolidacao.py
└── validacao.py
```

---

## 6.4 Funções recomendadas

### `extratores.py`
- `carregar_nfe(caminhos, cnpj)`
- `carregar_nfce(caminhos, cnpj)`
- `carregar_c170(caminhos)`
- `carregar_bloco_h(caminhos)`
- `carregar_referencia_cfop(caminho_cfop)`

### `normalizacao.py`
- `normalizar_campos_nfe(df)`
- `normalizar_campos_nfce(df)`
- `normalizar_campos_c170(df)`
- `normalizar_campos_bloco_h(df)`
- `padronizar_schema_produtos_unidades(df)`

### `consolidacao.py`
- `identificar_compras_c170(df_c170, df_cfop)`
- `identificar_vendas_nfe(df_nfe, df_cfop, cnpj)`
- `identificar_vendas_nfce(df_nfce, df_cfop, cnpj)`
- `consolidar_fontes_produtos_unidades(...)`
- `agrupar_produto_unidade(df)`

### `validacao.py`
- `validar_colunas_obrigatorias(df)`
- `validar_tipos(df)`
- `validar_unidades_vazias(df)`
- `validar_resultado_final(df)`

### `gerador.py`
- `gerar_produtos_unidades(contexto_execucao)`
- `salvar_produtos_unidades(df, destino)`

---

## 6.5 Critérios de aceite
- tabela é gerada sem mock;
- compras e vendas obedecem às regras;
- schema final corresponde ao contrato;
- linhas possuem origem rastreável;
- arquivo final é salvo em local padronizado.

---

## 7. Rastreabilidade dos produtos

## 7.1 Objetivo
Permitir rastrear um produto desde a origem até os agrupamentos e fatores de conversão.

## 7.2 Campos mínimos de rastreabilidade
Toda tabela derivada de produto deve preservar, quando aplicável:

- `fonte_origem`
- `arquivo_origem`
- `registro_origem`
- `chave_documento`
- `item_documento`
- `codigo_original`
- `descricao_original`
- `unidade_original`

## 7.3 Estratégia
- não sobrescrever campos de origem sem preservar cópia;
- criar colunas normalizadas em paralelo;
- manter chave técnica por linha;
- documentar regras de consolidação.

---

## 8. Estrutura de pastas recomendada para o motor

```text
server/python/audit_engine/
├── __init__.py
├── pipeline/
│   ├── __init__.py
│   ├── orquestrador.py
│   ├── contexto.py
│   ├── status.py
│   └── logs.py
├── tabelas/
│   ├── produtos/
│   ├── produtos_unidades/
│   ├── produtos_agrupados/
│   ├── fatores_conversao/
│   └── auxiliares/
├── utils/
│   ├── __init__.py
│   ├── polars_utils.py
│   ├── io_utils.py
│   ├── path_utils.py
│   └── schema_utils.py
└── contratos/
```

---

## 9. Plano por fases

## Fase 1 — Fechar infraestrutura de execução
### Meta
Sistema sobe e frontend chama backend real.

### Tarefas
- consolidar `server/index.ts`
- consolidar `vite.config.ts`
- consolidar `api.py`
- revisar hooks/serviços do frontend
- validar rotas básicas

### Saída esperada
- execução real iniciando pela UI

---

## Fase 2 — Estabilizar o pipeline
### Meta
Orquestrador rodando com logging e retorno padronizado.

### Tarefas
- revisar `orquestrador.py`
- padronizar contexto de execução
- padronizar respostas por etapa
- implementar tratamento de falhas

### Saída esperada
- pipeline executando sem travar silenciosamente

---

## Fase 3 — Entregar `produtos_unidades`
### Meta
Primeira tabela crítica funcional e validada.

### Tarefas
- criar/ajustar contrato
- implementar extratores
- implementar normalização
- implementar identificação de compras e vendas
- implementar consolidação
- implementar validações
- salvar resultado final

### Saída esperada
- `produtos_unidades.parquet` gerado corretamente

---

## Fase 4 — Entregar tabelas derivadas
### Meta
Concluir encadeamento principal do domínio produto.

### Tarefas
- revisar `produtos`
- concluir `produtos_agrupados`
- concluir `fatores_conversao`
- amarrar dependências entre tabelas

### Saída esperada
- fluxo de produto completo no pipeline

---

## Fase 5 — Qualidade e testes
### Meta
Garantir estabilidade mínima antes de consolidar na `main`.

### Tarefas
- testes unitários de regras de negócio;
- testes de integração das rotas;
- teste ponta a ponta com amostra real;
- validação de schemas;
- validação de arquivos gerados.

### Saída esperada
- baseline estável para evolução

---

## 10. Testes mínimos obrigatórios

## 10.1 Backend
- subir API sem erro;
- chamar endpoint de health;
- iniciar pipeline com parâmetros válidos;
- iniciar pipeline com parâmetros inválidos;
- validar resposta de erro.

## 10.2 Frontend
- renderizar tela de Extração;
- enviar parâmetros;
- exibir loading;
- exibir sucesso;
- exibir erro.

## 10.3 Pipeline
- gerar tabela vazia com schema correto quando não houver dados;
- gerar tabela com dados reais mínimos;
- validar deduplicação;
- validar regras de compras e vendas.

---

## 11. Riscos principais e mitigação

## Risco 1 — Divergência de schema entre fontes
**Mitigação:** contratos explícitos e funções de normalização por fonte.

## Risco 2 — Regras de compra e venda inconsistentes
**Mitigação:** testes com amostras pequenas e validação por CFOP.

## Risco 3 — Acoplamento excessivo entre tabelas
**Mitigação:** contratos e interfaces por tabela, sem dependência implícita.

## Risco 4 — Interface mostrar estado incorreto
**Mitigação:** status unificado vindo do backend.

## Risco 5 — Falta de rastreabilidade
**Mitigação:** preservar campos de origem desde o início.

---

## 12. Critério de pronto

A implementação será considerada concluída quando:

- a UI iniciar a execução real do pipeline;
- o backend estiver operacional e estável;
- `produtos_unidades` estiver funcional e validada;
- as tabelas principais derivadas de produtos estiverem integradas;
- existir rastreabilidade mínima entre fontes e saída;
- a execução gerar artefatos por CNPJ;
- testes mínimos passarem;
- a `main` puder receber a versão final sem depender de mock ou ajustes manuais essenciais.

---

## 13. Ordem prática recomendada de execução

1. fechar `server/index.ts`
2. fechar `vite.config.ts`
3. fechar `server/python/api.py`
4. revisar `pipeline/orquestrador.py`
5. implementar `produtos_unidades`
6. revisar `produtos_agrupados`
7. revisar `fatores_conversao`
8. integrar status e listagem de arquivos no frontend
9. testar ponta a ponta
10. consolidar na `main`

---

## 14. Entregáveis finais esperados

- backend funcional
- frontend funcional
- pipeline funcional
- `produtos_unidades.py` implementado
- documentação mínima de execução
- testes mínimos
- versão final pronta para merge/consolidação

---

## 15. Próxima ação recomendada

A próxima ação mais eficiente é:

**implementar primeiro a Fase 1 e, em seguida, atacar diretamente a `produtos_unidades`, porque ela é o gargalo funcional central do domínio de produto e libera a maior parte do restante do pipeline.**
