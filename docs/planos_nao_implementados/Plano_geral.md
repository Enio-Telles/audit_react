# Plano de Implementação — Dossiê Fiscal / Tributário (Rondônia)

> Objetivo: permitir a seleção das abordagens que devem entrar no produto, com ordem sugerida, dependências, impacto e esforço.

---

## 1) Premissas

- O plano considera o foco em **auditoria tributária**, **rastreabilidade** e **apoio à decisão**.
- A priorização parte de uma visão em que o sistema precisa ligar:
  - **produto / documento / cálculo / regra fiscal / risco / responsável**
- As abordagens abaixo foram organizadas para permitir implementação incremental.

---

## 2) Critérios de decisão

Use estes critérios para selecionar o que entra primeiro:

- **Valor fiscal imediato**: reduz erro, glosa, exposição ou retrabalho?
- **Dependência técnica**: exige base/modelagem antes da UI?
- **Velocidade de entrega**: gera resultado visível rápido?
- **Aderência regulatória**: melhora vínculo com fundamento legal?
- **Capacidade de prova**: ajuda auditoria, defesa e rastreabilidade?

Escala sugerida:

- Impacto: Baixo / Médio / Alto
- Esforço: Baixo / Médio / Alto
- Prioridade: P0 / P1 / P2

---

## 3) Abordagens candidatas

### A. Modelagem da seção de contatos e responsáveis

**Objetivo**
Estruturar a seção `contato` para exibir contador, sócios, administradores, responsáveis, parceiros e histórico de vínculo.

**Inclui**

- [ ] Agrupamento por papel (`contador`, `sócio`, `administrador`, `parceiro`)
- [ ] Deduplicação de e-mails e telefones
- [ ] Exibição de origem da informação
- [ ] Score de confiança da informação
- [ ] Linha do tempo de mudanças de responsáveis
- [ ] Bloco específico para contador atual
- [ ] Bloco específico para sócios/administradores atuais
- [ ] Relação entre responsáveis e empresas vinculadas

**Benefício principal**
Transforma cadastro em mapa de responsabilização e contato útil para auditoria.

**Dependências**

- Normalização de entidades e pessoas
- Regras de deduplicação
- Estrutura de origem/fonte do dado

**Impacto**: Alto
**Esforço**: Médio
**Prioridade sugerida**: P0

---

### B. Painel de enquadramento tributário por produto/NCM/CEST

**Objetivo**
Permitir leitura rápida do tratamento tributário aplicável por item.

**Inclui**

- [ ] Tabela por produto/NCM/CEST
- [ ] Badges para alíquota, FECOEP, ST, DIFAL, isenção, diferimento, suspensão, redução de base, crédito presumido
- [ ] Coluna de fundamento legal
- [ ] Filtro por NCM, CEST, UF, fornecedor, período
- [ ] Drilldown para memória de cálculo
- [ ] Semáforo de risco por divergência entre regra esperada e aplicada

**Benefício principal**
Entrega visão normativa direta, reduzindo análise manual item a item.

**Dependências**

- Dicionário tributário por item
- Regras de enquadramento
- Base de referência legal

**Impacto**: Alto
**Esforço**: Alto
**Prioridade sugerida**: P0

---

### C. Painel de ST / antecipação / MVA

**Objetivo**
Mostrar exposição tributária relacionada a substituição tributária, antecipação e margem de valor agregado.

**Inclui**

- [ ] Identificação de itens sujeitos a ST
- [ ] Identificação de itens sujeitos a antecipação
- [ ] Exibição de MVA aplicada e MVA esperada
- [ ] Heatmap por período
- [ ] Ranking dos itens com maior exposição
- [ ] Cadeia de cálculo expandida (base, MVA, ICMS próprio, ICMS-ST)

**Benefício principal**
Ajuda a validar cálculo tributário e localizar divergências com alto efeito financeiro.

**Dependências**

- Base fiscal por NCM/CEST
- Motor de cálculo ou memória de cálculo consolidada
- Estrutura de documentos de entrada/saída

**Impacto**: Alto
**Esforço**: Alto
**Prioridade sugerida**: P1

---

### D. Painel de créditos, glosas, estornos e CIAP

**Objetivo**
Concentrar análise de crédito tributário aproveitado, potencial, vedado e estornado.

**Inclui**

- [ ] Crédito aproveitado
- [ ] Crédito potencial
- [ ] Crédito vedado
- [ ] Estorno obrigatório
- [ ] Controle de ativo imobilizado / CIAP
- [ ] Waterfall do crédito
- [ ] Tabela de documentos com motivo da glosa/vedação
- [ ] Flags para uso/consumo, ativo, saída isenta, documento irregular, destaque a maior

**Benefício principal**
Ataca uma das áreas mais sensíveis de auditoria tributária.

**Dependências**

- Classificação fiscal das entradas
- Regras de crédito e vedação
- Estrutura temporal para apropriação mensal

**Impacto**: Alto
**Esforço**: Alto
**Prioridade sugerida**: P0

---

### E. Painel de estoque desacobertado, levantamento e presunções fiscais

**Objetivo**
Expor divergências entre estoque teórico, movimentos e documentos, com foco em risco fiscal.

**Inclui**

- [ ] Linha do tempo de entradas, saídas e saldo teórico
- [ ] Divergência entre saldo teórico e saldo apurado
- [ ] Ranking de itens com maior diferença
- [ ] Alertas para entrada sem cobertura documental
- [ ] Alertas para saída sem cobertura
- [ ] Alertas para saldo impossível/inconsistente
- [ ] Matriz de risco por recorrência x valor

**Benefício principal**
Conecta estoque, documento e presunção fiscal com alto valor probatório.

**Dependências**

- Reconciliação confiável de estoque
- Eventos de entrada/saída normalizados
- Chaveamento entre XML, fiscal e estoque

**Impacto**: Alto
**Esforço**: Alto
**Prioridade sugerida**: P1

---

### F. Painel de cadastro e regularidade cadastral de parceiros

**Objetivo**
Mostrar risco cadastral de emitentes, destinatários e parceiros relacionados às operações.

**Inclui**

- [ ] Situação cadastral atual do parceiro
- [ ] Histórico de suspensão/cancelamento
- [ ] Operações com parceiros irregulares
- [ ] Exposição financeira por parceiro
- [ ] Flag para crédito tomado de emitente irregular
- [ ] Ranking de parceiros com maior risco

**Benefício principal**
Evita análise cega de documentos de parceiros em situação fiscal problemática.

**Dependências**

- Fonte de situação cadastral
- Histórico por parceiro
- Vinculação entre operação e cadastro

**Impacto**: Médio/Alto
**Esforço**: Médio
**Prioridade sugerida**: P1

---

### G. Painel de meios de pagamento x documento fiscal

**Objetivo**
Cruzar faturamento/documento fiscal com recebimentos eletrônicos.

**Inclui**

- [ ] Faturamento fiscal x recebimento eletrônico por mês
- [ ] Desvio percentual por período
- [ ] Ranking de divergências
- [ ] Filtro por estabelecimento, bandeira, canal, intermediador
- [ ] Documentos sem correspondência financeira
- [ ] Recebimentos sem lastro fiscal

**Benefício principal**
Excelente para identificar omissão de receita e descasamentos relevantes.

**Dependências**

- Base consolidada de documentos fiscais
- Base consolidada de recebimentos
- Chave ou heurística de conciliação

**Impacto**: Alto
**Esforço**: Alto
**Prioridade sugerida**: P1

---

### H. Motor de regras fiscais e alertas automáticos

**Objetivo**
Centralizar regras de consistência tributária para uso em todos os painéis.

**Inclui**

- [ ] Regra de alíquota esperada x aplicada
- [ ] Regra de ST / MVA / antecipação
- [ ] Regra de crédito vedado
- [ ] Regra de DIFAL
- [ ] Regra de benefício condicionado
- [ ] Regra de parceiro irregular
- [ ] Regra de divergência financeira
- [ ] Regra de estoque inconsistente

**Benefício principal**
Evita lógica espalhada pela interface e aumenta consistência do sistema.

**Dependências**

- Catálogo de regras
- Estrutura de execução e versionamento
- Padronização dos eventos fiscais

**Impacto**: Muito Alto
**Esforço**: Alto
**Prioridade sugerida**: P0

---

### I. Camada de prova e rastreabilidade

**Objetivo**
Garantir que toda visualização tenha evidência verificável.

**Inclui**

- [ ] Origem do dado por linha
- [ ] Documento de origem vinculado
- [ ] Data de coleta/processamento
- [ ] Hash, chave ou identificador de evidência
- [ ] Link “ver memória de cálculo”
- [ ] Link “ver documentos relacionados”

**Benefício principal**
Sem isso, o painel pode ser bonito, mas fraco para auditoria e defesa técnica.

**Dependências**

- Pipeline de evidências
- Metadados por registro
- Interface de drilldown

**Impacto**: Muito Alto
**Esforço**: Médio/Alto
**Prioridade sugerida**: P0

---

### J. Refatoração arquitetural da UI e módulos do dossiê

**Objetivo**
Tirar acoplamento da tela principal e organizar a solução por módulos funcionais.

**Inclui**

- [ ] Separação de componentes por domínio
- [ ] Padronização de tabelas, cards e filtros
- [ ] Camada de estado mais previsível
- [ ] Rotas/seções independentes do dossiê
- [ ] Contratos claros entre backend e UI

**Benefício principal**
Reduz custo de evolução e facilita manter o produto coerente.

**Dependências**

- Mapeamento da UI atual
- Definição de contratos de dados
- Estratégia de migração gradual

**Impacto**: Alto
**Esforço**: Médio/Alto
**Prioridade sugerida**: P1

---

## 4) Pacotes de implementação sugeridos

### Pacote 1 — Fundação mínima viável

**Objetivo**: criar base sólida antes dos painéis mais pesados.

- [ ] A. Modelagem da seção de contatos e responsáveis
- [ ] H. Motor de regras fiscais e alertas automáticos
- [ ] I. Camada de prova e rastreabilidade
- [ ] J. Refatoração arquitetural da UI e módulos do dossiê

**Quando escolher**

- Quando a base atual ainda está muito acoplada
- Quando há risco de retrabalho se a UI avançar primeiro
- Quando o foco é durabilidade técnica

---

### Pacote 2 — Entrega fiscal de alto impacto

**Objetivo**: entregar valor perceptível para usuário/auditoria cedo.

- [ ] B. Painel de enquadramento tributário
- [ ] D. Painel de créditos, glosas e estornos
- [ ] F. Painel de cadastro e regularidade cadastral

**Quando escolher**

- Quando o foco é apoio à análise tributária desde a primeira versão útil
- Quando já existe base suficiente para regras e documentos

---

### Pacote 3 — Inteligência de risco operacional e fiscal

**Objetivo**: atacar inconsistências de estoque, ST e faturamento.

- [ ] C. Painel de ST / antecipação / MVA
- [ ] E. Painel de estoque desacobertado e presunções
- [ ] G. Painel de meios de pagamento x documento fiscal

**Quando escolher**

- Quando o objetivo é fiscalização analítica e identificação de exposição financeira
- Quando a integração documental já está madura

---

## 5) Ordem de implementação recomendada

### Opção recomendada (equilíbrio entre base e valor entregue)

#### Fase 1 — Base estrutural

- [ ] A. Contatos e responsáveis
- [ ] H. Motor de regras
- [ ] I. Prova e rastreabilidade

#### Fase 2 — Primeiro valor fiscal visível

- [ ] B. Enquadramento tributário
- [ ] D. Créditos, glosas, estornos e CIAP

#### Fase 3 — Risco transacional

- [ ] F. Regularidade cadastral
- [ ] C. ST / antecipação / MVA

#### Fase 4 — Cruzamentos avançados

- [ ] E. Estoque desacobertado / presunções
- [ ] G. Pagamentos x documento fiscal

#### Fase 5 — Consolidação técnica

- [ ] J. Refatoração completa dos módulos restantes

---

## 6) Decisão resumida por abordagem

| Código | Abordagem                            |     Impacto |    Esforço | Prioridade sugerida | Selecionar |
| ------- | ------------------------------------ | ----------: | ----------: | ------------------- | ---------- |
| A       | Contatos e responsáveis             |        Alto |      Médio | P0                  | [ ]        |
| B       | Enquadramento tributário            |        Alto |        Alto | P0                  | [ ]        |
| C       | ST / antecipação / MVA             |        Alto |        Alto | P1                  | [ ]        |
| D       | Créditos / glosas / estornos / CIAP |        Alto |        Alto | P0                  | [ ]        |
| E       | Estoque desacobertado / presunções |        Alto |        Alto | P1                  | [ ]        |
| F       | Regularidade cadastral               | Médio/Alto |      Médio | P1                  | [ ]        |
| G       | Pagamentos x documento fiscal        |        Alto |        Alto | P1                  | [ ]        |
| H       | Motor de regras fiscais              |  Muito Alto |        Alto | P0                  | [ ]        |
| I       | Prova e rastreabilidade              |  Muito Alto | Médio/Alto | P0                  | [ ]        |
| J       | Refatoração arquitetural           |        Alto | Médio/Alto | P1                  | [ ]        |

---

## 7) Combinações prontas para escolher

### Escolha 1 — Entrega mais segura

- [ ] A + H + I + B

**Bom para**: começar com base sólida e já entregar uma tela útil.

### Escolha 2 — Foco em crédito e conformidade

- [ ] A + H + I + D + F

**Bom para**: times que querem reduzir erro de crédito e risco de cadastro.

### Escolha 3 — Foco em risco e fiscalização

- [ ] H + I + C + E + G

**Bom para**: análise investigativa e auditoria intensiva.

### Escolha 4 — Evolução completa faseada

- [ ] Fase 1 + Fase 2 + Fase 3 + Fase 4

**Bom para**: roadmap principal do produto.

---

## 8) Backlog técnico transversal

Implementar junto ou antes das telas que dependem disso:

- [ ] Normalização de pessoas/empresas
- [ ] Normalização de produtos/NCM/CEST
- [ ] Catálogo de fundamentos legais por regra
- [ ] Serviço de memória de cálculo
- [ ] Serviço de evidências por linha/documento
- [ ] Sistema de alertas e severidade
- [ ] Contratos de API estáveis por módulo
- [ ] Testes de regra fiscal
- [ ] Testes de regressão de UI

---

## 9) Recomendação objetiva

Se a intenção é decidir agora o que entra primeiro, minha recomendação de corte inicial é:

### Primeiro lote

- [ ] A. Contatos e responsáveis
- [ ] H. Motor de regras fiscais
- [ ] I. Prova e rastreabilidade
- [ ] B. Enquadramento tributário
- [ ] D. Créditos, glosas e estornos

### Segundo lote

- [ ] F. Regularidade cadastral
- [ ] C. ST / antecipação / MVA

### Terceiro lote

- [ ] E. Estoque desacobertado / presunções
- [ ] G. Pagamentos x documento fiscal
- [ ] J. Refatoração arquitetural complementar

Motivo:

- entrega valor fiscal real cedo;
- evita construir telas importantes sem base de regra/evidência;
- preserva espaço para cruzamentos avançados depois.

---

## 10) Campo de decisão

### Abordagens aprovadas

- [ ] A
- [ ] B
- [ ] C
- [ ] D
- [ ] E
- [ ] F
- [ ] G
- [ ] H
- [ ] I
- [ ] J

### Ordem aprovada

1. ---
2. ---
3. ---
4. ---
5. ---

### Restrições / observações

---

---

---
