# Plano de Implementação — Melhoria da Visualização do Dossiê

> Objetivo: permitir a seleção das melhorias de **UI/UX e apresentação visual** do dossiê, sem entrar em backend, cálculo tributário, regras fiscais ou otimizações de processamento.

---

## 1. Escopo deste plano

Este plano cobre apenas:

- organização visual do dossiê
- hierarquia de informação
- navegação entre seções
- cards, tabelas e gráficos
- filtros e formas de comparação
- leitura executiva vs leitura analítica
- exibição agregada de contatos e responsáveis
- padrões de destaque visual para risco, divergência e evidência

Este plano **não cobre**:

- modelagem de dados
- novas regras fiscais
- refatoração de serviços
- performance de processamento
- cache
- integração com novas fontes

---

## 2. Critérios para escolher o que implementar primeiro

Use estes critérios para selecionar os itens:

- **Ganho imediato de leitura**: melhora a compreensão já na primeira abertura?
- **Baixa dependência**: depende pouco de backend novo?
- **Redução de ruído**: tira excesso de colunas, abas e densidade visual?
- **Valor para auditoria**: facilita localizar risco, prova e fundamento?
- **Adoção pelo usuário**: melhora a chance de o dossiê virar a tela principal?

Escala sugerida:

- Impacto: Baixo / Médio / Alto
- Esforço visual: Baixo / Médio / Alto
- Prioridade: P0 / P1 / P2

---

## 3. Abordagens visuais candidatas

### A. Reorganizar o dossiê em blocos principais

**Objetivo**
Reduzir a sensação de tela monolítica e transformar o dossiê em leitura por contexto.

**Proposta**
Dividir o dossiê em blocos visuais fixos:

- Resumo
- Contatos e responsáveis
- Fiscal
- Estoque
- Documentos e evidências
- Divergências e alertas

**Selecionar**

- [X] Criar cabeçalho do dossiê com nome da empresa, CNPJ, período e status
- [X] Separar o conteúdo em blocos visuais com títulos claros
- [X] Incluir menu lateral ou navegação por âncoras
- [X] Mostrar resumo no topo e detalhes abaixo

**Benefício principal**
Melhora muito a orientação do usuário dentro do dossiê.

**Impacto**: Alto
**Esforço visual**: Médio
**Prioridade sugerida**: P0

---

### B. Criar modo “Resumo Executivo” e modo “Auditoria”

**Objetivo**
Evitar que todos os usuários vejam a mesma densidade de informação.

**Proposta**
Dois modos de leitura:

- **Resumo Executivo**: KPIs, cards, rankings, poucos campos
- **Auditoria**: colunas detalhadas, evidências, origem, observações e memória de cálculo

**Selecionar**

- [ ] Botão de troca entre modo executivo e auditoria
- [ ] Perfis de colunas diferentes por modo
- [ ] KPIs no modo executivo
- [ ] Exibição de evidências e origem no modo auditoria

**Benefício principal**
Aumenta muito a usabilidade sem perder profundidade.

**Impacto**: Alto
**Esforço visual**: Médio
**Prioridade sugerida**: P0

---

### C. Criar cabeçalho executivo com cards-resumo

**Objetivo**
Dar leitura imediata do caso antes de abrir tabelas extensas.

**Proposta**
Exibir no topo do dossiê cards como:

- total de alertas
- total de documentos
- total de divergências
- exposição estimada
- quantidade de contatos relevantes
- situação cadastral geral

**Selecionar**

- [X] Mostrar 4 a 6 cards principais no topo
- [X] Permitir clique no card para abrir o detalhe filtrado
- [ ] Exibir tendência por período quando aplicável
- [ ] Destacar cards críticos com semáforo visual

**Benefício principal**
A tela já entrega valor sem exigir leitura tabular imediata.

**Impacto**: Alto
**Esforço visual**: Baixo/Médio
**Prioridade sugerida**: P0

---

### D. Melhorar a seção de contatos com exibição agregada por papel

**Objetivo**
Atender a necessidade de mostrar contatos “todos juntos” por grupo funcional.

**Proposta**
Exibir blocos separados:

- Empresa atual
- Contador atual
- Sócios atuais
- Filiais atuais

Dentro de cada bloco:

- nome
- documento
- e-mails agregados
- telefones agregados
- endereço
- origem
- observações/divergências

**Selecionar**

- [X] Criar bloco fixo “Contador atual”
- [X] Criar bloco fixo “Sócios atuais”
- [X] Criar bloco fixo “Empresa atual”
- [X] Criar bloco fixo “Filiais atuais”
- [X] Mostrar e-mails agregados em linha única ou lista compacta
- [X] Mostrar telefones agregados em linha única ou lista compacta
- [X] Exibir divergências em destaque discreto

**Benefício principal**
Resolve um dos principais problemas práticos de leitura do dossiê.

**Impacto**: Alto
**Esforço visual**: Médio
**Prioridade sugerida**: P0

---

### E. Criar padrão visual de risco, divergência e confiança

**Objetivo**
Fazer o usuário bater o olho e entender o que exige atenção.

**Proposta**
Padronizar badges e sinais visuais para:

- risco alto / médio / baixo
- divergência encontrada
- dado consolidado
- dado com conflito
- dado sem evidência suficiente

**Selecionar**

- [ ] Badge de risco
- [X] Badge de divergência
- [ ] Badge de confiança do dado
- [X] Ícones ou chips para origem da informação
- [X] Legenda visual padronizada em todo o dossiê

**Benefício principal**
Reduz tempo de leitura e melhora consistência da interface.

**Impacto**: Alto
**Esforço visual**: Baixo
**Prioridade sugerida**: P0

---

### F. Redesenhar tabelas para leitura mais limpa

**Objetivo**
Reduzir o efeito de “planilha crua” e melhorar uso das tabelas grandes.

**Proposta**
Aplicar uma estrutura padrão para todas as tabelas do dossiê:

- cabeçalho com resumo do recorte
- linha de totais
- colunas técnicas ocultas por padrão
- agrupamento expansível
- destaque de colunas-chave
- perfil de visualização salvo

**Selecionar**

- [ ] Ocultar colunas técnicas por padrão
- [ ] Fixar colunas principais à esquerda
- [ ] Adicionar linha de totais no topo ou rodapé
- [X] Permitir agrupamento expansível
- [X] Criar perfil “compacto” de tabela
- [X] Criar perfil “analítico” de tabela
- [ ] Inserir busca local na própria tabela

**Benefício principal**
As tabelas continuam poderosas, mas ficam muito menos cansativas.

**Impacto**: Alto
**Esforço visual**: Médio
**Prioridade sugerida**: P0

---

### G. Introduzir gráficos simples e acionáveis

**Objetivo**
Usar gráficos onde eles ajudam de verdade, sem poluir o dossiê.

**Proposta**
Priorizar poucos gráficos por seção, sempre ligados a um filtro ou detalhe.

**Gráficos sugeridos**

- barras para ranking de maiores riscos
- linha para evolução mensal
- heatmap para recorrência de divergências
- waterfall para composição de valores
- pizza ou donut apenas em casos de composição simples

**Selecionar**

- [ ] Gráfico de ranking no resumo
- [ ] Evolução mensal de alertas
- [ ] Heatmap de divergências por período
- [ ] Waterfall para composição de exposição
- [X] Drill-down do gráfico para a tabela filtrada

**Benefício principal**
Aumenta leitura executiva sem virar dashboard excessivo.

**Impacto**: Médio/Alto
**Esforço visual**: Médio
**Prioridade sugerida**: P1

---

### H. Criar navegação por drill-down

**Objetivo**
Permitir que o usuário saia do macro para o detalhe sem se perder.

**Proposta**
Fluxo ideal:

- card ou gráfico
- tabela resumida
- linha específica
- evidência/documento

**Selecionar**

- [X] Clique em KPI abre lista filtrada
- [X] Clique em linha abre painel lateral de detalhe
- [X] Clique em evidência abre documento ou origem
- [X] Breadcrumb para voltar ao nível anterior

**Benefício principal**
Melhora muito a sensação de produto auditável e navegável.

**Impacto**: Alto
**Esforço visual**: Médio
**Prioridade sugerida**: P1

---

### I. Criar painel lateral de detalhe rápido

**Objetivo**
Evitar troca de tela desnecessária para consultar informações complementares.

**Proposta**
Abrir um painel lateral com:

- dados principais da linha selecionada
- badges de risco
- origem dos dados
- documentos relacionados
- observações

**Selecionar**

- [X] Painel lateral de detalhe
- [X] Ações rápidas no painel
- [X] Exibição de origem/evidência no painel
- [X] Comparação rápida dentro do painel

**Benefício principal**
Preserva o contexto da tabela e acelera inspeção.

**Impacto**: Médio/Alto
**Esforço visual**: Médio
**Prioridade sugerida**: P1

---

### J. Criar visão de comparação lado a lado

**Objetivo**
Facilitar comparação entre períodos, fontes ou agrupamentos.

**Proposta**
Comparar lado a lado:

- dois períodos
- duas empresas
- duas fontes de dado
- dois cenários de consolidação

**Selecionar**

- [X] Comparação por período
- [ ] Comparação por empresa
- [X] Comparação por origem/fonte
- [X] Destaque automático das diferenças

**Benefício principal**
Ajuda muito revisão, conferência e validação.

**Impacto**: Médio
**Esforço visual**: Médio/Alto
**Prioridade sugerida**: P2

---

### K. Criar seção de evidências e documentos relacionados

**Objetivo**
Deixar mais claro por que um dado está sendo mostrado e de onde ele veio.

**Proposta**
Cada bloco relevante do dossiê deve ter acesso claro a:

- documento de origem
- chave/identificador
- data
- fonte
- observações

**Selecionar**

- [X] Lista de evidências por seção
- [X] Link de abertura do documento relacionado
- [X] Exibição de fonte/origem no detalhe
- [X] Contador de evidências por item

**Benefício principal**
Aumenta confiabilidade visual e capacidade de prova.

**Impacto**: Alto
**Esforço visual**: Médio
**Prioridade sugerida**: P1

---

### L. Criar presets de layout por perfil de uso

**Objetivo**
Permitir que diferentes públicos usem o mesmo dossiê sem conflito.

**Perfis sugeridos**

- Fiscal
- Auditor
- Gestor
- Atendimento/triagem

**Selecionar**

- [X] Preset Fiscal
- [X] Preset Auditor
- [ ] Preset Gestor
- [ ] Preset Atendimento
- [X] Salvar último layout usado

**Benefício principal**
Aumenta adoção e reduz retrabalho de configuração visual.

**Impacto**: Médio
**Esforço visual**: Médio
**Prioridade sugerida**: P2

---

## 4. Pacotes prontos para decisão

### Pacote 1 — Base visual mínima recomendada

Implementar:

- [X] A. Reorganizar o dossiê em blocos principais
- [X] B. Modo Resumo Executivo / Auditoria
- [X] C. Cabeçalho executivo com cards
- [X] D. Contatos agregados por papel
- [ ] E. Padrão visual de risco/divergência/confiança
- [X] F. Redesenho das tabelas

**Resultado esperado**
Grande salto de usabilidade sem depender de mudança grande em regra fiscal.

---

### Pacote 2 — Dossiê visual orientado à auditoria

Implementar:

- [X] Pacote 1
- [X] H. Drill-down
- [X] I. Painel lateral de detalhe
- [X] K. Evidências e documentos relacionados

**Resultado esperado**
O dossiê passa a funcionar como ferramenta de navegação e investigação.

---

### Pacote 3 — Dossiê visual com camada gerencial

Implementar:

- [X] Pacote 2
- [X] G. Gráficos simples e acionáveis
- [X] J. Comparação lado a lado
- [X] L. Presets por perfil

**Resultado esperado**
O dossiê ganha leitura executiva e flexibilidade para públicos diferentes.

---

## 5. Ordem sugerida de implementação

### Fase 1 — Limpeza visual imediata

- [X] A. Reorganizar blocos
- [X] C. Cards-resumo
- [X] E. Badges e padrão visual
- [X] F. Tabelas mais limpas

### Fase 2 — Resolver leitura prática de contatos

- [X] D. Contatos agregados por papel
- [X] B. Modos Executivo e Auditoria

### Fase 3 — Melhorar navegação e contexto

- [X] H. Drill-down
- [X] I. Painel lateral
- [X] K. Evidências

### Fase 4 — Elevar leitura analítica

- [X] G. Gráficos
- [X] J. Comparação lado a lado
- [X] L. Presets por perfil

---

## 6. Decisão final

### Itens aprovados para implementação agora

- [X] A
- [X] B
- [X] C
- [X] D
- [X] E
- [X] F
- [X] G
- [X] H
- [X] I
- [X] J
- [X] K
- [X] L

### Pacote escolhido

- [X] Pacote 1
- [X] Pacote 2
- [X] Pacote 3
- [ ] Seleção personalizada

### Observações de decisão

> Preencher aqui quais abordagens entram primeiro, quais ficam para depois e quais foram descartadas.
