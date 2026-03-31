# Análise Crítica de UX/UI - Audit React

Abaixo apresento a análise crítica da interface e experiência do sistema Audit React, com foco na otimização de fluxos operacionais densos, legibilidade e redução de esforço cognitivo, dividida pelas áreas solicitadas.

---

## 1. Problemas Encontrados por Área

### 1.1. Home / Entrada Operacional
- **Tela ou componente:** Cards de Etapas e Descrições.
- **Problema:** Textos técnicos e longos demais descrevendo o comportamento do sistema.
- **Impacto no usuário:** Alta carga cognitiva logo na tela de entrada. Usuários operacionais não precisam saber de "materialização" ou "storage" para agir.
- **Evidência visual ou estrutural:** `<CardDescription>A resolucao local e preferencial. O Oracle entra somente para documentos ainda nao materializados.</CardDescription>`
- **Proposta de melhoria:** Focar no benefício e simplificar.
- **Exemplo de antes e depois:**
  - *Antes:* "A resolucao local e preferencial. O Oracle entra somente para documentos ainda nao materializados."
  - *Depois:* "Busca rápida local. Consulta ao banco apenas se necessário."
- **Prioridade:** Alta

- **Tela ou componente:** Ações de busca (Etapa 1).
- **Problema:** Verbos de ação pouco convencionais.
- **Impacto no usuário:** Gera incerteza. "Resolver seleção" não soa como "Pesquisar" ou "Buscar".
- **Evidência visual ou estrutural:** `<Button>Resolver selecao</Button>`
- **Proposta de melhoria:** Utilizar verbos universais.
- **Exemplo de antes e depois:**
  - *Antes:* "Resolver selecao"
  - *Depois:* "Buscar CNPJ" / "Buscar CPF" / "Buscar Lote"
- **Prioridade:** Alta

- **Tela ou componente:** Atalhos de destino (Etapa 3).
- **Problema:** Cards que funcionam como atalhos não parecem clicáveis (falta de *affordance*).
- **Impacto no usuário:** O usuário pode se sentir "preso" após a Etapa 2, sem perceber que os grandes blocos abaixo são a continuação do fluxo.
- **Evidência visual ou estrutural:** `<Button asChild variant="outline">` sem efeitos visuais fortes de interação (como um ícone direcional ou mudança de borda no hover).
- **Proposta de melhoria:** Adicionar ícone de seta (`ArrowRight`) e estados de hover (`hover:border-primary hover:bg-muted/40`).
- **Exemplo de antes e depois:**
  - *Antes:* Card estático com texto descritivo.
  - *Depois:* Card interativo que muda de cor e exibe uma seta ao passar o mouse.
- **Prioridade:** Alta

### 1.2. Sidebar e Header
- **Tela ou componente:** Sidebar recolhida.
- **Problema:** Tooltips poluem a visualização.
- **Impacto no usuário:** Ao recolher a sidebar para ganhar espaço, passar o mouse por cima dos ícones exibe um grande bloco de texto que cobre o conteúdo da página.
- **Evidência visual ou estrutural:** `<TooltipContent>` renderiza tanto `item.label` quanto `item.description`.
- **Proposta de melhoria:** Exibir estritamente o título (label) no tooltip.
- **Exemplo de antes e depois:**
  - *Antes:* "Configurações - Gestão do sistema, Oracle, etc."
  - *Depois:* "Configurações"
- **Prioridade:** Média

- **Tela ou componente:** Header / Seletor de CNPJ Ativo.
- **Problema:** O input de contexto ativo parece uma barra de busca global convencional.
- **Impacto no usuário:** Risco do usuário tentar buscar outras informações ali em vez de entender que aquilo define o escopo (CNPJ) de toda a aplicação.
- **Evidência visual ou estrutural:** `<Input placeholder="00.000.000/0000-00" />` solto.
- **Proposta de melhoria:** Trocar a label de "CNPJ ativo" para "Contexto" acompanhada de um ícone de "Alvo" (Target), ou utilizar um bloco visualmente distinto.
- **Exemplo de antes e depois:**
  - *Antes:* "CNPJ ativo [ input ]"
  - *Depois:* "[ícone] Contexto atual [ input ]"
- **Prioridade:** Alta

### 1.3. Dados Cadastrais
- **Tela ou componente:** Cards da Ficha Cadastral (BlocoFicha).
- **Problema:** Títulos muito grandes para áreas limitadas, podendo quebrar o layout dependendo da resolução (truncamento).
- **Impacto no usuário:** Quebra de harmonia visual e dificuldade de leitura.
- **Evidência visual ou estrutural:** `<p className="text-[11px] font-semibold uppercase tracking-[0.18em]">` em telas menores.
- **Proposta de melhoria:** Manter o uso do `tracking`, mas prever regras de overflow (`truncate`) ou limitar os textos de rótulo.
- **Exemplo de antes e depois:**
  - *Antes:* "Natureza Jurídica Detalhada" quebrando em duas ou três linhas em telas apertadas.
  - *Depois:* "Natureza Jurídica" (limitado via `truncate`).
- **Prioridade:** Baixa

### 1.4. Extração
- **Tela ou componente:** Tabela de Extração / Status.
- **Problema:** Não há distinção clara e imediata entre o erro na extração do banco e o status "Concluído com Erro" local.
- **Impacto no usuário:** O usuário não sabe se deve tentar de novo, verificar o banco ou apenas ignorar aquele CNPJ.
- **Evidência visual ou estrutural:** Badges de status similares.
- **Proposta de melhoria:** Badges semânticos: usar "Destructive" para falhas de rede/banco (erros bloqueantes) e um "Warning/Outline" amarelo para alertas operacionais ou conclusões parciais.
- **Exemplo de antes e depois:**
  - *Antes:* Erros de diferentes naturezas mostrados iguais.
  - *Depois:* Erro Sistêmico (Vermelho vibrante) vs Pendência Cadastral (Amarelo).
- **Prioridade:** Média

### 1.5. Consulta
- **Tela ou componente:** Filtros da Tabela.
- **Problema:** Hierarquia visual ruim, empilhando todos os filtros e botões de ação em uma mesma "grid".
- **Impacto no usuário:** Em telas largas, parece desorganizado; falta foco na ação principal da tabela (Atualizar/Exportar).
- **Evidência visual ou estrutural:** `<div className="grid grid-cols-1 gap-3 md:grid-cols-4">` agrupa ações e selects indiscriminadamente.
- **Proposta de melhoria:** Utilizar layout `flex` com `justify-between`. Filtros agrupados à esquerda e ações (`ml-auto`) à direita.
- **Exemplo de antes e depois:**
  - *Antes:* Select, Select, Input, Botões espalhados uniformemente.
  - *Depois:* [Filtros juntos] --------espaço-------- [Atualizar][Exportar]
- **Prioridade:** Alta

### 1.6. Configurações / Mapeamento Oracle
- **Tela ou componente:** Textos Explicativos de Card.
- **Problema:** Densidade cognitiva absurda na descrição da arquitetura do sistema ("Analise estrutural dos SQLs fiscais para decompor extracoes...").
- **Impacto no usuário:** Intimidação. Excesso de jargão de engenharia (Polars, Parquet) na interface do usuário.
- **Evidência visual ou estrutural:** Descrições longas em quase todos os painéis.
- **Proposta de melhoria:** Ocultar ou simplificar os jargões.
- **Exemplo de antes e depois:**
  - *Antes:* "Analise estrutural dos SQLs fiscais para decompor extracoes por CNPJ, persistencia em Parquet e recomposicao lazy em Polars."
  - *Depois:* "Estrutura de extração dos SQLs detectados."
- **Prioridade:** Média

### 1.7. Padrões Globais (Relatórios e Tabelas)
- **Tela ou componente:** Ações repetitivas em Tabela (ex: Aba Relatórios).
- **Problema:** Botões largos ("DOCX", "PDF") repetidos linha a linha da tabela de CNPJs.
- **Impacto no usuário:** Alta poluição visual. Concorrência com os dados que realmente importam na tabela.
- **Evidência visual ou estrutural:** `<Button className="h-7 gap-1 text-[10px]"><FileText/> DOCX</Button>`.
- **Proposta de melhoria:** Converter botões repetitivos de tabelas para "Icon Buttons" (`size="icon"`, `variant="ghost"`) e adicionar `title` para acessibilidade.
- **Exemplo de antes e depois:**
  - *Antes:* [📄 DOCX] [📥 PDF] em todas as linhas.
  - *Depois:* [📄] [📥] sutis, sem borda forte.
- **Prioridade:** Alta

---

## 2. Top 15 Melhorias Mais Importantes

1.  **Reforço de Clicabilidade (Home):** Adicionar `ArrowRight` e `hover:border-primary` nos atalhos da Etapa 3.
2.  **Separação de Ações vs. Filtros (Consulta):** Flex-wrap com `ml-auto` para isolar Exportar e Atualizar no canto direito.
3.  **Botões Ícone em Tabelas (Relatórios):** Substituir textos "DOCX"/"PDF" repetidos por botões minimalistas (`variant="ghost" size="icon"`).
4.  **Encurtar Labels de Ação (Geral):** Trocar "Resolver" por "Buscar" em todo o sistema.
5.  **Diferenciação de Contexto (Header):** Incluir ícone de alvo no input do CNPJ para diferenciar de uma barra de busca global.
6.  **Redução de Tooltips (Sidebar):** Omitir a descrição na sidebar recolhida, mostrando só o título.
7.  **Despoluição de Textos (Configurações):** Remover descrições de arquitetura (Polars/Parquet) da interface.
8.  **Simplificação do Botão Consolidado (Relatórios):** Mudar "Gerar relatorio geral consolidado" para "Relatório Consolidado".
9.  **Limitação de Transbordamento (Geral):** Envolver todos os `<DataTable>` em wrappers de `overflow-x-auto`.
10. **Ações Primárias Visíveis (Configurações):** O botão "Salvar Configurações" está no final da página; garantir que seja contrastante (`variant="default"`).
11. **Consistência do Hover (Relatórios/Listas):** Adicionar estados `hover:bg-accent` em itens selecionáveis como DSFs.
12. **Otimização do Espaço do Botão "Atualizar":** Em contextos apertados, usar apenas o ícone `<RefreshCw>`.
13. **Uso de Labels Relacionados (Acessibilidade):** Garantir que os switches em Configurações tenham `id` conectados corretamente aos labels para aumento da área de clique.
14. **Feedback de Input (Conversão):** As inputs numéricas (Fator) não têm *feedback* óbvio quando salvas. (Necessário Toast de sucesso imediato).
15. **Grids Responsivas (Telas de painéis):** Garantir que grids que usam `md:grid-cols-2` ou `4` mudem para `grid-cols-1` no mobile para evitar amontoamento horizontal.

---

## 3. Sugestões de Microcopy (UX Writing)

### Botões
- 🔴 *Evite:* Resolver selecao, Resolver lote.
  - 🟢 *Use:* Buscar CNPJ, Buscar Lote.
- 🔴 *Evite:* Gerar relatorio geral consolidado.
  - 🟢 *Use:* Relatório Consolidado (com ícone).
- 🔴 *Evite:* Gerar DOCX individual / Gerar PDF individual.
  - 🟢 *Use:* Gerar DOCX / Gerar PDF.
- 🔴 *Evite:* Recalcular derivados.
  - 🟢 *Use:* Recalcular.

### Títulos e Subtítulos
- 🔴 *Evite:* "A resolucao local e preferencial. O Oracle entra somente para documentos ainda nao materializados."
  - 🟢 *Use:* "Busca rápida local. Consulta ao banco apenas se necessário."
- 🔴 *Evite:* "Analise estrutural dos SQLs fiscais para decompor extracoes por CNPJ, persistencia em Parquet e recomposicao lazy em Polars."
  - 🟢 *Use:* "Estrutura de extração dos SQLs."
- 🔴 *Evite:* "Os atalhos abaixo respeitam o tipo de selecao e a quantidade de CNPJs efetivamente resolvidos."
  - 🟢 *Use:* "Navegue para o próximo passo da sua análise."

### Mensagens de Erro e Estados Vazios
- 🔴 *Evite:* "Valide a conexao Oracle ativa antes de ajustar aliases ou executar extracao."
  - 🟢 *Use:* "Valide a conexão Oracle para editar."
- 🔴 *Evite:* "Nenhuma DSF efetiva encontrada no storage."
  - 🟢 *Use:* "Nenhuma DSF encontrada."
- 🔴 *Evite:* "Tabela sem dados para o CNPJ selecionado."
  - 🟢 *Use:* "Tabela vazia."

---

## 4. Recomendações Visuais para Áreas Clicáveis

1. **Containers Navegáveis (Cards da Home):** Sempre adicione a classe utilitária `group` no container principal (o `Link` ou `Button`). Isso permite transições fluidas. Utilize `hover:border-primary` para o contorno acender em azul/destaque.
2. **Setas Indicativas:** Insira o ícone `<ArrowRight className="opacity-0 group-hover:opacity-100 transition-opacity" />` dentro dos cards. Isso cria um micromovimento sutil que o cérebro reconhece como "ir para outra página".
3. **Ghost Buttons em Tabelas:** Ações dentro de linhas não devem competir com os dados. Utilize `<Button variant="ghost" size="icon">` (que remove a borda de fundo) e apenas revele o fundo escuro de `hover:bg-accent` quando o mouse estiver sobre o botão.
4. **Listas de Seleção (ex: DSFs na aba relatórios):** Para cada item clicável da lista, aplique classes do Tailwind de *transition* como `transition-colors hover:bg-accent/50 cursor-pointer` para que o usuário sinta que a linha responde ao mouse antes mesmo do clique.

---

## 5. Checklist Técnico para Implementação (Frontend)

- [ ] **Tabelas (`DataTable.tsx` e `<Table>` nativo):** Inspecionar se o componente contêiner possui `overflow-x-auto` globalmente aplicado para prevenir scroll na página inteira em telas pequenas.
- [ ] **Truncamento Textual (`truncate`):** Aplicar `max-w-[x]` + `truncate` nas descrições da ficha cadastral ou em células de tabelas (ex: nomes muito longos de produtos ou descrições padrão).
- [ ] **Isolamento de Header Global (`DashboardLayout.tsx`):**
  - Adicionar o ícone de `<Search>` ou `<Target>` dentro de um wrapper que contém o `<Input>` do CNPJ ativo.
  - Alterar o texto ao lado do input de "CNPJ ativo" para "Contexto".
- [ ] **Tooltips Condicionais:** No mapeamento de navegação, checar se a sidebar está em estado reduzido (`collapsed`) para renderizar **apenas o nome da página** em vez do nome + descrição técnica longa.
- [ ] **Alinhamento Flex de Barra de Ferramentas (`Consulta.tsx`):** Remover a classe `grid-cols-4` superior; envelopar em `<div className="flex flex-wrap items-center justify-between">`.
- [ ] **Acessibilidade de Formulários (`Configurações.tsx`):** Garantir que todo `<Switch>` e `<Checkbox>` tenha um `<Label htmlFor="id-do-switch">` válido ou envolva a área com `cursor-pointer`, permitindo clique indireto no texto.
- [ ] **Atalhos da Home (`Home.tsx`):** Substituir todo bloco de descrição dos botões da Etapa 3 pela versão enxuta sugerida acima e incluir as classes de affordance visual (`group`, etc.).
