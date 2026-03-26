# Brainstorm de Design — audit_react

## Contexto
Sistema de auditoria fiscal profissional para uso interno (SEFIN). Interface de dashboard com sidebar persistente, múltiplas abas de trabalho (Extração, Consulta, Agregação, Conversão, Estoque), visualização de tabelas de dados pesadas e controles de pipeline.

---

<response>
<text>

## Ideia 1: "Institutional Precision" — Swiss Design Fiscal

**Design Movement**: International Typographic Style (Swiss Design) adaptado para ferramentas fiscais governamentais.

**Core Principles**:
1. Grid rigoroso com alinhamento matemático — cada elemento ocupa posição previsível
2. Hierarquia tipográfica forte — títulos condensados em peso bold, corpo em regular
3. Monocromia funcional — cor usada apenas para status e ações, nunca decorativa
4. Densidade informacional alta — maximizar dados visíveis sem poluição visual

**Color Philosophy**: Base em slate escuro (sidebar #0f172a) com área de trabalho em off-white (#f8fafc). Cor primária azul institucional (#1e40af) para ações e seleções. Verde (#059669) para sucesso/conclusão, âmbar (#d97706) para pendências, vermelho (#dc2626) para erros. A paleta transmite seriedade governamental e confiança institucional.

**Layout Paradigm**: Sidebar fixa à esquerda (240px) com navegação vertical por ícone+texto. Área principal dividida em header contextual + zona de trabalho. Tabelas ocupam toda a largura disponível. Painéis de filtro colapsáveis à direita.

**Signature Elements**:
1. Barra de status do pipeline com etapas horizontais conectadas por linhas finas
2. Badges tipográficos monoespaçados para CNPJs e códigos fiscais
3. Separadores finos (1px) com espaçamento generoso entre seções

**Interaction Philosophy**: Feedback imediato e discreto. Toasts sutis no canto. Transições de 150ms. Hover states com mudança de background sutil. Foco em eficiência — zero animações desnecessárias.

**Animation**: Transições de fade-in (200ms ease-out) para troca de páginas. Skeleton loading para tabelas. Progress bars lineares para pipeline. Sem bouncing, sem spring physics.

**Typography System**: 
- Display: "DM Sans" 700 para títulos de página
- Body: "DM Sans" 400/500 para texto corrido e labels
- Mono: "JetBrains Mono" para CNPJs, valores numéricos, nomes de arquivos

</text>
<probability>0.08</probability>
</response>

---

<response>
<text>

## Ideia 2: "Dark Command Center" — Control Room Aesthetic

**Design Movement**: Inspirado em interfaces de controle industrial e dashboards de monitoramento (estilo Bloomberg Terminal / mission control).

**Core Principles**:
1. Fundo escuro como base — reduz fadiga em uso prolongado
2. Informação em camadas — cards elevados sobre fundo profundo
3. Cor como semáforo — cada cor tem significado operacional preciso
4. Densidade máxima com legibilidade — tabelas compactas com tipografia clara

**Color Philosophy**: Background profundo (#0a0e1a), cards em (#111827), borders sutis em (#1f2937). Primária ciano-azulada (#3b82f6) para ações e links. Emerald (#10b981) para operações concluídas. Amber (#f59e0b) para alertas. Rose (#f43f5e) para erros. A paleta evoca controle operacional e vigilância fiscal.

**Layout Paradigm**: Sidebar colapsável escura (ícones quando fechada, ícone+texto quando aberta). Header com breadcrumb e indicador de CNPJ ativo. Área de trabalho com grid de cards para métricas e tabela principal abaixo. Painel lateral direito para detalhes/filtros que desliza sobre o conteúdo.

**Signature Elements**:
1. Indicadores luminosos (dots pulsantes) para status de pipeline em tempo real
2. Cards com borda superior colorida indicando categoria (extração=azul, agregação=verde, estoque=âmbar)
3. Mini-gráficos sparkline nos KPI cards

**Interaction Philosophy**: Responsividade tátil — hover com glow sutil, clicks com ripple contido. Sidebar com transição suave de colapso. Tabelas com row highlighting ao hover. Drag-and-drop para reordenar colunas.

**Animation**: Entrada de cards com stagger (50ms delay entre cada). Sidebar collapse com ease-in-out 300ms. Números em KPIs com count-up animation. Loading states com shimmer gradient.

**Typography System**:
- Display: "Space Grotesk" 600/700 para títulos e KPIs
- Body: "Inter" 400/500 para texto e labels
- Mono: "Fira Code" para dados tabulares, CNPJs, valores

</text>
<probability>0.05</probability>
</response>

---

<response>
<text>

## Ideia 3: "Structured Clarity" — Neo-Brutalist Data Tool

**Design Movement**: Neo-Brutalism funcional — bordas definidas, sem sombras suaves, tipografia forte, cores sólidas em blocos.

**Core Principles**:
1. Bordas visíveis e definidas — cada elemento tem contorno claro (2px solid)
2. Tipografia como arquitetura — tamanhos contrastantes criam hierarquia imediata
3. Cor em blocos sólidos — sem gradientes, sem transparências
4. Funcionalidade brutal — cada pixel serve a um propósito

**Color Philosophy**: Fundo branco puro (#ffffff), bordas pretas (#000000), sidebar em bloco amarelo (#fbbf24) ou azul royal (#2563eb). Accent em vermelho (#ef4444) para ações destrutivas. Verde (#22c55e) para confirmações. A paleta é direta, sem ambiguidade — cada cor grita sua função.

**Layout Paradigm**: Sidebar como bloco sólido colorido à esquerda. Área de trabalho dividida em blocos retangulares com bordas grossas. Tabelas com headers em fundo sólido contrastante. Sem cantos arredondados — tudo retangular.

**Signature Elements**:
1. Headers de seção com fundo sólido colorido e texto branco em caps
2. Botões com borda grossa (3px) e shadow offset (4px 4px 0px #000)
3. Status tags em blocos coloridos sólidos sem border-radius

**Interaction Philosophy**: Feedback imediato e óbvio. Botões com offset shadow que "pressiona" no click. Hover com inversão de cores. Sem transições suaves — mudanças instantâneas.

**Animation**: Mínima. Entrada de página com slide-in rápido (100ms). Sem fade, sem ease. Loading com barra de progresso sólida pulsante.

**Typography System**:
- Display: "Space Mono" 700 para títulos (all-caps)
- Body: "Work Sans" 400/500 para texto
- Mono: "Space Mono" 400 para dados

</text>
<probability>0.03</probability>
</response>

---

## Decisão

**Escolha: Ideia 1 — "Institutional Precision" (Swiss Design Fiscal)**

Motivo: O sistema é uma ferramenta profissional de auditoria fiscal para uso governamental. A abordagem Swiss Design prioriza legibilidade, densidade informacional e seriedade institucional — exatamente o que auditores fiscais precisam. O tema escuro da sidebar com área de trabalho clara oferece contraste funcional sem fadiga visual. A tipografia DM Sans + JetBrains Mono é ideal para dados fiscais.
