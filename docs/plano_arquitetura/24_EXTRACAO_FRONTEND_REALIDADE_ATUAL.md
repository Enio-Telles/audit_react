# Extração no frontend alinhada à realidade atual do código

## Objetivo

Este passo ajusta a UX de extração para refletir o que o frontend e o backend realmente suportam hoje, sem prometer fluxos que ainda não existem.

---

## Análise do estado atual

Antes deste ajuste, o `LeftPanel` já disparava três modos reais do pipeline:

- `full` → extrai e processa
- `extract` → extrai somente tabelas brutas
- `process` → processa somente o que já está materializado

Mas isso aparecia de forma mais operacional e fragmentada, com botões separados e pouca explicação sobre quando usar cada abordagem.

Além disso, a seleção de SQL ficava muito próxima da execução, o que dificultava entender que ela só afeta modos que passam pela extração.

---

## O que foi implementado

### Nova camada visual de escolha de abordagem

Arquivo:

- `frontend/src/components/pipeline/ExtractionApproachSelector.tsx`

O componente apresenta três abordagens com:

- explicação curta
- exemplo de uso
- imagem SVG simples do fluxo
- resumo do que cada abordagem usa ou ignora

As três abordagens expostas são exatamente as que o código já suporta hoje:

1. `Extrair + Processar`
2. `Somente Extração`
3. `Somente Processamento`

### LeftPanel adaptado

Arquivo:

- `frontend/src/components/layout/LeftPanel.tsx`

Mudanças:

- a execução agora parte de uma abordagem selecionada;
- a UI deixa explícito quando `data_limite` e `consultas` são usadas;
- a seleção guiada de SQL foi desacoplada da execução;
- o texto do painel passou a explicar a realidade atual do pipeline.

### Modal de seleção guiada ajustado

Arquivo:

- `frontend/src/components/modals/ExtrairSelecaoModal.tsx`

Mudança:

- o botão principal deixou de pressupor execução imediata e passou a permitir apenas aplicar a seleção.

---

## Ganho prático

Isso melhora a aderência entre UX e implementação real:

- evita sugerir fluxos inexistentes;
- deixa claro quando a extração bate no banco e quando só reaproveita material já salvo;
- ajuda o usuário a escolher o modo certo sem ler código ou deduzir pelo nome do botão.

---

## Próximo passo recomendado

Agora o próximo passo de maior valor é conectar essa escolha de abordagem com ações operacionais adicionais, como:

- sugerir abordagem com base no estado do catálogo;
- alertar quando `process` foi escolhido mas faltam datasets-base;
- abrir inspeção do catálogo logo após a execução.
