# Datasets ausentes e famílias SQL sugeridas no frontend

## Objetivo

Este passo torna a sugestão automática de abordagem mais acionável, mostrando não apenas a recomendação geral, mas também:

- quais datasets-base ainda faltam;
- quais famílias SQL tendem a completar a base;
- um atalho direto para abrir a seleção guiada de SQL.

---

## O que foi implementado

### Banner expandido de prontidão

Arquivo:

- `frontend/src/components/pipeline/ExtractionReadinessBanner.tsx`

Novidades:

- lista de datasets-base ausentes;
- agrupamento de famílias SQL sugeridas;
- botão `Abrir seleção SQL`.

### Integração no painel lateral

Arquivo:

- `frontend/src/components/layout/LeftPanel.tsx`

Mudança:

- o `LeftPanel` agora entrega ao banner também a ação de abrir a seleção guiada de SQL.

---

## Como a sugestão funciona agora

O frontend continua usando o catálogo materializado para sugerir a abordagem, mas agora complementa essa leitura com pistas operacionais:

- `C170 XML`, `C176 XML`, `Bloco H` → família `EFD`
- `NF-e`, `NFC-e`, `CT-e`, `tb_documentos` → família `Documentos fiscais`
- `dados_cadastrais`, `malhas` → família `Fisconforme / cadastro` ou `Fisconforme / malhas`

Assim, quando a base está incompleta, a própria UI mostra o que está faltando e em qual grupo de SQL faz mais sentido atuar.

---

## Ganho prático

Isso reduz ainda mais a tentativa e erro na operação:

- o usuário entende o que falta;
- o usuário entende onde agir;
- a seleção guiada de SQL fica a um clique do diagnóstico.

---

## Próximo passo recomendado

Agora o próximo passo de maior valor é usar essa mesma leitura para sugerir reprocessamento seletivo por domínio, em vez de sempre empurrar a decisão para o pipeline inteiro.
