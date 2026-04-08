# Fisconforme em lote

Este documento descreve o fluxo atual da tela **Fisconforme — Análise em Lote** no frontend web.

## Etapas do fluxo

### 1. Consulta

Responsabilidades atuais:

- preencher os dados da DSF;
- informar período, PDF e pasta de saída;
- informar os dados do auditor;
- salvar os dados do auditor como padrão e também na DSF atual;
- revisar o saneamento da lista de CNPJs antes da consulta;
- executar a consulta individual ou em lote.

Observação:

- o botão `Salvar dados do auditor` fica nesta etapa para evitar duplicação de edição na etapa final.

### 2. Resultados

Responsabilidades atuais:

- exibir resumo executivo da consulta;
- permitir filtro rápido por situação do CNPJ;
- abrir os detalhes cadastrais e de pendências;
- oferecer atalho `Abrir Dossiê` por CNPJ em nova aba do navegador.

### 3. Para Notificações

Responsabilidades atuais:

- mostrar resumo somente leitura dos dados que serão usados na geração;
- exibir checklist de prontidão;
- permitir geração TXT, Word e lote;
- oferecer atalho `Abrir Dossiê` por CNPJ em nova aba do navegador.

Observação:

- esta etapa não edita mais os dados do auditor;
- qualquer ajuste de auditor, órgão, PDF ou DSF deve ser feito na etapa `1. Consulta`.

## Acesso direto ao Dossiê

O frontend suporta bootstrap por query string para abrir o modo auditoria já no Dossiê:

```text
?mode=audit&tab=dossie&cnpj=12345678000190
```

Comportamento esperado:

- força `appMode = audit`;
- ativa a aba `dossie`;
- seleciona o CNPJ informado;
- limpa os parâmetros da URL após aplicar o bootstrap para evitar reaplicação em refresh posterior.

## Regras preservadas

- nenhuma rota do backend foi alterada para esta funcionalidade;
- a geração TXT, Word e lote continua usando os mesmos payloads já existentes;
- salvar o auditor continua persistindo os defaults e a DSF atual no mesmo fluxo.
