# Reprocessamento seletivo por domínio

## Objetivo

Este passo transforma a orientação do frontend em ação prática para o modo `Somente Processamento`, usando o contrato real já disponível no backend (`tabelas`).

---

## O que foi implementado

### Seletor de domínio analítico

Arquivo:

- `frontend/src/components/pipeline/ProcessingDomainSelector.tsx`

Presets adicionados:

- pipeline analítico completo
- documentos base
- EFD enriquecido
- produtos e classificação
- estoque e análises

Cada preset mapeia para os IDs reais de tabela já aceitos pelo backend, como:

- `tb_documentos`
- `c170_xml`
- `c176_xml`
- `item_unidades`
- `itens`
- `descricao_produtos`
- `produtos_final`
- `fontes_produtos`
- `fatores_conversao`
- `movimentacao_estoque`
- `calculos_mensais`
- `calculos_anuais`

### Integração com a execução

Arquivo:

- `frontend/src/components/layout/LeftPanel.tsx`

Mudanças:

- o seletor aparece quando a abordagem escolhida é `Somente Processamento`;
- a execução passa a enviar `tabelas` seletivas para o backend quando um preset é escolhido;
- se nenhum recorte for escolhido, o comportamento continua sendo reprocessar tudo.

---

## Ganho prático

Isso permite:

- reprocessar apenas o domínio afetado por uma mudança;
- reduzir tempo de feedback quando o ajuste foi localizado;
- alinhar melhor a operação do frontend com o contrato real do pipeline.

---

## Limite assumido conscientemente

Este passo não finge seletividade na extração. O recorte por domínio foi aplicado apenas ao modo `Somente Processamento`, porque é isso que o backend já suporta de forma objetiva hoje.

---

## Próximo passo recomendado

Agora o próximo passo de maior valor é cruzar catálogo + domínio para sugerir automaticamente qual preset analítico faz mais sentido reprocessar após uma extração parcial ou após uma lacuna detectada.
