# Sugestão automática de abordagem no frontend

## Objetivo

Este passo usa o estado do catálogo materializado para tornar a escolha de abordagem mais guiada e menos manual.

---

## O que foi implementado

### Banner de prontidão da extração

Arquivo:

- `frontend/src/components/pipeline/ExtractionReadinessBanner.tsx`

O banner consulta a disponibilidade de datasets para o CNPJ em foco e:

- sugere uma abordagem automaticamente;
- alerta quando `Somente Processamento` parece fraco para o estado atual;
- oferece atalho para abrir a aba de catálogo.

### Integração com o catálogo

Arquivo:

- `frontend/src/components/layout/LeftPanel.tsx`

Mudanças:

- o `LeftPanel` agora busca `getDatasetCatalogForCnpj(cnpj)`;
- a sugestão automática aparece junto do seletor de abordagem;
- após fim do pipeline, o painel oferece botão para abrir o catálogo do CNPJ executado.

---

## Heurística aplicada

A sugestão hoje é simples e coerente com o estado atual do sistema:

- sem base materializada relevante → sugerir `Extrair + Processar`
- base materializada e poucos/nenhum analítico → sugerir `Somente Processamento`
- base muito parcial → sugerir `Extrair + Processar`

Além disso, o frontend alerta quando o usuário escolhe `Somente Processamento` com base materializada muito fraca.

---

## Ganho prático

Isso aproxima a UX do estado real do ambiente:

- reduz tentativa e erro;
- evita escolher `process` quando o CNPJ ainda não tem base suficiente;
- encurta o caminho entre execução e inspeção do resultado no catálogo.

---

## Próximo passo recomendado

Agora o próximo passo de maior valor é transformar essa orientação em ação operacional mais forte, por exemplo:

- abrir automaticamente o dataset crítico ausente no catálogo;
- sugerir quais famílias SQL faltam para completar a base;
- acoplar reprocessamento seletivo por domínio.
