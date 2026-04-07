# Estado atual do `main`

Este documento descreve **o estado operacional atual** do repositório `audit_react` e serve como referência rápida para manutenção.

## Objetivo

Evitar ambiguidade entre:

- material histórico;
- ideias de arquitetura que já foram descartadas;
- implementação realmente ativa no `main`.

## Estrutura considerada atual

A referência atual do projeto é a estrutura presente no próprio `main`, com foco em:

- `frontend/` para a aplicação React;
- `backend/` para a API e rotas ativas;
- `src/` para serviços, transformação e orquestração do pipeline.

## Pipeline oficial atual

A ordem de negócio atualmente considerada oficial é a que aparece no orquestrador principal:

1. `tb_documentos`
2. `item_unidades`
3. `itens`
4. `descricao_produtos`
5. `produtos_final`
6. `fontes_produtos`
7. `fatores_conversao`
8. `c170_xml`
9. `c176_xml`
10. `movimentacao_estoque`
11. `calculos_mensais`
12. `calculos_anuais`

Qualquer documentação ou branch que apresente outra estrutura deve ser tratada como **histórica** até nova validação explícita.

## Direção funcional vigente

As decisões de manutenção mais recentes para o `main` são:

- priorizar a versão **mais íntegra e coerente** do fluxo principal;
- evitar crescimento excessivo de arquivos grandes no frontend e backend;
- manter documentação didática e atualizada;
- preparar a integração do dossiê a partir do CNPJ selecionado;
- reutilizar consultas SQL existentes antes de criar novas consultas;
- persistir extrações por CNPJ e parâmetros para evitar duplicação.

## O que deve ser tratado como histórico

Os itens abaixo **não devem ser usados como verdade operacional automática** sem nova validação:

- branches antigos que usam estruturas divergentes, como `client/`, `server/` e `shared/`;
- documentação experimental de fluxos que não estão mais no núcleo ativo;
- propostas temporárias que não foram incorporadas ao `main`.

## Sobre a documentação de atomização

Qualquer material de atomização deve ser lido como **histórico ou experimental** enquanto não houver nova decisão formal de produto e manutenção no `main`.

Na prática:

- a referência operacional continua sendo o pipeline oficial listado acima;
- documentação antiga não substitui a implementação realmente ativa;
- decisões novas devem ser registradas em documentos curtos, objetivos e versionados no próprio `main`.

## Regras de manutenção recomendadas

### 1. Evitar arquivos longos

Ao evoluir o sistema, preferir módulos menores por domínio.

Exemplos desejáveis:

- `frontend/src/features/<dominio>/...`
- `backend/services/<dominio>/...`
- `docs/<tema>.md`

### 2. Reutilizar SQL antes de duplicar

Antes de adicionar uma consulta nova, verificar se já existe consulta equivalente no catálogo SQL ativo.

### 3. Documentar decisões junto com a mudança

Mudanças estruturais devem vir acompanhadas de documentação curta contendo:

- objetivo;
- arquivos impactados;
- regra de uso;
- limitações atuais.

### 4. Tratar branches auxiliares como descartáveis

Branches automáticos, experimentais ou com arquitetura divergente não devem competir com o `main` como referência do projeto.

## Próximos passos naturais

1. limpar resíduos de fluxos descartados do frontend;
2. integrar o acesso ao dossiê ao lado do CNPJ selecionado;
3. criar serviços modulares de dossiê com cache por CNPJ + seção + parâmetros;
4. manter a documentação do `main` como fonte de verdade.
