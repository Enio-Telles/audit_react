# dossie_enquadramento_042026.md

## Objetivo

Compatibilizar o Dossie por CNPJ com a nova diretriz de frontend fiscal sem perder o contrato backend ja implementado.

Este documento nao remove o Dossie.
Ele redefine seu enquadramento dentro da experiencia do usuario.

---

## Decisao de enquadramento

O Dossie por CNPJ deixa de ser tratado como eixo unico da navegacao principal do sistema.

A partir da diretriz de abril/2026:

- a navegacao principal do usuario deve ser organizada em:
  - EFD
  - Documentos Fiscais
  - Analise Fiscal
- o Dossie passa a ser um recurso contextual e transversal;
- seu acesso deve partir do contexto do CNPJ selecionado;
- sua posicao natural no frontend passa a ser dentro de **Analise Fiscal**, ou como acesso contextual associado ao contribuinte selecionado.

---

## O que permanece valido do contrato atual

Continuam validos:

- cache canonico por CNPJ + secao + parametros + versao de consulta;
- leitura de secoes materializadas sem reexecutar Oracle quando houver cache valido;
- reaproveitamento de consultas existentes;
- rastreabilidade auditavel da origem do dado;
- sidecars de metadata;
- estrategia cache-first;
- secoes baseadas em catalogo e artefatos analiticos reutilizaveis.

Ou seja:

**o backend do Dossie permanece ativo e reaproveitavel.**

O que muda e apenas seu enquadramento na experiencia principal do usuario.

---

## O que deixa de ser regra

Deixa de valer como diretriz principal de frontend a afirmacao de que:

- o Dossie deve ser o fluxo principal de navegacao da interface.

Essa orientacao conflita com a nova arquitetura de navegacao fiscal e por isso deve ser considerada superada para a camada de UX.

---

## Novo papel do Dossie

O Dossie deve funcionar como:

- visao consolidada por CNPJ;
- ponto de apoio para investigacao fiscal;
- recurso de consulta contextual dentro de Analise Fiscal;
- mecanismo de reaproveitamento de secoes materializadas;
- base auditavel para navegacao de secoes tecnicamente rastreaveis.

Ele nao deve substituir os tres blocos principais do frontend.

---

## Relacao com os tres blocos fiscais

### EFD

- nao deve ser absorvida pelo Dossie;
- deve continuar sendo uma area propria de escritutacao;
- o Dossie pode referenciar secoes relacionadas, mas nao deve redefinir a navegacao pura da EFD.

### Documentos Fiscais

- nao devem ser absorvidos pelo Dossie como navegacao principal;
- o Dossie pode servir como painel consolidado ou atalho contextual;
- a consulta principal de documentos deve existir em modulo proprio.

### Analise Fiscal

- aqui o Dossie se encaixa melhor;
- pode concentrar secoes consolidadas por CNPJ;
- pode servir como porta de entrada para investigacao contextual, sem substituir modulos especificos de cruzamento.

---

## Regra operacional recomendada

1. Selecionar CNPJ.
2. Navegar pelos blocos principais do frontend.
3. Quando necessario, abrir o Dossie como recurso contextual do CNPJ atual.
4. Manter o Dossie como apoio auditavel e nao como substituto da arquitetura principal de navegacao.

---

## Consequencia pratica para a migracao

Na migracao para o frontend alvo:

- o backend do Dossie deve ser preservado;
- as APIs e contratos de cache devem ser mantidos;
- a navegacao principal nao deve nascer centrada no Dossie;
- o Dossie deve ser encaixado como submodulo de Analise Fiscal ou como painel contextual do contribuinte.

---

## Observacao final

Este enquadramento nao trata o Dossie como legado a remover.
Trata-o como ativo funcional relevante que precisa ser reposicionado para coexistir com a nova arquitetura fiscal do frontend.
