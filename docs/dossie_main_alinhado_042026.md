# Dossie - contrato e posicionamento alinhados

## Objetivo

Registrar a versão alinhada do posicionamento do Dossiê por CNPJ dentro da arquitetura fiscal definida em abril/2026.

Este documento existe porque o arquivo `docs/dossie_main.md` ainda reflete a diretriz anterior, em que o Dossiê era tratado como fluxo principal do frontend.

---

## Decisão atual

O Dossiê por CNPJ **não** deve mais ser tratado como eixo único da navegação principal do sistema.

A navegação principal do usuário deve ser organizada em:

- **EFD**
- **Documentos Fiscais**
- **Análise Fiscal**

Dentro dessa arquitetura:

- o Dossiê passa a ser um recurso **contextual e transversal**;
- sua posição natural fica dentro de **Análise Fiscal**, ou como acesso contextual associado ao CNPJ selecionado;
- ele não substitui os três blocos principais do frontend.

---

## O que continua válido no contrato do Dossiê

Continuam válidos e reaproveitáveis:

- cache canônico por `cnpj + secao + parametros normalizados + versao_consulta`;
- leitura de seções materializadas sem reexecutar Oracle quando houver cache válido;
- reaproveitamento de consultas SQL existentes;
- sidecars de metadata;
- estratégia cache-first;
- rastreabilidade auditável da origem do dado;
- artefatos analíticos reutilizáveis por seção.

Ou seja:

**o backend do Dossiê permanece ativo e relevante.**

O que muda é o seu enquadramento na experiência principal do usuário.

---

## Relação com os blocos fiscais

### EFD

- a EFD continua sendo área própria de escrituração;
- o Dossiê pode referenciar seções relacionadas, mas não deve substituir a navegação pura de EFD.

### Documentos Fiscais

- a consulta principal de documentos deve existir em módulo próprio;
- o Dossiê pode funcionar como painel consolidado ou atalho contextual.

### Análise Fiscal

- este é o melhor ponto de encaixe do Dossiê;
- ele pode servir como visão consolidada por CNPJ e recurso de investigação contextual;
- pode reaproveitar materializações já existentes sem redefinir a arquitetura principal.

---

## Regra operacional recomendada

1. selecionar o CNPJ;
2. navegar pelos blocos fiscais principais;
3. quando necessário, abrir o Dossiê como recurso contextual do contribuinte atual;
4. manter o Dossiê como apoio auditável e não como substituto da arquitetura principal de navegação.

---

## Regra de uso

Enquanto `docs/dossie_main.md` não for fisicamente substituído:

1. este documento deve ser usado como referência de posicionamento arquitetural do Dossiê;
2. o documento antigo deve ser lido como registro da diretriz anterior;
3. decisões novas devem seguir este alinhamento e o plano unificado de abril/2026.
