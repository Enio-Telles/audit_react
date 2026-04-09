# Plano de evolução do Dossiê de Contatos — Agenda integrada por entidade

## Status em 2026-04-09

- a visualização integrada passou a existir no frontend em três tabelas: empresa, sócios e contadores
- a composição Polars agora preserva sócio atual e sócio antigo
- a composição Polars agora reaproveita a FAC como fonte complementar de telefone, email e endereço da empresa
- o `sql/dossie_contato.sql` foi alinhado parcialmente para incluir `EMPRESA_FAC_ATUAL`, `SOCIO_ANTIGO` e situação consolidada do contador
- a comparação histórica da seção `contato` passou a usar chave funcional agregada por entidade, reduzindo divergência artificial entre `composicao_polars` e `sql_consolidado`
- o resumo e o relatório técnico da comparação agora expõem também a estratégia de referência, a SQL de referência e a última contagem de chaves faltantes e extras
- o relatório técnico agora detalha amostras de chaves faltantes/extras e o delta de cobertura dos campos críticos de contato
- o relatório mestre agora usa esses sinais para classificar a prioridade operacional da seção `contato` por CNPJ
- foi criado um script operacional para gerar um painel markdown com o ranking dessas prioridades por CNPJ
- o levantamento técnico das fontes Oracle materializadas e candidatas foi documentado em `docs/dossie_contatos_fontes_integradas.md`
- o modo `sql_consolidado` avançou, mas ainda depende de validação funcional contra Oracle real para fechar convergência total com a composição Polars

---

## Objetivo

Transformar a seção `contato` do Dossiê em uma **agenda integrada de contatos** que relacione, para:

- **Empresa**
- **Sócios**
- **Contadores**

todos os:

- **telefones**
- **emails**
- **endereços**

encontrados em **todas as fontes disponíveis**, mantendo rastreabilidade, confirmação por fonte e visualização simples.

A ideia central é sair de uma leitura “linha bruta de parquet” e entregar uma **agenda operacional auditável**.

---

## Conceito da nova experiência

A seção `contato` passa a ter duas camadas complementares:

### 1. Camada operacional

Uma **agenda de contatos** organizada por entidade:

- empresa
- sócios
- contadores

Cada entidade funciona como uma “ficha” de agenda.

### 2. Camada auditável

Tabelas e evidências ficam na parte de baixo:

- consolidado
- contatos por fonte
- bruto
- metadata

---

## Definição da Agenda de Contatos

A agenda deve relacionar:

- quem é a entidade
- qual o vínculo dela com a empresa
- quais telefones foram encontrados
- quais emails foram encontrados
- quais endereços foram encontrados
- em quais fontes cada dado apareceu
- se o dado é confirmado, divergente ou único

A agenda não deve esconder variações.
Ela deve mostrar **todos os dados encontrados**, com organização suficiente para leitura rápida.

---

## Estrutura funcional da agenda

## Bloco A — Resumo superior

Manter os cards no topo com leitura geral.

Cada card por grupo deve mostrar:

- quantidade de entidades
- quantidade total de telefones encontrados
- quantidade total de emails encontrados
- quantidade total de endereços encontrados
- quantidade de entidades com conflito entre fontes
- quantidade de entidades sem contato
- data de atualização

Grupos:

- Empresa
- Sócios
- Contadores

---

## Bloco B — Agenda integrada de contatos

Abaixo dos cards, exibir três painéis fixos:

- **Agenda da Empresa**
- **Agenda dos Sócios**
- **Agenda dos Contadores**

Cada painel mostra uma **lista consolidada por entidade**, não por linha bruta.

---

## Estrutura visual de cada ficha da agenda

Cada ficha deve conter:

### Cabeçalho

- nome
- documento
- tipo de vínculo
- selo de status:
  - confirmado
  - parcial
  - divergente
  - sem contato

### Seção Telefones

Listar **todos os telefones encontrados**, agrupados por fonte.

Exemplo:

- Cadastro: (68) 99999-1111
- FAC atual: (68) 3222-1000
- NFe: (68) 99999-1111
- NFCe: (68) 98888-7777

### Seção Emails

Listar **todos os emails encontrados**, agrupados por fonte.

Exemplo:

- Cadastro: contato@empresa.com.br
- FAC atual: fiscal@empresa.com.br
- NFe: xml@empresa.com.br

### Seção Endereços

Listar **todos os endereços encontrados**, agrupados por fonte.

Exemplo:

- Cadastro: Rua X, 100, Centro, Rio Branco/AC
- FAC atual: Rua X, 100, Sala 2, Centro, Rio Branco/AC
- Requerimento: Av. Y, 800, Bosque, Rio Branco/AC

### Rodapé da ficha

- fontes envolvidas
- observações
- alertas de divergência ou ausência
- ação para ver evidências completas

---

## Regras da agenda

## 1. Agrupamento por entidade

Cada ficha da agenda deve ser consolidada por chave estável, nesta ordem:

1. `cpf_cnpj_referencia`
2. `nome_referencia`
3. `tipo_vinculo`

---

## 2. Grupos da agenda

### Empresa

Entram aqui:

- `EMPRESA_PRINCIPAL`
- `MATRIZ_RAIZ`
- `FILIAL_RAIZ`

### Sócios

Entram aqui:

- `SOCIO_ATUAL`

### Contadores

Entram aqui:

- `CONTADOR_EMPRESA`

### Fontes documentais complementares

Entradas como `EMAIL_NFE` não devem formar grupo isolado principal.
Elas entram como **evidência complementar** da agenda da empresa ou da entidade associada quando houver vínculo útil.

---

## 3. Regra principal: exibir tudo

A agenda deve exibir:

- **todos os telefones encontrados**
- **todos os emails encontrados**
- **todos os endereços encontrados**

Mesmo quando houver:

- repetição em várias fontes
- conflito entre fontes
- ausência em cadastro formal e presença só em documento fiscal

---

## 4. Deduplicação

Deduplicar por:

- tipo do dado
- valor normalizado
- fonte

Exemplo:

- o mesmo telefone vindo de Cadastro e NFe aparece uma vez como valor, mas com indicação:
  - confirmado por Cadastro + NFe

Se o mesmo valor aparecer com pequenas variações de formatação, normalizar antes.

---

## 5. Divergência

Quando houver mais de um valor diferente para o mesmo tipo, manter todos.

A ficha deve marcar:

- **divergência de telefone**
- **divergência de email**
- **divergência de endereço**

Critério:

- dois ou mais valores distintos vindos de fontes relevantes
- especialmente quando o conflito envolve fontes cadastrais formais

---

## 6. Ausência

Quando não houver dado para um tipo, mostrar explicitamente:

- sem telefone encontrado
- sem email encontrado
- sem endereço encontrado

---

## Visual sugerido da agenda

## Exemplo de ficha

**Empresa XYZ LTDA**
12.345.678/0001-90 | EMPRESA_PRINCIPAL

**Telefones encontrados**

- Cadastro: (68) 99999-1111
- FAC atual: (68) 3222-1000
- NFe: (68) 99999-1111

**Emails encontrados**

- Cadastro: contato@empresa.com.br
- NFe: xml@empresa.com.br

**Endereços encontrados**

- Cadastro: Rua X, 100, Centro, Rio Branco/AC
- Requerimento: Av. Y, 800, Bosque, Rio Branco/AC

**Fontes**
Cadastro | FAC atual | NFe | Requerimento

**Alertas**

- Endereço divergente entre fontes
- Email apenas documental

---

## Organização da parte inferior

A agenda fica em cima.
As tabelas ficam embaixo.

### Estrutura inferior recomendada

#### 1. Consolidado

Uma linha por entidade com:

- grupo
- nome
- documento
- quantidade de telefones
- quantidade de emails
- quantidade de endereços
- status de conflito

#### 2. Contatos por fonte

Uma linha por ocorrência encontrada:

- entidade
- grupo
- tipo do dado
- valor
- fonte
- origem
- tabela_origem
- confirmado_em_outras_fontes
- status

#### 3. Bruto

Linhas originais da seção contato.

#### 4. Metadata

- estratégia
- SQL principal
- cache
- comparação de estratégias
- arquivos de origem

---

## Mudanças necessárias no frontend

## 1. Mudar o layout do Dossiê

Hoje o detalhe está na lateral.
A nova experiência precisa ficar vertical.

### Ação

Colocar o detalhe da seção abaixo do grid de seções.

### Arquivos

- `frontend/src/features/dossie/components/DossieTab.tsx`
- `frontend/src/features/dossie/components/DossieDetailPanel.tsx`

---

## 2. Criar a agenda de contatos

A seção `contato` precisa ganhar um componente novo.

### Componentes sugeridos

- `DossieContatoAgenda.tsx`
- `DossieContatoAgendaGrupo.tsx`
- `DossieContatoAgendaFicha.tsx`
- `DossieContatoTabelaPorFonte.tsx`

### Papel de cada um

- `DossieContatoAgenda.tsx`: orquestra os grupos
- `DossieContatoAgendaGrupo.tsx`: renderiza empresa / sócios / contadores
- `DossieContatoAgendaFicha.tsx`: renderiza cada entidade da agenda
- `DossieContatoTabelaPorFonte.tsx`: renderiza a tabela inferior detalhada

---

## 3. Atualizar a seção de contato

Hoje `DossieContatoDetalhe.tsx` já faz agrupamento básico por vínculo e mostra alguns campos.

### Evolução

Ele deve deixar de ser uma listagem de registros quase brutos e passar a:

- consolidar entidades
- montar a agenda
- separar dados por:
  - telefones
  - emails
  - endereços
- organizar por fonte
- empurrar a parte tabular para baixo

### Arquivo principal

- `frontend/src/features/dossie/components/DossieContatoDetalhe.tsx`

---

## Mudanças necessárias no backend

## Situação atual

O builder da seção `contato` já suporta parcialmente:

- `telefone`
- `telefone_nfe_nfce`
- `email`
- `telefones_por_fonte`
- `emails_por_fonte`
- `fontes_contato`

Mas ainda falta um contrato equivalente e consistente para:

- **endereços por fonte**

Sem isso, a agenda nunca conseguirá listar endereços com o mesmo nível de qualidade que já existe para telefones e emails.

---

## Evolução do contrato de dados

## Novo objetivo do backend

A saída da seção `contato` deve permitir renderizar a agenda sem depender de parsing textual frágil.

### Estrutura ideal

Cada entidade consolidada deveria permitir acesso a:

- lista de telefones por fonte
- lista de emails por fonte
- lista de endereços por fonte
- fontes consolidadas
- alertas gerados
- registros originais associados

---

## Estrutura sugerida

```ts
type ContatoValorPorFonte = {
  tipo: "telefone" | "email" | "endereco";
  valor: string;
  fonte: string;
  origem_dado?: string | null;
  tabela_origem?: string | null;
  confirmadoEmOutrasFontes?: string[];
};

type AgendaContatoEntidade = {
  grupo: "empresa" | "socio" | "contador";
  tipoVinculo: string;
  nome: string;
  documento: string | null;

  telefones: ContatoValorPorFonte[];
  emails: ContatoValorPorFonte[];
  enderecos: ContatoValorPorFonte[];

  fontesEnvolvidas: string[];
  alertas: string[];
  registrosOriginais: Record<string, unknown>[];
};
```

---

## Mudanças concretas no builder

## Arquivo principal

- `src/interface_grafica/services/dossie_section_builder.py`

## Ações

1. Incluir `enderecos_por_fonte` no contrato final
2. Padronizar coleta de endereços de todas as fontes relevantes
3. Manter telefones e emails por fonte em estrutura mais forte
4. Evitar depender apenas de string formatada como:
   - `"Fonte A: valor1, valor2 | Fonte B: valor3"`

## Evolução recomendada

Além dos campos existentes, incluir:

- `enderecos_por_fonte`
- `telefones_lista`
- `emails_lista`
- `enderecos_lista`

---

## Mudanças complementares

## Backend

- `backend/routers/dossie.py`
  - expor melhor leitura do consolidado se necessário

## Extração / sync

- `src/interface_grafica/services/dossie_extraction_service.py`
  - garantir metadata coerente
  - manter compatibilidade com estratégia atual
  - preservar histórico de comparação

---

## Fases de implementação

## Fase 1 — Layout e conceito de agenda

1. Mover detalhe da lateral para baixo
2. Criar a estrutura visual da agenda
3. Separar claramente:
   - agenda em cima
   - tabelas embaixo

## Fase 2 — Agenda completa no frontend

4. Consolidar entidades no frontend
5. Exibir todos os telefones, emails e endereços
6. Organizar por grupo
7. Exibir confirmação e divergência
8. Criar tabela de contatos por fonte

## Fase 3 — Fortalecer contrato do backend

9. Padronizar `enderecos_por_fonte`
10. Melhorar estrutura de listas por fonte
11. Diminuir parsing textual frágil
12. Garantir consistência entre fontes

## Fase 4 — Testes

13. Testes do builder da seção contato
14. Testes do router do Dossiê
15. Testes do componente da agenda no frontend
16. Testes de cenários:

- sem contatos
- contatos repetidos
- conflito entre fontes
- só email documental
- múltiplos endereços

---

## Critérios de aceite

A agenda estará pronta quando for possível:

- abrir a seção contato e enxergar **empresa, sócios e contadores** como agenda
- ver **todos os telefones, emails e endereços** encontrados
- identificar **de qual fonte veio cada dado**
- perceber rapidamente se um dado está:
  - confirmado
  - parcial
  - divergente
  - ausente
- consultar as tabelas detalhadas na parte de baixo
- não depender mais de detalhe lateral para uso principal

---

## Recomendação prática

A melhor ordem é:

### Entrega 1

- mudar layout do Dossiê
- criar agenda visual no frontend
- já listar todos os telefones e emails por fonte
- colocar tabelas embaixo

### Entrega 2

- reforçar backend com `enderecos_por_fonte`
- padronizar listas estruturadas
- melhorar alertas e divergência
- consolidar testes

---

## Arquivos principais

### Frontend

- `frontend/src/features/dossie/components/DossieTab.tsx`
- `frontend/src/features/dossie/components/DossieDetailPanel.tsx`
- `frontend/src/features/dossie/components/DossieSectionDetail.tsx`
- `frontend/src/features/dossie/components/DossieContatoDetalhe.tsx`

### Novos componentes sugeridos

- `frontend/src/features/dossie/components/DossieContatoAgenda.tsx`
- `frontend/src/features/dossie/components/DossieContatoAgendaGrupo.tsx`
- `frontend/src/features/dossie/components/DossieContatoAgendaFicha.tsx`
- `frontend/src/features/dossie/components/DossieContatoTabelaPorFonte.tsx`

### Backend

- `backend/routers/dossie.py`

### Builder / extração

- `src/interface_grafica/services/dossie_section_builder.py`
- `src/interface_grafica/services/dossie_extraction_service.py`
