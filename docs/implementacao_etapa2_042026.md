# implementacao_etapa2_042026.md

## Objetivo

Registrar a segunda etapa efetiva de implementação do plano de abril/2026, com foco em:

- inventário operacional da UI atual;
- alinhamento substitutivo de `README.md`;
- alinhamento substitutivo do posicionamento do Dossiê;
- registro explícito das limitações do conector usado nesta rodada.

---

## O que foi implementado nesta etapa

### 1. Inventário real da `MainWindow`

Foi criado `docs/inventario_abas_mainwindow_042026.md`, contendo:

- abas reais confirmadas na `MainWindow`;
- subtabs reais de `Estoque`;
- destino funcional de cada área em:
  - Manutenção / T.I.
  - Documentos Fiscais
  - Análise Fiscal
  - EFD como lacuna ainda não implementada;
- paridade mínima exigida para a migração.

### 2. README alinhado criado em documento de substituição

Foi criado `docs/readme_alinhado_042026.md` para registrar a formulação correta de:

- runtime atual = PySide6;
- arquitetura-alvo = Tauri;
- frontend orientado ao analista fiscal;
- referência operacional baseada no plano unificado e na matriz de migração.

### 3. Dossiê alinhado criado em documento de substituição

Foi criado `docs/dossie_main_alinhado_042026.md` para registrar a decisão correta de que:

- o Dossiê não é mais o eixo único da navegação principal;
- o Dossiê passa a ser recurso contextual e transversal;
- seu encaixe natural fica em Análise Fiscal ou no contexto do CNPJ selecionado.

---

## Limitação técnica encontrada nesta etapa

O conector GitHub disponível nesta rodada permite:

- criar arquivos novos;
- buscar arquivos existentes;
- identificar o `sha` atual do arquivo existente.

Mas a função exposta de criação/atualização disponível aqui **não aceita o parâmetro `sha`** necessário para sobrescrever diretamente arquivos já versionados.

Consequência prática:

- não foi possível substituir fisicamente `README.md` e `docs/dossie_main.md` nesta rodada usando apenas o conector exposto;
- em vez disso, foram criados documentos canônicos de substituição em `docs/`;
- o alinhamento lógico e documental foi implementado, mas a troca física dos arquivos antigos ainda permanece pendente.

---

## O que ficou resolvido

- o projeto já tem documentação canônica suficiente para orientar corretamente a arquitetura atual;
- o conflito entre runtime atual e arquitetura-alvo ficou documentado de forma explícita;
- o conflito entre Dossiê como fluxo principal e navegação fiscal em três blocos ficou formalmente resolvido na documentação nova;
- a separação entre área fiscal e área técnica ficou sustentada por inventário real da UI.

---

## O que ainda não foi implementado nesta etapa

- substituição física de `README.md`;
- substituição física de `docs/dossie_main.md`;
- reorganização real da UI em três blocos fiscais;
- criação do bloco EFD na interface;
- isolamento físico da área técnica no frontend real;
- runtime Tauri ativo como principal.

---

## Próximo passo recomendado

O próximo passo técnico deve ser:

1. seguir com a separação documental e funcional entre área fiscal e área técnica no código e na navegação;
2. usar o inventário da `MainWindow` para definir a primeira leva de componentes/tabelas da migração;
3. manter como prioridade os módulos de maior valor fiscal já confirmados:
   - mov_estoque
   - tabela mensal
   - tabela anual
   - NFe Entrada
   - destaque de tabelas
