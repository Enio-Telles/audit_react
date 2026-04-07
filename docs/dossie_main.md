# Dossiê como direção principal do `main`

Este documento registra a decisão de produto e manutenção de que a evolução principal do repositório `audit_react` passa a considerar o **dossiê por CNPJ** como eixo central do `main`.

## Decisão

A partir desta diretriz, o `main` deve priorizar:

- acesso ao dossiê sempre que houver CNPJ selecionado;
- reaproveitamento de consultas SQL já existentes;
- persistência das extrações para evitar duplicação;
- navegação didática por seções, tabelas, indicadores e visões derivadas;
- documentação curta, clara e atualizada junto com cada evolução.

## O que isso significa na prática

O dossiê deixa de ser tratado como recurso lateral ou experimental e passa a ser a **linha principal de integração funcional** do sistema.

Na manutenção do `main`, isso implica:

1. o contexto do CNPJ selecionado deve expor acesso direto ao dossiê;
2. o backend deve favorecer catálogo e cache de seções do dossiê;
3. o frontend deve tratar o dossiê como fluxo de navegação principal;
4. consultas já existentes têm prioridade sobre novas consultas duplicadas;
5. qualquer extração reaproveitável deve ser persistida e reutilizada.

## Regras de implementação

### 1. O dossiê nasce do CNPJ

Sem CNPJ selecionado, o dossiê pode ficar indisponível.

Com CNPJ selecionado, o acesso ao dossiê deve ser imediato e visível no fluxo principal da interface.

### 2. Persistência antes de repetição

A extração de dados do dossiê deve ser salva por combinação de:

- CNPJ;
- seção;
- parâmetros normalizados;
- versão da consulta efetivamente utilizada.

Se a mesma combinação já existir, a aplicação deve reutilizar o resultado em vez de salvar de novo.

### 3. SQL existente tem prioridade

Ao implementar uma nova seção do dossiê, a ordem recomendada é:

1. localizar consulta equivalente no catálogo atual;
2. reutilizar a consulta já existente;
3. criar nova consulta apenas quando não houver equivalente viável.

### 4. Arquivos curtos e modulares

A evolução do dossiê não deve inflar arquivos grandes já existentes.

Prefira estruturas por domínio, por exemplo:

- `frontend/src/features/dossie/...`
- `backend/services/dossie/...`
- `docs/dossie_*.md`

### 5. Documentação faz parte da entrega

Toda mudança relevante do dossiê deve vir acompanhada de documentação simples contendo:

- objetivo;
- arquivos impactados;
- regra de cache/reuso;
- limitações atuais.

## Impacto sobre decisões anteriores

Com essa diretriz, o `main` deve ser avaliado pela coerência com o fluxo do dossiê e do CNPJ.

Isso vale mais do que caminhos antigos, branches experimentais ou propostas que não convergem com:

- CNPJ como ponto de entrada;
- dossiê como fluxo principal;
- reutilização de consultas;
- persistência sem duplicação.

## Próximos passos de implementação

1. colocar acesso ao dossiê ao lado do CNPJ selecionado;
2. criar catálogo de seções do dossiê com aliases para SQL existente;
3. persistir extrações por chave reutilizável;
4. expor no frontend indicadores de dado reaproveitado vs. dado recém-extraído;
5. manter a documentação do `main` coerente com essa linha.
