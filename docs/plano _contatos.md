# Plano de Implementacao: Modulo de Contatos e Localizacao Expandido

Este plano revisa o modulo de contatos do Dossie para unificar, em um unico fluxo de extracao, os contatos da empresa principal, a enumeracao de suas filiais por CNPJ raiz e os contatos dos socios atuais.

Plano mestre relacionado: `docs/plano_dossie_integral.md`

Analise de mapeamento considerada:
foram verificados os artefatos em `dados/referencias/referencias/mapeamento` para levantar as tabelas candidatas a dados de contador, contato e identificacao.

## 1. Objetivo

Consolidar em uma secao `contato` do Dossie os seguintes grupos de dados:

- Empresa principal consultada pelo CNPJ.
- Filiais da mesma raiz de CNPJ.
- Contadores vinculados a empresa.
- Socios atuais vinculados a empresa principal.
- Emails recentes observados em notas fiscais da empresa principal.
- Telefones observados em NFe e NFCe que possam ser reconciliados com o contador da empresa.

O objetivo funcional e permitir leitura auditavel da rede de contato ligada ao contribuinte, com rastreabilidade clara do tipo de vinculo de cada registro.

Regra adicional obrigatoria:
cada dado exibido ou persistido na secao `contato` deve referenciar sua fonte de origem de forma auditavel, idealmente com a tabela ou view de origem no banco, alem do `sql_id`, parquet ou dataset compartilhado que viabilizou a composicao.

## 2. Escopo Funcional

O fluxo `contato` deve responder a duas perguntas distintas:

1. Quais sao os contatos diretamente associados ao CNPJ consultado.
2. Quais outras entidades da mesma raiz de CNPJ, quais contadores e quais socios atuais ampliam a malha de contato analisavel.

Para evitar ambiguidade na leitura, cada linha retornada deve indicar explicitamente:

- `tipo_vinculo`
- `cnpj_consultado`
- `cnpj_raiz`
- `cpf_cnpj_referencia`
- `nome_referencia`
- `origem_dado`
- `tabela_origem` ou referencia equivalente quando a origem vier diretamente de tabela/view do banco

## 3. Hierarquia e Fontes de Dados

### Nivel 1: Entidade Principal

- Origem: `SITAFE.SITAFE_PESSOA` e `SITAFE.SITAFE_HISTORICO_CONTRIBUINTE`.
- Dados esperados: endereco fiscal, telefone, email principal, identificacao cadastral e situacao.
- Papel: representar o contato base do CNPJ consultado.

### Nivel 2: Filiais da Mesma Raiz de CNPJ

- Origem: tabelas cadastrais do contribuinte com filtro pelo CNPJ raiz.
- Regra: o CNPJ raiz deve ser derivado dos 8 primeiros digitos do CNPJ consultado.
- Decisao congelada: enumerar todas as filiais encontradas para a raiz, e nao apenas as ativas.
- Dados esperados: CNPJ completo da filial, indicacao matriz/filial, nome empresarial, situacao cadastral, endereco, telefone e email quando disponiveis.
- Papel: enumerar todas as empresas irmas da mesma raiz para identificar dispersao geografica, reutilizacao de contatos e diferencas cadastrais.

### Nivel 3: Socios Atuais

- Origem: `SITAFE.SITAFE_HISTORICO_SOCIO`, filtrando somente socios ativos na fotografia atual.
- Regra sugerida: manter o filtro funcional existente por `it_in_ultima_fac = '9'`, salvo confirmacao posterior de outra regra de negocio.
- Papel: identificar os CPFs/CNPJs dos socios atuais ligados ao CNPJ consultado.

### Nivel 4: Contadores da Empresa

- Origem principal identificada no mapeamento:
  `SITAFE.SITAFE_HISTORICO_CONTRIBUINTE`, com destaque para `GR_IDENT_CONTADOR`, `IT_IN_LIVRO_CONTADOR`, `IT_IN_CONTADOR_EMPREGADO` e `SP_CONTADOR_INSC_ULTIMO`.
- Origem de contato associada:
  `SITAFE.SITAFE_PESSOA`, com `GR_IDENTIFICACAO`, nome, endereco, telefone, email e campos de CRC.
- Origens de fallback identificadas no mapeamento:
  `SITAFE.SITAFE_RASCUNHO_FAC` e `SITAFE.SITAFE_REQ_INSCRICAO`, que possuem nome do contador, CRC e campos diretos de endereco/telefone/email.
- Apoio analitico identificado:
  `BI.DM_CONTRIBUINTE` e `BI.DM_PESSOA`, ambos com `CO_CNPJ_CPF_CONTADOR`, uteis para reconciliacao e enriquecimento.
- Papel: capturar os contatos do contador responsavel pela escrituracao ou cadastro do contribuinte, preservando o tipo de vinculo e o canal de origem do dado.
- Decisao congelada:
  FAC passa a ser a referencia principal de consolidacao do contador.
  Isso nao implica ocultar fontes complementares; emails e telefones vindos de `SITAFE_PESSOA`, `SITAFE_RASCUNHO_FAC`, `SITAFE_REQ_INSCRICAO` ou NFe/NFCe reconciliadas devem continuar visiveis de forma complementar e auditavel.

### Nivel 5: Contatos dos Socios

- Origem: busca em `SITAFE.SITAFE_PESSOA` para cada CPF/CNPJ identificado no nivel de socios.
- Dados esperados: endereco, telefone, email e demais campos cadastrais de contato.
- Papel: ampliar a analise de localizacao e meios de contato dos socios atuais.
- Decisao congelada: a exibicao de socios pessoa fisica esta permitida no contexto fiscal desta secao.

### Nivel 6: Inteligencia Dinamica por Notas Fiscais da Empresa

- Origem: `BI.FATO_NFE_DETALHE`.
- Regra: manter esta camada restrita a empresa principal, evitando explosao de custo para filiais e socios.
- Dados esperados: emails observados recentemente em notas de entrada e saida.
- Papel: complementar o cadastro formal com sinais operacionais recentes.

### Nivel 7: Telefones Dinamicos do Contador em NFe e NFCe

- Origem mapeada:
  `BI.FATO_NFE_DETALHE`, `BI.FATO_NFCE_DETALHE`, `BI.DM_NFE_NFCE_CHAVE_ACESSO` e `BI.VW_FISC_NFE_CABECALHO_XML`.
- Campos confirmados no mapeamento:
  `CO_EMITENTE`, `CO_DESTINATARIO`, `CNPJ_EMIT`, `CNPJ_CPF_DEST`, `FONE_EMIT`, `FONE_DEST`.
- Regra conservadora:
  so considerar telefone de nota como telefone do contador quando o CPF/CNPJ do contador puder ser reconciliado com emitente ou destinatario do documento.
- Papel: enriquecer o bloco do contador com telefones observados operacionalmente em NFe e NFCe, sem substituir o cadastro formal.

## 4. Estrutura Proposta para o SQL

Arquivo alvo: `sql/dossie_contato.sql`

Sugestao de organizacao em CTEs:

1. `CTE_PARAMETROS`
   - Normaliza `:CNPJ` e deriva `:CNPJ_RAIZ`.
2. `CTE_CONTRIBUINTE`
   - Carrega os dados de contato do CNPJ consultado.
3. `CTE_FILIAIS_RAIZ`
   - Enumera todas as filiais e a matriz vinculadas ao mesmo CNPJ raiz.
4. `CTE_CONTATOS_FILIAIS`
   - Enriquecimento de endereco, telefone, email e situacao das filiais.
5. `CTE_CONTADOR_EMPRESA`
   - Resolve o contador vinculado a empresa a partir da FAC atual e aplica fallbacks de cadastro quando necessario.
6. `CTE_CONTATOS_CONTADOR`
   - Busca nome, CRC, endereco, telefone e email do contador.
7. `CTE_SOCIOS_IDS`
   - Resolve os identificadores dos socios atuais.
8. `CTE_CONTATOS_SOCIOS`
   - Busca os contatos dos socios identificados.
9. `CTE_EMAILS_NFE`
   - Captura emails recentes da operacao fiscal da empresa principal.
10. `CTE_FONES_CONTADOR_NFE_NFCE`
   - Busca telefones em NFe e NFCe para o contador quando houver reconciliacao por CPF/CNPJ.
11. `CTE_RESULTADO_FINAL`
   - Consolida tudo em `UNION ALL`, preservando colunas padrao e rastreabilidade.

## 5. Contrato de Saida Recomendado

Cada registro retornado pelo SQL deve seguir um formato coerente para facilitar uso no backend e no frontend:

- `tipo_vinculo`
  Valores sugeridos: `EMPRESA_PRINCIPAL`, `MATRIZ_RAIZ`, `FILIAL_RAIZ`, `CONTADOR_EMPRESA`, `SOCIO_ATUAL`, `EMAIL_NFE`.
- `cnpj_consultado`
- `cnpj_raiz`
- `cpf_cnpj_referencia`
- `nome_referencia`
- `crc_contador`
- `endereco`
- `telefone`
- `telefone_nfe_nfce`
- `email`
- `situacao_cadastral`
- `indicador_matriz_filial`
- `origem_dado`
- `tabela_origem`
- `telefones_por_fonte`
- `emails_por_fonte`
- `fontes_contato`
- `ordem_exibicao`

Observacao importante:
o campo `tipo_vinculo` precisa distinguir claramente quando a linha representa a empresa consultada e quando representa apenas outra entidade da mesma raiz.

Regras finais de contrato:

- colunas oficiais congeladas: `tipo_vinculo`, `cnpj_consultado`, `cnpj_raiz`, `cpf_cnpj_referencia`, `nome_referencia`, `crc_contador`, `endereco`, `telefone`, `telefone_nfe_nfce`, `email`, `telefones_por_fonte`, `emails_por_fonte`, `fontes_contato`, `situacao_cadastral`, `indicador_matriz_filial`, `origem_dado`, `tabela_origem`, `ordem_exibicao`.
- valores oficiais de `tipo_vinculo`: `EMPRESA_PRINCIPAL`, `MATRIZ_RAIZ`, `FILIAL_RAIZ`, `CONTADOR_EMPRESA`, `SOCIO_ATUAL`, `EMAIL_NFE`.
- ordenacao oficial do resultado: `ordem_exibicao`, depois `tipo_vinculo`, depois `nome_referencia`, depois `cpf_cnpj_referencia`.
- ordem funcional oficial dos blocos: `EMPRESA_PRINCIPAL=10`, `MATRIZ_RAIZ=20`, `FILIAL_RAIZ=25`, `CONTADOR_EMPRESA=30`, `SOCIO_ATUAL=40`, `EMAIL_NFE` vindo de `NFe.sql=50`, `EMAIL_NFE` vindo de `NFCe.sql=55`.
- os campos `telefones_por_fonte`, `emails_por_fonte` e `fontes_contato` sao obrigatorios para `CONTADOR_EMPRESA` quando houver complementaridade entre fontes, justamente para nao perder rastreabilidade funcional.

## 6. Regras de Negocio e Performance

### Regras de negocio

- A enumeracao de filiais deve considerar a raiz do CNPJ consultado, nao apenas os estabelecimentos com contato preenchido.
- A enumeracao por raiz deve incluir todas as filiais encontradas, independentemente de estarem ativas ou nao.
- A ausencia de telefone ou email em uma filial nao deve excluir a filial do resultado.
- A ausencia de telefone ou email do contador nao deve excluir o contador do resultado quando houver identificacao valida do vinculo.
- Os socios devem continuar limitados ao conjunto atual, evitando historico completo sem necessidade.
- Emails de notas fiscais devem permanecer restritos ao CNPJ consultado, nao se expandindo automaticamente para filiais.
- Quando houver mais de uma fonte para o contador, usar FAC como referencia principal de consolidacao e manter exibicao complementar de contatos adicionais das demais fontes.
- Telefones de NFe e NFCe so podem enriquecer o contador quando houver reconciliacao por CPF/CNPJ; CRC isolado nao basta para inferir o telefone na nota.

### Regras de performance

- Derivar o CNPJ raiz uma unica vez em CTE propria.
- Filtrar cedo pelas chaves de contribuinte relacionadas ao CNPJ raiz.
- Evitar recursividade aberta.
- Priorizar `UNION ALL` em vez de `UNION` quando a deduplicacao nao fizer parte da regra funcional.
- Se a base de filiais ficar volumosa, prever ordenacao deterministica sem materializacao excessiva.

## 7. Backend

- Desenvolver `sql/dossie_contato.sql` com os niveis descritos acima.
- Configurar a secao `contato` em `src/interface_grafica/services/dossie_catalog.py`.
- Mapear o SQL correspondente em `src/interface_grafica/services/dossie_aliases.py`.
- Garantir que o retorno persistido preserve a enumeracao de filiais e o `tipo_vinculo`.
- Garantir que o retorno persistido preserve tambem a identificacao e os contatos do contador quando houver vinculo.
- Se houver necessidade de filtros futuros, manter os binds em padrao Oracle via `:CNPJ` e derivados internos no proprio SQL.
- Antes de qualquer nova consulta Oracle, verificar se os datasets de empresa, filial, contador, socio, NFe e NFCe ja existem materializados para reaproveitamento.
- Priorizar a montagem da secao `contato` por joins em Polars sobre datasets canonicos compartilhados.

Status atual da implementacao:

- A secao `contato` ja foi integrada ao catalogo do Dossie.
- `sql/dossie_contato.sql` ja foi criado como contrato consolidado inicial.
- A implementacao atual segue usando composicao Polars sobre datasets reutilizados e SQLs existentes como fluxo principal, mas o builder ja aceita o contrato direto de `sql/dossie_contato.sql` quando ele estiver disponivel no dataset da secao.
- O backend agora tambem aceita ativacao controlada do SQL consolidado por parametro (`usar_sql_consolidado=true`) durante o sync da secao `contato`, preservando o fluxo Polars atual como comportamento padrao.
- O frontend do Dossie agora expoe essa alternancia de forma explicita, permitindo validar o contrato consolidado sem perder o caminho padrao de composicao por datasets compartilhados.
- Filiais por raiz sao reaproveitadas a partir de `dados_cadastrais_<cnpj>.parquet` ja existentes no workspace.
- Contador, socios e sinais de NFe/NFCe entram por composicao de datasets reaproveitados ou SQLs existentes com `cache-first`.
- O backend ja expoe leitura detalhada do parquet materializado da secao `contato`, sem nova consulta Oracle.
- O sync da secao `contato` agora evita persistir um novo parquet quando o resultado final for equivalente ao de um cache canonico ja existente da mesma secao e estrategia, reaproveitando o artefato materializado anterior.
- O frontend ja possui visualizacao especializada da secao `contato`, com agrupamento por `tipo_vinculo`, destaque da empresa principal e indicacao de telefones observados em NFe/NFCe.
- A composicao atual da secao `contato` ja materializa `tabela_origem` por linha para empresa, filiais, contador, socios e sinais de NFe/NFCe.
- A consolidacao do contador agora usa `dossie_historico_fac.sql` como referencia principal quando houver correspondencia funcional, preservando `dossie_contador.sql`, `dossie_rascunho_fac_contador.sql` e `dossie_req_inscricao_contador.sql` como fontes complementares auditaveis.
- A secao `contato` passou a materializar `emails_por_fonte`, `telefones_por_fonte` e `fontes_contato` no bloco `CONTADOR_EMPRESA`, mantendo todos os emails e telefones observados nas fontes consolidadas.
- A secao `contato` agora tambem consulta `dossie_rascunho_fac_contador.sql` e `dossie_req_inscricao_contador.sql` como fallback adicional do contador, com `tabela_origem` explicita para `SITAFE.SITAFE_RASCUNHO_FAC` e `SITAFE.SITAFE_REQ_INSCRICAO`.
- A regra atual de reconciliacao para `telefone_nfe_nfce` ficou congelada em modo conservador: o telefone so e anexado ao contador quando o documento fiscal trouxer CPF/CNPJ completo e esse identificador coincidir exatamente com o CPF/CNPJ do contador; documentos incompletos, truncados ou nao numericos sao descartados do enriquecimento.
- A composicao Polars da secao `contato` agora tambem consome `dossie_filiais_raiz.sql`, deixando de depender apenas da existencia de pastas irmas em `dados/CNPJ` para enumerar matriz e filiais da mesma raiz.
- O reuso de `NFe.sql` e `NFCe.sql` passou a preferir `shared_sql` atual ao legado, e o sync do Dossie agora promove automaticamente artefatos antigos para o cache SQL compartilhado quando a referencia vigente ainda nao existir.
- Depois desses ajustes e de novas execucoes reais com Oracle, a secao `contato` passou a registrar convergencia funcional total entre `composicao_polars` e `sql_consolidado` nos CNPJs `37671507000187` e `84654326000394`.
- A secao `contato` agora tambem grava sidecar de metadata do parquet final, historico JSONL de comparacoes entre estrategias e relatorio tecnico markdown por CNPJ, reforcando a trilha de auditoria funcional.
- O resumo da secao no backend e no frontend ja expoe estrategia da ultima materializacao, SQL principal, status de convergencia com a estrategia alternada e magnitude de chaves faltantes/extras quando houver comparacao disponivel.

Decisoes ja congeladas na implementacao:

- A regra de reconciliacao do telefone do contador em NFe/NFCe ficou congelada em modo estrito por CPF/CNPJ completo e identico ao do contador.
- A composicao `composicao_polars` permanece como caminho padrao da secao `contato`; `sql_consolidado` fica disponivel como caminho controlado para validacao e auditoria.
- A materializacao final da secao prioriza reaproveitamento de cache canonico equivalente quando o conteudo ja existir, evitando gerar novo parquet redundante.

## 8. Frontend

- Exibir a secao de contatos como mini-tabela ou tabela virtualizada.
- Permitir agrupamento visual por `tipo_vinculo`.
- Dar destaque para a empresa principal consultada.
- Exibir as filiais agrupadas por CNPJ raiz, com ordenacao previsivel.
- Exibir o contador da empresa como grupo proprio ou destaque funcional entre empresa e socios.
- Permitir identificar rapidamente filiais sem telefone ou email.

Status atual da implementacao:

- A aba do Dossie agora permite abrir a leitura detalhada da secao materializada.
- A secao `contato` ja e exibida em componente proprio, organizado por grupos de vinculo.
- O fluxo atual ja marca, com sinal visual dedicado, filiais e contadores sem contato cadastral preenchido.

## 9. Pontos de Atencao

### Privacidade e acesso

- A exibicao de enderecos de socios pessoa fisica exige avaliacao de permissao de acesso no contexto fiscal.
- O plano parte de uma postura conservadora: a documentacao preve a extracao, mas a liberacao operacional depende das permissoes adequadas.

### Risco funcional

- Empresas com muitas filiais na mesma raiz podem ampliar significativamente o volume retornado.
- Se a regra de negocio exigir apenas filiais ativas, isso precisa ser explicitado antes da implementacao para nao alterar o significado funcional da busca.
- O vinculo do contador pode vir por identificacao, por CRC ou por campos de fallback em rascunho/requerimento; isso exige regra clara de prioridade para nao duplicar linhas.

### Risco de duplicacao de extracao

- A secao `contato` nao deve criar uma nova extracao Oracle para dados que ja existam em Parquet para o mesmo CNPJ, CNPJ raiz, CPF/CNPJ de contador ou CPF/CNPJ de socio.
- Sempre que a informacao puder ser conectada por `GR_IDENTIFICACAO`, `GR_IDENT_CONTADOR`, `CO_CNPJ_CPF_CONTADOR`, `CO_EMITENTE` ou `CO_DESTINATARIO`, a composicao deve ocorrer em Polars.
- O caminho padrao da secao `contato` fica congelado como `composicao_polars` sobre datasets compartilhados; `sql_consolidado` continua existindo apenas como modo controlado de auditoria, validacao e comparacao funcional.
- Nova extracao Oracle so e permitida quando o `cache-first` nao localizar dataset materializado suficiente para o bloco necessario, ou quando o fluxo solicitar explicitamente o modo consolidado.

## 10. Tabelas Mapeadas Relevantes para a Consulta

As principais evidencias encontradas em `dados/referencias/referencias/mapeamento` foram:

- `SITAFE.SITAFE_HISTORICO_CONTRIBUINTE`
  Campos relevantes: `GR_IDENT_CONTADOR`, `IT_IN_LIVRO_CONTADOR`, `IT_IN_CONTADOR_EMPREGADO`, `IT_NU_RESPONSAVEL_EMPRESA`, `SP_CONTADOR_INSC_ULTIMO`.
- `SITAFE.SITAFE_PESSOA`
  Campos relevantes: `GR_IDENTIFICACAO`, `IT_NO_PESSOA`, `IT_NU_TELEFONE`, `IT_CO_CORREIO_ELETRONICO`, `IT_TX_LOGRADOURO_CORRESP`, `IT_NU_TELEFONE_CORRESP`, `IT_CO_CORREIO_ELETRO_CORRESP`, `GR_NUMERO_CRC`, `SP_CRC_ULTIMO`.
- `SITAFE.SITAFE_RASCUNHO_FAC`
  Campos relevantes: `GR_IDENT_CONTADOR`, `IT_NO_CONTADOR`, `IT_TX_LOGRADOURO_CONTADOR`, `IT_NU_DDD_CONTADOR`, `IT_NU_TELEFONE_CONTADOR`, `IT_CO_CORREIO_ELETRO_CONTADOR`, `GR_NUMERO_CRC`.
- `SITAFE.SITAFE_REQ_INSCRICAO`
  Campos relevantes: `GR_IDENTIFICACAO_CONTADOR`, `IT_NO_CONTADOR`, `IT_NU_CRC`.
- `BI.DM_CONTRIBUINTE`
  Campo relevante: `CO_CNPJ_CPF_CONTADOR`.
- `BI.DM_PESSOA`
  Campo relevante: `CO_CNPJ_CPF_CONTADOR`.
- `BI.FATO_NFE_DETALHE`
  Campos relevantes: `CO_EMITENTE`, `CO_DESTINATARIO`, `FONE_EMIT`, `FONE_DEST`, `EMAIL_DEST`.
- `BI.FATO_NFCE_DETALHE`
  Campos relevantes: `CO_EMITENTE`, `CO_DESTINATARIO`, `FONE_EMIT`, `FONE_DEST`.
- `BI.DM_NFE_NFCE_CHAVE_ACESSO`
  Campos relevantes: `CO_EMITENTE`, `CO_DESTINATARIO`, `FONE_EMIT`, `FONE_DEST`.
- `BI.VW_FISC_NFE_CABECALHO_XML`
  Campos relevantes: `CNPJ_EMIT`, `CNPJ_CPF_DEST`, `FONE`, `FONE_DEST`.

Interpretacao tecnica:

- A chave mais promissora para o vinculo principal do contador e `GR_IDENT_CONTADOR` na FAC atual.
- A chave mais promissora para enriquecimento de contato e `GR_IDENTIFICACAO` em `SITAFE_PESSOA`.
- `SITAFE_RASCUNHO_FAC` e `SITAFE_REQ_INSCRICAO` devem ser tratados como fallback ou apoio, nao como fonte primaria, salvo ausencia de registro consistente na FAC atual.
- Os telefones de NFe e NFCe devem ser tratados como enriquecimento dinamico do contador, nunca como fonte primaria de identificacao.

## 11. Estrategia de Consulta Recomendada para o Contador

Ordem sugerida para resolver o contador da empresa:

1. Localizar a FAC atual do contribuinte em `SITAFE_HISTORICO_CONTRIBUINTE`.
2. Ler `GR_IDENT_CONTADOR` como chave primaria do contador.
3. Tentar join de `GR_IDENT_CONTADOR` com `SITAFE_PESSOA.GR_IDENTIFICACAO`.
4. Se o join nao devolver contato suficiente, aplicar fallback por `SITAFE_RASCUNHO_FAC.GR_IDENT_CONTADOR`.
5. Se ainda assim nao houver dados suficientes, usar `SITAFE_REQ_INSCRICAO.GR_IDENTIFICACAO_CONTADOR`.
6. Usar `BI.DM_CONTRIBUINTE.CO_CNPJ_CPF_CONTADOR` e `BI.DM_PESSOA.CO_CNPJ_CPF_CONTADOR` como apoio de reconciliacao, nao como origem primaria.

Prioridade de campos sugerida:

- Nome do contador:
  `SITAFE_PESSOA.IT_NO_PESSOA` -> `SITAFE_RASCUNHO_FAC.IT_NO_CONTADOR` -> `SITAFE_REQ_INSCRICAO.IT_NO_CONTADOR`
- CRC:
  `SITAFE_PESSOA.GR_NUMERO_CRC` -> `SITAFE_PESSOA.SP_CRC_ULTIMO` -> `SITAFE_RASCUNHO_FAC.GR_NUMERO_CRC` -> `SITAFE_REQ_INSCRICAO.IT_NU_CRC`
- Telefone:
  `SITAFE_PESSOA.IT_NU_TELEFONE` -> `SITAFE_PESSOA.IT_NU_TELEFONE_CORRESP` -> `SITAFE_RASCUNHO_FAC.IT_NU_TELEFONE_CONTADOR` -> `SITAFE_REQ_INSCRICAO.IT_NU_TELEFONE`
- Email:
  `SITAFE_PESSOA.IT_CO_CORREIO_ELETRONICO` -> `SITAFE_PESSOA.IT_CO_CORREIO_ELETRO_CORRESP` -> `SITAFE_RASCUNHO_FAC.IT_CO_CORREIO_ELETRO_CONTADOR`
- Endereco:
  `SITAFE_PESSOA` -> `SITAFE_PESSOA` correspondencia -> `SITAFE_RASCUNHO_FAC` campos do contador

Regra de rastreabilidade recomendada:

- Cada linha de contador deve carregar `origem_dado` indicando claramente se veio de `FAC_ATUAL`, `PESSOA`, `RASCUNHO_FAC`, `REQ_INSCRICAO` ou `BI_RECONCILIACAO`.
- Sempre que possivel, a linha tambem deve registrar a tabela ou view de banco correspondente, como `SITAFE.SITAFE_HISTORICO_CONTRIBUINTE`, `SITAFE.SITAFE_PESSOA`, `SITAFE.SITAFE_RASCUNHO_FAC`, `SITAFE.SITAFE_REQ_INSCRICAO`, `BI.FATO_NFE_DETALHE` ou `BI.FATO_NFCE_DETALHE`.
- Quando o telefone do contador vier de nota fiscal, `origem_dado` deve distinguir `NFE_EMIT`, `NFE_DEST`, `NFCE_EMIT` ou `NFCE_DEST`.

## 12. TODO de Reuso Eficiente para a Secao `contato`

Objetivo:
montar a secao `contato` com o maximo possivel de reaproveitamento de datasets ja extraidos, evitando novas consultas Oracle quando a informacao ja existir no workspace.

### TODO especifico

- [x] Definir quais blocos da secao `contato` podem ser 100% montados a partir de datasets ja extraidos.
- [x] Reaproveitar dados cadastrais da empresa principal antes de nova extracao.
- [x] Reaproveitar datasets de filiais por `cnpj_raiz` quando ja materializados.
- [x] Reaproveitar dados de contador por `GR_IDENT_CONTADOR`, `GR_IDENTIFICACAO` e `CO_CNPJ_CPF_CONTADOR`.
- [x] Reaproveitar dados de socios por `GR_IDENTIFICACAO` ou CPF/CNPJ ja materializado.
- [x] Reaproveitar telefones e emails de NFe/NFCe ja materializados antes de nova consulta de notas.
- [x] Padronizar a secao `contato` para atuar como composicao Polars sobre datasets compartilhados, e nao como extracao Oracle independente.
- [x] Persistir apenas o resultado consolidado da secao quando ele representar uma composicao nova; evitar salvar duplicatas do mesmo dataset base.

Regras finais de montagem por bloco:

- `EMPRESA_PRINCIPAL`: deve ser montado por reuso de `dados_cadastrais.sql` ou cache canonicamente equivalente.
- `MATRIZ_RAIZ` e `FILIAL_RAIZ`: devem ser montados por `dossie_filiais_raiz.sql` quando disponivel; na ausencia, por reaproveitamento cadastral por `cnpj_raiz`.
- `CONTADOR_EMPRESA`: deve ser montado por composicao de `dossie_contador.sql`, `dossie_historico_fac.sql`, `dossie_rascunho_fac_contador.sql`, `dossie_req_inscricao_contador.sql` e sinais reconciliados de `NFe.sql`/`NFCe.sql`.
- `SOCIO_ATUAL`: deve ser montado por reuso de `dossie_historico_socios.sql`.
- `EMAIL_NFE`: deve ser montado por reuso de `NFe.sql` e `NFCe.sql`, sem abrir Oracle novamente quando os datasets compartilhados ja estiverem materializados.

## 13. Verificacao e Testes

- Teste de integridade: validar que a empresa consultada aparece como `EMPRESA_PRINCIPAL`.
- Teste de integridade: validar que todas as filiais da mesma raiz aparecem, inclusive sem contato preenchido.
- Teste de integridade: validar que o contador da empresa aparece como `CONTADOR_EMPRESA` quando houver `GR_IDENT_CONTADOR`.
- Teste de integridade: validar fallback para campos de contador oriundos de `SITAFE_RASCUNHO_FAC` ou `SITAFE_REQ_INSCRICAO` quando nao houver dados suficientes em `SITAFE_PESSOA`.
- Teste de integridade: validar que telefone de NFe ou NFCe so e anexado ao contador quando houver CPF/CNPJ reconciliado com emitente ou destinatario.
- Teste de integridade: validar que a matriz e as filiais nao se confundem no `tipo_vinculo`.
- Teste de integridade: validar que um socio que tambem e contribuinte aparece com seus dados proprios no bloco de socios.
- Teste de carga: simular empresa com muitas filiais e varios socios para medir tempo de resposta.
- Teste funcional: confirmar que emails de NFe permanecem restritos ao CNPJ consultado.
- Teste funcional: confirmar que a secao `contato` reaproveita datasets existentes e nao dispara extração Oracle redundante.

## 14. Resultado Esperado

Ao final, a secao `contato` do Dossie deve entregar uma visao unica e auditavel contendo:

- contato da empresa principal;
- enumeracao das filiais por CNPJ raiz;
- contatos do contador vinculado a empresa;
- telefones observados em NFe e NFCe para o contador quando houver reconciliacao valida;
- contatos dos socios atuais;
- emails operacionais recentes da empresa principal.
- composicao eficiente sobre datasets compartilhados, sem reextrair a mesma entidade desnecessariamente.

Isso melhora a leitura investigativa sem perder separacao de responsabilidade, rastreabilidade do dado e clareza da regra de negocio.
