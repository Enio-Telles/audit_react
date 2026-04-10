# Plano de Arquitetura Fiscal

Este diretório consolida a proposta e o estado atual da reestruturação do módulo fiscal do projeto.

## Princípio operacional

1. extrair dados dos bancos com eficiência;
2. consolidar e reaproveitar em datasets canônicos e Parquet;
3. exibir para visualização, auditoria e análise.

## Domínios fiscais oficiais

- EFD
- Documentos Fiscais
- Fiscalização
- Cruzamentos / Verificações / Classificação dos Produtos

## Situação atual de implementação

A arquitetura proposta já saiu do plano e entrou em operação inicial dentro do repositório:

- **EFD**: resumo real + tabela operacional para `C170` e `Bloco H` + filtro textual + filtro por coluna + ordenação + detalhe de registro;
- **Documentos Fiscais**: resumo real + tabelas operacionais para `NF-e`, `NFC-e`, `CT-e`, `informações complementares` e `contatos` + filtro textual + filtro por coluna + ordenação + detalhe de registro;
- **Fiscalização**: resumo real + painel de cadastro + tabela de malhas + lista de DSFs relacionadas + filtro textual + filtro por coluna + ordenação nas malhas + detalhe da malha selecionada;
- **Análise Fiscal**: resumo real + tabelas operacionais para estoque, agregação, conversão e produtos-base + filtro textual + filtro por coluna + ordenação + detalhe de registro.

## Migração das abas atuais

- Estoque -> Cruzamentos
- Agregação -> Verificações
- Conversão -> Verificações

## Documento consolidado

Use `00_CONSOLIDADO_MODULO_FISCAL.md` como visão concatenada do estado atual, da arquitetura alvo, dos contratos e da migração.

## Arquivos deste diretório

- `00_CONSOLIDADO_MODULO_FISCAL.md`
- `01_MAPA_ESTRUTURA_FISCAL.md`
- `02_CATALOGO_DATASETS_PARQUET.md`
- `03_CONTRATO_ENDPOINTS_FISCAL.md`
- `04_ESTRUTURA_PASTAS_FRONTEND_BACKEND.md`
- `05_MIGRACAO_ABAS_ATUAIS.md`
- `06_PLANO_COMPLETO_IMPLEMENTACAO.md`
- `07_BACKLOG_DETALHADO_POR_ETAPA.md`
- `08_CRITERIOS_ACEITE_TESTES_RISCOS.md`
- `09_SEQUENCIA_EXECUCAO.md`
- `AGENTS_NOVO.md`
- `AGENTS_SQL_NOVO.md`
