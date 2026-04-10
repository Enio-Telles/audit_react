# Plano de Arquitetura Fiscal

Este diretório consolida a proposta oficial de reestruturação do módulo fiscal do projeto.

## Princípio operacional

1. extrair dados dos bancos com eficiência;
2. consolidar e reaproveitar em datasets canônicos e Parquet;
3. exibir para visualização, auditoria e análise.

## Domínios fiscais oficiais

- EFD
- Documentos Fiscais
- Fiscalização
- Cruzamentos / Verificações / Classificação dos Produtos

## Migração das abas atuais

- Estoque -> Cruzamentos
- Agregação -> Verificações
- Conversão -> Verificações

## Arquivos deste diretório

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

> Estes documentos passam a ser a referência arquitetural proposta para a evolução do módulo fiscal. Os arquivos raiz legados podem ser migrados depois sem perda de rastreabilidade.
