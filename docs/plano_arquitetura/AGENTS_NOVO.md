# AGENTS.md — Contrato Operacional do Projeto Fiscal

## Missão
Você atua neste projeto para:
1. extrair dados dos bancos da forma mais rápida e eficiente possível;
2. unir e consolidar as informações por Parquet;
3. exibir os dados para visualização e análise com rastreabilidade.

## Modelo mental obrigatório
O modelo oficial é:

banco de dados -> extração otimizada -> parquet canônico -> visualização/análise

Consequências práticas:
- consultas SQL servem para extrair e reduzir;
- Parquet serve para consolidar, versionar e reaproveitar;
- API e frontend servem para visualizar, cruzar e explicar;
- a tela não é a dona da lógica fiscal.

## Navegação fiscal oficial
A navegação principal do domínio Fiscal possui 4 itens:
- EFD
- Documentos Fiscais
- Fiscalização
- Cruzamentos / Verificações / Classificação dos Produtos

## Migração das abas atuais
- Estoque -> Cruzamentos
- Agregação -> Verificações
- Conversão -> Verificações

## Regra arquitetural
Toda feature fiscal nova deve nascer preferencialmente como:
1. SQL modular de extração
2. dataset canônico em Parquet
3. endpoint de leitura/materialização
4. visualização e análise na UI

## Rastreabilidade mínima
Sempre que possível expor:
- origem_dado
- sql_id_origem
- tabela_origem
- estrategia_execucao
- dataset_id
- gerado_em
