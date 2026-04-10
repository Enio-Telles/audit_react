# Contrato de Endpoints do Módulo Fiscal

## EFD
- GET /api/fiscal/efd/{cnpj}/resumo
- GET /api/fiscal/efd/{cnpj}/blocos
- GET /api/fiscal/efd/{cnpj}/registros/{registro}
- GET /api/fiscal/efd/{cnpj}/arvore
- POST /api/fiscal/efd/{cnpj}/materializar

## Documentos Fiscais
- GET /api/fiscal/documentos/{cnpj}/resumo
- GET /api/fiscal/documentos/{cnpj}/nfe
- GET /api/fiscal/documentos/{cnpj}/nfce
- GET /api/fiscal/documentos/{cnpj}/cte
- GET /api/fiscal/documentos/{cnpj}/info-complementar
- GET /api/fiscal/documentos/{cnpj}/contatos
- POST /api/fiscal/documentos/{cnpj}/materializar

## Fiscalização
- GET /api/fiscal/fiscalizacao/{cnpj}/resumo
- GET /api/fiscal/fiscalizacao/{cnpj}/fronteira
- GET /api/fiscal/fiscalizacao/{cnpj}/fisconforme
- GET /api/fiscal/fiscalizacao/{cnpj}/malhas
- GET /api/fiscal/fiscalizacao/{cnpj}/chaves
- GET /api/fiscal/fiscalizacao/{cnpj}/resolucoes
- POST /api/fiscal/fiscalizacao/{cnpj}/materializar

## Análise
- GET /api/fiscal/analise/{cnpj}/cruzamentos
- GET /api/fiscal/analise/{cnpj}/verificacoes
- GET /api/fiscal/analise/{cnpj}/classificacao-produtos
- POST /api/fiscal/analise/{cnpj}/materializar

## Dataset genérico
- GET /api/fiscal/datasets/{dataset_id}/schema
- GET /api/fiscal/datasets/{dataset_id}/rows
- GET /api/fiscal/datasets/{dataset_id}/metadata
