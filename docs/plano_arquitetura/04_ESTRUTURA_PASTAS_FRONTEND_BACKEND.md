# Estrutura de Pastas Frontend e Backend

## Backend sugerido

```text
backend/
  routers/
    fiscal_efd.py
    fiscal_documentos.py
    fiscal_fiscalizacao.py
    fiscal_analise.py
  services/
    fiscal/
      efd/
      documentos/
      fiscalizacao/
      analise/
  schemas/
    fiscal/
```

## Frontend sugerido

```text
frontend/src/features/fiscal/
  shared/
  efd/
  documentos_fiscais/
  fiscalizacao/
  analise/
```

## Regra estrutural
- backend expõe leitura e materialização
- frontend consome contratos estáveis
- lógica fiscal reaproveitável não deve ficar escondida em uma tela única
