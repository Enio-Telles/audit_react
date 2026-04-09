# Mapeamento SPED Fiscal (EFD ICMS/IPI)

Esta documentação fornece uma referência detalhada dos registros **Core** (principais) do SPED Fiscal, conforme definido no [Guia Prático EFD - Versão 3.2.1](http://sped.rfb.gov.br).

Os campos descritos aqui refletem os metadados extraídos dos artefatos Parquet do sistema, servindo como guia para auditorias, extrações e validações fiscais.

## Estrutura da Documentação

A documentação está dividida por blocos para facilitar a consulta:

- **[Bloco 0 - Abertura e Identificação](file:///c:/Sistema_react/docs/mapeamento_sped/bloco_0.md)**
  - Identificação do contribuinte, participantes, unidades de medida e cadastro de itens (produtos).
- **[Bloco C - Documentos Fiscais I (Mercadorias)](file:///c:/Sistema_react/docs/mapeamento_sped/bloco_c.md)**
  - Notas fiscais (NF-e, NFC-e), itens de documentos e registros analíticos.
- **[Bloco D - Documentos Fiscais II (Serviços)](file:///c:/Sistema_react/docs/mapeamento_sped/bloco_d.md)**
  - Conhecimentos de transporte e serviços de comunicação.
- **[Bloco E - Apuração do ICMS e IPI](file:///c:/Sistema_react/docs/mapeamento_sped/bloco_e.md)**
  - Registros de apuração mensal e de substituição tributária.
- **[Bloco H - Inventário Físico](file:///c:/Sistema_react/docs/mapeamento_sped/bloco_h.md)**
  - Detalhamento do estoque e avaliação de mercadorias.
- **[Bloco K - Controle da Produção e do Estoque](file:///c:/Sistema_react/docs/mapeamento_sped/bloco_k.md)**
  - Períodos de apuração, movimentação física e produção.

## Glossário de Tipos de Dados

| Sigla | Significado |
| :--- | :--- |
| **C** | Caractere (Texto) |
| **N** | Numérico |
| **D** | Data (no formato DDMMAAAA) |

---
> [!NOTE]
> Esta documentação é gerada automaticamente a partir dos metadados de referência do sistema.
