# Pacote de implementação — próxima etapa EFD

Este pacote reúne a próxima passada de implementação do módulo **EFD**, alinhada ao plano arquitetural de separar extração, base tipada, marts e frontend por domínio.

## Conteúdo

### Backend
- `backend/services/efd_service.py`
  - catálogo dos registros EFD;
  - leitura de datasets `raw/base/marts`;
  - consulta paginada;
  - manifest por registro;
  - comparação entre períodos;
  - árvore documental `C100 -> C170/C190/C176/C197`;
  - proveniência por linha.

- `backend/routers/fiscal_efd.py`
  - endpoints REST para records, dataset, manifest, comparison, tree e row provenance.

### Processamento
- `src/processing/efd/base_utils.py`
  - utilitários comuns de schema, aliases, tipagem e escrita.

- `src/processing/efd/base_builders.py`
  - builders para:
    - `base__efd__arquivos_validos`
    - `base__efd__reg_c100_tipado`
    - `base__efd__reg_c170_tipado`
    - `base__efd__reg_c176_tipado`
    - `base__efd__bloco_h_tipado`

### Frontend
- `frontend/src/features/fiscal/efd/api.ts`
- `frontend/src/features/fiscal/efd/EfdPage.tsx`
- `frontend/src/features/fiscal/efd/components/*`

## Observações

1. Este pacote foi preparado como **patch pronto para encaixe**.  
2. Os caminhos foram escritos para combinar com a arquitetura alvo do projeto, não com um checkout reduzido.  
3. As funções usam `polars` e presumem que os datasets estejam em Parquet, conforme o plano.

## Próximo passo sugerido depois deste patch

- integrar os novos builders ao pipeline real;
- materializar os datasets base em `dados/CNPJ/<cnpj>/base/efd/`;
- ligar os endpoints de lineage ao catálogo/manifests já existentes;
- substituir a aba EFD de transição pela nova página canônica.
