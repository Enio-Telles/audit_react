import { fiscalFeatureApi } from "../api";
import { FiscalDatasetExplorer } from "../shared/FiscalDatasetExplorer";

const DATASET_OPTIONS = [
  {
    id: "agrupacoes",
    label: "Grupos de produto",
    description:
      "Workbench de agrupamento: candidatos de identidade, score de confiança e justificativa de merge. Base para o futuro Produto Master com evidências e conflitos.",
  },
  {
    id: "produtos-base",
    label: "Catálogo de produtos",
    description:
      "Catálogo mestre de produtos com GTIN, NCM, CEST, descrições normalizadas e unidade praticada. Ponte para a camada canônica de identidade com conflitos e evidências.",
  },
];

const COLUMN_PRESETS = [
  {
    label: "Identidade",
    columns: ["COD_ITEM", "DESCR_COMPLEMENTAR", "COD_NCM", "TIPO_ITEM", "UNID_INV"],
  },
  {
    label: "Classificação",
    columns: ["COD_NCM", "CEST", "COD_GEN", "ALIQ_ICMS", "TIPO_ITEM"],
  },
];

export function ProdutoMasterTab() {
  return (
    <FiscalDatasetExplorer
      tabId="produto-master"
      domainKey="produto"
      title="Produto Master"
      subtitle="Identidade, agrupamento, conflitos e classificação fiscal dos produtos. Workbench de merge com evidências por GTIN, NCM, CEST, descrição e unidade."
      detailTitle="Detalhe do produto"
      detailSubtitle="Selecione uma linha para inspecionar identidade, classificação e evidências do produto selecionado."
      emptyMessage="Selecione um produto da tabela para ver o registro completo com todas as evidências."
      datasetOptions={DATASET_OPTIONS}
      loadSummary={fiscalFeatureApi.getProdutoResumo}
      loadPage={(datasetId, cnpj, options) =>
        datasetId === "agrupacoes"
          ? fiscalFeatureApi.getProdutoAgrupacoes(cnpj, options)
          : fiscalFeatureApi.getProdutoBase(cnpj, options)
      }
    />
  );
}

export default ProdutoMasterTab;
