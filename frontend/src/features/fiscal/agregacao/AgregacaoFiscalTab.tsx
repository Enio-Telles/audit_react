import { fiscalFeatureApi } from "../api";
import { FiscalDatasetExplorer } from "../shared/FiscalDatasetExplorer";

const DATASET_OPTIONS = [
  {
    id: "grupos",
    label: "Grupos de agregacao",
    description:
      "Tabela de identidade de produto com agrupamentos, conflitos e sugestoes de merge reaproveitada da camada atual.",
  },
  {
    id: "produtos-base",
    label: "Produtos base",
    description:
      "Catalogo base usado como evidencia estrutural para descricao, GTIN, NCM, CEST e unidade praticada.",
  },
];

export function AgregacaoFiscalTab() {
  return (
    <FiscalDatasetExplorer
      tabId="fiscal-agregacao"
      domainKey="agregacao"
      title="Agregacao"
      subtitle="Workbench de identidade de produto com grupos, conflitos, evidencias e merges operacionais."
      detailTitle="Detalhe da agregacao"
      detailSubtitle="Selecione uma linha para inspecionar o grupo, as evidencias e os campos do registro."
      emptyMessage="Selecione uma linha da tabela para ver o registro completo da agregacao."
      datasetOptions={DATASET_OPTIONS}
      loadSummary={fiscalFeatureApi.getAgregacaoResumo}
      loadPage={(datasetId, cnpj, options) =>
        datasetId === "grupos"
          ? fiscalFeatureApi.getAgregacaoGrupos(cnpj, options)
          : fiscalFeatureApi.getAgregacaoProdutosBase(cnpj, options)
      }
    />
  );
}

export default AgregacaoFiscalTab;
