import { fiscalFeatureApi } from "../api";
import { FiscalDatasetExplorer } from "../shared/FiscalDatasetExplorer";

const DATASET_OPTIONS = [
  {
    id: "fatores",
    label: "Fatores de conversão",
    description:
      "Base operacional de fatores de conversão. Inclui fator declarado na EFD, inferido por cruzamento, auxiliar por referência e manual do auditor. Ponte para a futura separação por tipo e impacto.",
  },
];

export function ConversaoFiscalTab() {
  return (
    <FiscalDatasetExplorer
      tabId="fiscal-conversao"
      domainKey="conversao"
      title="Conversão"
      subtitle="Fatores de conversão por tipo (EFD, inferido, auxiliar, manual), origem do fator final, evidências de unidade e histórico de edição."
      detailTitle="Detalhe do fator de conversão"
      detailSubtitle="Selecione uma linha para inspecionar origem, tipo, impacto e histórico do fator de conversão."
      emptyMessage="Selecione um fator de conversão na tabela para ver o registro completo com origem e impacto."
      datasetOptions={DATASET_OPTIONS}
      loadSummary={fiscalFeatureApi.getConversaoResumo}
      loadPage={(_, cnpj, options) => fiscalFeatureApi.getConversaoFatores(cnpj, options)}
    />
  );
}

export default ConversaoFiscalTab;
