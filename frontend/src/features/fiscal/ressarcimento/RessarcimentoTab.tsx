import { fiscalFeatureApi } from "../api";
import { FiscalDatasetExplorer } from "../shared/FiscalDatasetExplorer";

const DATASET_OPTIONS = [
  {
    id: "mensal",
    label: "Resumo Mensal",
    description:
      "Consolidação mensal dos valores de ressarcimento e complemento de ICMS-ST.",
  },
  {
    id: "itens",
    label: "Detalhe por Item",
    description:
      "Detalhamento granular dos itens que compõem o ressarcimento no período.",
  },
  {
    id: "conciliacao",
    label: "Conciliação Fiscal",
    description:
      "Batimento entre valores apurados e valores declarados nas obrigações acessórias.",
  },
];

export function RessarcimentoTab() {
  return (
    <FiscalDatasetExplorer
      domainKey="ressarcimento"
      title="Ressarcimento ST — Gestão de Créditos"
      subtitle="Módulo analítico para gestão de ressarcimento e complemento de ICMS-ST (CAT 42, CAT 158 e EFD)."
      detailTitle="Detalhe do registro de ressarcimento"
      detailSubtitle="Exibição completa dos campos do dataset de ressarcimento selecionado."
      emptyMessage="Selecione um registro na tabela para visualizar o detalhamento técnico e a memória de cálculo."
      datasetOptions={DATASET_OPTIONS}
      loadSummary={fiscalFeatureApi.getRessarcimentoResumo}
      loadPage={(datasetId, cnpj, options) => {
        switch (datasetId) {
          case "mensal":
            return fiscalFeatureApi.getRessarcimentoMensal(cnpj, options);
          case "itens":
            return fiscalFeatureApi.getRessarcimentoItens(cnpj, options);
          case "conciliacao":
            return fiscalFeatureApi.getRessarcimentoConciliacao(cnpj, options);
          default:
            return fiscalFeatureApi.getRessarcimentoMensal(cnpj, options);
        }
      }}
    />
  );
}

export default RessarcimentoTab;
