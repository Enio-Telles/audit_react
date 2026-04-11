import { fiscalFeatureApi } from "../api";
import { FiscalDatasetExplorer } from "../shared/FiscalDatasetExplorer";

const DATASET_OPTIONS = [
  {
    id: "mov",
    label: "Eventos de estoque",
    description:
      "Movimentação escriturada: entradas, saídas e devoluções por item. Base operacional para a camada canônica de eventos fiscais do estoque.",
  },
  {
    id: "mensal",
    label: "Saldo mensal",
    description:
      "Série mensal de saldo por produto. Calculada a partir de eventos e ajustes, com confronto contra inventário declarado.",
  },
  {
    id: "anual",
    label: "Saldo anual",
    description:
      "Série anual de saldo consolidada. Agregação do saldo mensal com corte por exercício fiscal.",
  },
  {
    id: "bloco-h",
    label: "Inventário (Bloco H)",
    description:
      "Snapshot fiscal de inventário declarado na EFD (H005/H010/H020). Usado para confronto com saldo calculado — não é movimentação.",
  },
];

export function EstoqueFiscalTab() {
  return (
    <FiscalDatasetExplorer
      tabId="fiscal-estoque"
      domainKey="estoque"
      title="Estoque"
      subtitle="Visão em camadas: eventos fiscais, inventário declarado, saldo mensal, saldo anual e divergências. Drill-down até documento, item e origem."
      detailTitle="Detalhe do registro de estoque"
      detailSubtitle="Selecione uma linha para inspecionar todos os campos, origem e cálculo do registro selecionado."
      emptyMessage="Selecione uma linha do domínio de estoque para ver o registro completo com origem e cálculo."
      datasetOptions={DATASET_OPTIONS}
      loadSummary={fiscalFeatureApi.getEstoqueResumo}
      loadPage={(datasetId, cnpj, options) => {
        switch (datasetId) {
          case "mov":
            return fiscalFeatureApi.getEstoqueMov(cnpj, options);
          case "mensal":
            return fiscalFeatureApi.getEstoqueMensal(cnpj, options);
          case "anual":
            return fiscalFeatureApi.getEstoqueAnual(cnpj, options);
          default:
            return fiscalFeatureApi.getEstoqueBlocoH(cnpj, options);
        }
      }}
    />
  );
}

export default EstoqueFiscalTab;
