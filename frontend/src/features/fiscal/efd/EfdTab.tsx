import { fiscalFeatureApi } from "../api";
import { FiscalDatasetExplorer } from "../shared/FiscalDatasetExplorer";

const DATASET_OPTIONS = [
  {
    id: "c170",
    label: "C170 — Itens Documentais",
    description:
      "Itens escriturados da EFD. Base operacional mais forte: alimenta estoque, cruzamentos e cálculos fiscais.",
  },
  {
    id: "c176",
    label: "C176 — Ressarcimento ST",
    description:
      "Registros de ressarcimento e complemento de ICMS-ST declarados na EFD. Vínculo fiscal entre saída e entrada.",
  },
  {
    id: "c197",
    label: "C197 — Ajustes Documento",
    description:
      "Outras obrigações, ajustes e informações de valores provenientes do documento fiscal por item.",
  },
  {
    id: "e111",
    label: "E111 — Ajustes de Apuração",
    description:
      "Ajustes, informações e dedução de valores da apuração do ICMS.",
  },
  {
    id: "e110",
    label: "E110 — Apuração do ICMS",
    description:
      "Valores referentes à apuração do ICMS.",
  },
  {
    id: "bloco-h",
    label: "Bloco H — Inventário",
    description:
      "Inventário declarado na EFD (H005/H010/H020). Snapshot fiscal usado para confronto com saldo de estoque.",
  },
  {
    id: "k200",
    label: "K200 — Estoque Escriturado",
    description:
      "Escrituração do estoque de propriedade do estabelecimento, do Bloco K (Controle de Produção e Estoque).",
  },
];

export function EfdTab() {
  return (
    <FiscalDatasetExplorer
      domainKey="efd"
      title="EFD — Escrituração Fiscal Digital"
      subtitle="Registros da EFD com cobertura de C170, C176, C197, E111, E110 e Bloco H. Navegação por registro, filtragem, ordenação e detalhe por linha."
      detailTitle="Detalhe do registro EFD"
      detailSubtitle="Clique em uma linha para inspecionar todos os campos disponíveis do registro selecionado."
      emptyMessage="Selecione uma linha da tabela EFD para inspecionar o registro completo."
      datasetOptions={DATASET_OPTIONS}
      loadSummary={fiscalFeatureApi.getEfdResumo}
      loadPage={(datasetId, cnpj, options) => {
        switch (datasetId) {
          case "c170":
            return fiscalFeatureApi.getEfdC170(cnpj, options);
          case "c176":
            return fiscalFeatureApi.getEfdC176(cnpj, options);
          case "c197":
            return fiscalFeatureApi.getEfdC197(cnpj, options);
          case "e111":
            return fiscalFeatureApi.getEfdE111(cnpj, options);
          case "e110":
            return fiscalFeatureApi.getEfdE110(cnpj, options);
          case "k200":
            return fiscalFeatureApi.getEfdK200(cnpj, options);
          default:
            return fiscalFeatureApi.getEfdBlocoH(cnpj, options);
        }
      }}
    />
  );
}

export default EfdTab;
