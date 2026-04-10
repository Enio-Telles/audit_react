import { useQuery } from "@tanstack/react-query";

import { useAppStore } from "../../../store/appStore";
import { fiscalFeatureApi } from "../api";
import { FiscalPageShell } from "../shared/FiscalPageShell";
import { FiscalDomainOverview } from "../shared/FiscalDomainOverview";

export function DocumentosFiscaisTab() {
  const selectedCnpj = useAppStore((state) => state.selectedCnpj);

  const query = useQuery({
    queryKey: [
      "fiscal",
      "documentos-fiscais",
      "resumo",
      selectedCnpj ?? "sem-cnpj",
    ],
    queryFn: () => fiscalFeatureApi.getDocumentosResumo(selectedCnpj),
  });

  return (
    <FiscalPageShell
      title="Documentos Fiscais"
      subtitle="NF-e, NFC-e, CT-e, informações complementares e contatos."
    >
      <FiscalDomainOverview
        data={query.data}
        isLoading={query.isLoading}
        errorMessage={query.error instanceof Error ? query.error.message : undefined}
      />
    </FiscalPageShell>
  );
}

export default DocumentosFiscaisTab;
