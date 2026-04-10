import { useQuery } from "@tanstack/react-query";

import { useAppStore } from "../../../store/appStore";
import { fiscalFeatureApi } from "../api";
import { FiscalPageShell } from "../shared/FiscalPageShell";
import { FiscalDomainOverview } from "../shared/FiscalDomainOverview";

export function AnaliseFiscalTab() {
  const selectedCnpj = useAppStore((state) => state.selectedCnpj);
  const setActiveTab = useAppStore((state) => state.setActiveTab);

  const query = useQuery({
    queryKey: ["fiscal", "analise", "resumo", selectedCnpj ?? "sem-cnpj"],
    queryFn: () => fiscalFeatureApi.getAnaliseResumo(selectedCnpj),
  });

  return (
    <FiscalPageShell
      title="Análise Fiscal"
      subtitle="Cruzamentos, verificações e classificação dos produtos."
    >
      <FiscalDomainOverview
        data={query.data}
        isLoading={query.isLoading}
        errorMessage={query.error instanceof Error ? query.error.message : undefined}
        onOpenShortcut={setActiveTab}
      />
    </FiscalPageShell>
  );
}

export default AnaliseFiscalTab;
