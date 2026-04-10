import { useQuery } from "@tanstack/react-query";

import { useAppStore } from "../../../store/appStore";
import { fiscalFeatureApi } from "../api";
import { FiscalPageShell } from "../shared/FiscalPageShell";
import { FiscalDomainOverview } from "../shared/FiscalDomainOverview";

export function EfdTab() {
  const selectedCnpj = useAppStore((state) => state.selectedCnpj);

  const query = useQuery({
    queryKey: ["fiscal", "efd", "resumo", selectedCnpj ?? "sem-cnpj"],
    queryFn: () => fiscalFeatureApi.getEfdResumo(selectedCnpj),
  });

  return (
    <FiscalPageShell
      title="EFD"
      subtitle="Resumo, blocos, registros, árvore e dicionário."
    >
      <FiscalDomainOverview
        data={query.data}
        isLoading={query.isLoading}
        errorMessage={query.error instanceof Error ? query.error.message : undefined}
      />
    </FiscalPageShell>
  );
}

export default EfdTab;
