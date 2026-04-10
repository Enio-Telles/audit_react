import { useQuery } from "@tanstack/react-query";

import { useAppStore } from "../../../store/appStore";
import { fiscalFeatureApi } from "../api";
import { FiscalPageShell } from "../shared/FiscalPageShell";
import { FiscalDomainOverview } from "../shared/FiscalDomainOverview";

export function FiscalizacaoTab() {
  const selectedCnpj = useAppStore((state) => state.selectedCnpj);

  const query = useQuery({
    queryKey: ["fiscal", "fiscalizacao", "resumo", selectedCnpj ?? "sem-cnpj"],
    queryFn: () => fiscalFeatureApi.getFiscalizacaoResumo(selectedCnpj),
  });

  return (
    <FiscalPageShell
      title="Fiscalização"
      subtitle="Fronteira, Fisconforme, malhas, chaves e resoluções."
    >
      <FiscalDomainOverview
        data={query.data}
        isLoading={query.isLoading}
        errorMessage={query.error instanceof Error ? query.error.message : undefined}
      />
    </FiscalPageShell>
  );
}

export default FiscalizacaoTab;
