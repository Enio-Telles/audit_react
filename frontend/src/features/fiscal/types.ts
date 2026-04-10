export interface FiscalDomainCard {
  id: string;
  title: string;
  value: string;
  description: string;
}

export interface FiscalDatasetSummary {
  id: string;
  label: string;
  stage: string;
  description: string;
}

export interface FiscalLegacyShortcut {
  id: string;
  label: string;
  description: string;
}

export interface FiscalDomainSummary {
  domain: string;
  title: string;
  subtitle: string;
  cnpj: string | null;
  status: string;
  pipeline: string;
  cards: FiscalDomainCard[];
  datasets: FiscalDatasetSummary[];
  next_steps: string[];
  legacy_shortcuts: FiscalLegacyShortcut[];
}
