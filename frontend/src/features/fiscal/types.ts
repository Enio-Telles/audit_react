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

export interface FiscalizacaoCadastroRecord {
  [key: string]: unknown;
}

export interface FiscalizacaoDsfRecord {
  id: string;
  dsf: string;
  referencia: string;
  auditor: string;
  cargo_titulo: string;
  orgao_origem: string;
  updated_at: string;
  created_at: string;
  pdf_file_name: string;
  cnpjs_count: number;
}

export interface DatasetCatalogSummary {
  total_datasets: number;
  total_aliases: number;
  materialized_datasets: string[];
  aliases: Record<string, string>;
}

export interface DatasetAvailabilityItem {
  dataset_id: string;
  aliases: string[];
  tipo: string;
  sql_id: string | null;
  disponivel: boolean;
  caminho: string | null;
  formato: string | null;
  reutilizado: boolean;
}

export interface DatasetCatalogAvailability {
  cnpj: string | null;
  items: DatasetAvailabilityItem[];
}

export interface DatasetInspection {
  cnpj: string | null;
  dataset_id: string;
  aliases: string[];
  caminho?: string;
  reutilizado?: boolean;
  metadata?: Record<string, unknown> | null;
  probe: Record<string, unknown>;
  columns?: string[];
  preview: Array<Record<string, unknown>>;
}
