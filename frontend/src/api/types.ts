export interface CNPJRecord {
  cnpj: string;
  added_at: string;
  last_run_at: string | null;
}

export interface ParquetFile {
  name: string;
  path: string;
  size: number;
}

export interface FilterItem {
  column: string;
  operator: string;
  value: string;
}

export interface PageResult {
  total_rows: number;
  page: number;
  page_size: number;
  total_pages: number;
  columns: string[];
  all_columns: string[];
  rows: Record<string, unknown>[];
}

export interface PipelineStatus {
  status: 'idle' | 'queued' | 'running' | 'done' | 'error';
  progresso: string[];
  erros: string[];
}

export interface SqlFile {
  name: string;
  path: string;
}

export const FILTER_OPERATORS = [
  'contem',
  'igual',
  'comeca_com',
  'termina_com',
  'maior',
  'maior_igual',
  'menor',
  'menor_igual',
  'e_nulo',
  'nao_e_nulo',
] as const;

export type FilterOperator = (typeof FILTER_OPERATORS)[number];
