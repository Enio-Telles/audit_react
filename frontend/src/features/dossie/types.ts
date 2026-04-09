export type DossieSectionStatus = 'idle' | 'cached' | 'loading' | 'fresh' | 'error';

export interface DossieSectionSummary {
  id: string;
  title: string;
  description: string;
  sourceType: 'sql_catalog' | 'xml_fallback' | 'mixed' | 'composed' | 'cache_catalog';
  syncEnabled: boolean;
  sourceFiles?: string[] | null;
  status: DossieSectionStatus;
  rowCount?: number;
  executionStrategy?: string | null;
  primarySql?: string | null;
  alternateStrategyComparison?: string | null;
  alternateStrategyMissingKeys?: number | null;
  alternateStrategyExtraKeys?: number | null;
  updatedAt?: string | null;
}

export interface DossieTabProps {
  cnpj: string | null;
  razaoSocial?: string | null;
  sections?: DossieSectionSummary[];
}

export interface DossieSyncResponse {
  status: string;
  cnpj: string;
  secao_id: string;
  linhas_extraidas: number;
  cache_file: string;
  metadata_file?: string;
  cache_key?: string;
  versao_consulta?: string;
  impacto_cache_first?: string;
  estrategia_execucao?: 'sql_direto' | 'composicao_polars' | 'sql_consolidado';
  sql_principal?: string | null;
  sql_ids?: string[];
  sql_ids_executados?: string[];
  sql_ids_reutilizados?: string[];
  total_sql_ids?: number;
  percentual_reuso_sql?: number;
  tempo_materializacao_ms?: number;
  tempo_total_sync_ms?: number;
  comparacao_estrategia_alternativa?: Record<string, unknown> | null;
  comparison_history_file?: string | null;
  updatedAt?: number | null;
}

export interface DossieSectionData {
  id: string;
  title: string;
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
  cacheFile: string;
  metadata?: Record<string, unknown> | null;
  updatedAt?: string | null;
}

export interface DossieComparisonHistory {
  cnpj: string;
  secaoId: string;
  items: Record<string, unknown>[];
  historyFile: string;
}

export interface DossieComparisonSummary {
  cnpj: string;
  secaoId: string;
  totalComparacoes: number;
  convergenciasFuncionais: number;
  divergenciasFuncionais: number;
  convergenciasBasicas: number;
  divergenciasBasicas: number;
  ultimaEstrategia?: string | null;
  ultimaSqlPrincipal?: string | null;
  ultimoStatusComparacao?: string | null;
  ultimoCacheKey?: string | null;
  updatedAt?: string | null;
  historyFile: string;
}

export interface DossieComparisonReport {
  cnpj: string;
  secaoId: string;
  reportFile: string;
  updatedAt?: string | null;
  content: string;
}
