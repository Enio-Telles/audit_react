export type DossieSectionStatus = 'idle' | 'cached' | 'loading' | 'fresh' | 'error';

export interface DossieSectionSummary {
  id: string;
  title: string;
  description: string;
  sourceType: 'sql_catalog' | 'xml_fallback' | 'mixed';
  status: DossieSectionStatus;
  rowCount?: number;
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
  updatedAt?: number | null;
}
