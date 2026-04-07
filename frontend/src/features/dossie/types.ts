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
