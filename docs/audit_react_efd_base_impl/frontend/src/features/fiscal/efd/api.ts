export type EfdManifest = {
  record: string;
  title: string;
  description: string;
  upstream: string[];
  dictionary_fields: number;
  datasets: Array<{ dataset_id: string; layer: string; path: string }>;
};

export type EfdDatasetResponse = {
  record: string;
  dataset_id: string;
  layer: string;
  path: string;
  page: number;
  page_size: number;
  total: number;
  columns: string[];
  records: Record<string, unknown>[];
  provenance: {
    upstream: string[];
    periodo?: string | null;
    cnpj?: string | null;
  };
};

export type EfdCompareResponse = {
  record: string;
  dataset_id: string;
  periodo_a: string;
  periodo_b: string;
  key_field: string;
  summary: {
    count_a: number;
    count_b: number;
    added: number;
    removed: number;
    intersection: number;
  };
  sample: {
    added_keys: string[];
    removed_keys: string[];
    intersection_keys: string[];
  };
};

const apiBase = "/api/fiscal/efd";

async function getJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Erro HTTP ${response.status}`);
  }
  return response.json() as Promise<T>;
}

export async function fetchEfdRecords() {
  return getJson<Array<{ record: string; title: string; description: string }>>(`${apiBase}/records`);
}

export async function fetchEfdManifest(record: string, cnpj?: string) {
  const params = new URLSearchParams();
  if (cnpj) params.set("cnpj", cnpj);
  return getJson<EfdManifest>(`${apiBase}/manifest/${record}?${params.toString()}`);
}

export async function fetchEfdDictionary(record: string) {
  return getJson<{ record: string; fields: Array<{ field: string; label: string; description: string }> }>(
    `${apiBase}/dictionary/${record}`,
  );
}

export async function fetchEfdDataset(record: string, options: {
  cnpj?: string;
  periodo?: string;
  page?: number;
  pageSize?: number;
  filters?: Record<string, string>;
  preferLayer?: string;
}) {
  const params = new URLSearchParams();
  if (options.cnpj) params.set("cnpj", options.cnpj);
  if (options.periodo) params.set("periodo", options.periodo);
  if (options.page) params.set("page", String(options.page));
  if (options.pageSize) params.set("page_size", String(options.pageSize));
  if (options.preferLayer) params.set("prefer_layer", options.preferLayer);
  if (options.filters && Object.keys(options.filters).length > 0) {
    params.set(
      "filters",
      Object.entries(options.filters)
        .map(([k, v]) => `${k}=${v}`)
        .join(";"),
    );
  }
  return getJson<EfdDatasetResponse>(`${apiBase}/dataset/${record}?${params.toString()}`);
}

export async function fetchEfdCompare(record: string, options: {
  cnpj: string;
  periodoA: string;
  periodoB: string;
  keyField?: string;
}) {
  const params = new URLSearchParams();
  params.set("cnpj", options.cnpj);
  params.set("periodo_a", options.periodoA);
  params.set("periodo_b", options.periodoB);
  if (options.keyField) params.set("key_field", options.keyField);
  return getJson<EfdCompareResponse>(`${apiBase}/compare/${record}?${params.toString()}`);
}

export async function fetchEfdTree(options: {
  cnpj: string;
  periodo?: string;
  chaveDocumento?: string;
}) {
  const params = new URLSearchParams();
  params.set("cnpj", options.cnpj);
  if (options.periodo) params.set("periodo", options.periodo);
  if (options.chaveDocumento) params.set("chave_documento", options.chaveDocumento);
  return getJson<{ doc_key: string; documents: Array<Record<string, unknown>> }>(
    `${apiBase}/tree/documents?${params.toString()}`,
  );
}

export async function fetchEfdRowProvenance(record: string, options: {
  rowIdentifier: string;
  cnpj?: string;
  keyField?: string;
  preferLayer?: string;
}) {
  const params = new URLSearchParams();
  params.set("row_identifier", options.rowIdentifier);
  if (options.cnpj) params.set("cnpj", options.cnpj);
  if (options.keyField) params.set("key_field", options.keyField);
  if (options.preferLayer) params.set("prefer_layer", options.preferLayer);
  return getJson(`${apiBase}/row-provenance/${record}?${params.toString()}`);
}
