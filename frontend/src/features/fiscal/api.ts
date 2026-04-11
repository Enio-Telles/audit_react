import type { PageResult } from "../../api/types";
import api from "../../api/client";
import type {
  DatasetCatalogAvailability,
  DatasetCatalogSummary,
  DatasetInspection,
  FiscalDomainSummary,
  FiscalizacaoCadastroRecord,
  FiscalizacaoDsfRecord,
} from "./types";

function getResumo(path: string, cnpj?: string | null) {
  return api
    .get<FiscalDomainSummary>(path, { params: cnpj ? { cnpj } : {} })
    .then((response) => response.data);
}

interface PageQueryOptions {
  page?: number;
  pageSize?: number;
  sortBy?: string;
  sortDesc?: boolean;
  filterText?: string;
  filterColumn?: string;
  filterValue?: string;
}

export interface EfdRecordOption {
  record: string;
  title: string;
  description: string;
  upstream: string[];
  dataset_candidates: string[];
}

export interface EfdDictionaryField {
  field: string;
  label: string;
  description: string;
}

export interface EfdManifest {
  record: string;
  title: string;
  description: string;
  upstream: string[];
  dictionary_fields: number;
  datasets: Array<{ dataset_id: string; layer: string; path: string }>;
}

export interface EfdDatasetResponse {
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
}

export interface EfdCompareResponse {
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
}

export interface EfdDocumentTreeResponse {
  doc_key: string;
  documents: Array<{
    document: Record<string, unknown>;
    items_c170: Record<string, unknown>[];
    summary_c190: Record<string, unknown>[];
    links_c176: Record<string, unknown>[];
    adjustments_c197: Record<string, unknown>[];
  }>;
}

export interface EfdRowProvenanceResponse {
  record: string;
  dataset_id: string;
  layer: string;
  path: string;
  key_field: string;
  row_identifier: string;
  upstream: string[];
  row: Record<string, unknown> | null;
}

function getPage(path: string, cnpj: string, options: PageQueryOptions = {}) {
  const {
    page = 1,
    pageSize = 50,
    sortBy,
    sortDesc,
    filterText,
    filterColumn,
    filterValue,
  } = options;
  const hasColumnFilter = Boolean(filterColumn && filterValue);
  return api
    .get<PageResult>(path, {
      params: {
        cnpj,
        page,
        page_size: pageSize,
        ...(sortBy ? { sort_by: sortBy } : {}),
        ...(sortDesc ? { sort_desc: true } : {}),
        ...(filterText ? { filter_text: filterText } : {}),
        ...(hasColumnFilter ? { filter_column: filterColumn, filter_value: filterValue } : {}),
      },
    })
    .then((response) => response.data);
}

export const fiscalFeatureApi = {
  getEfdResumo: (cnpj?: string | null) => getResumo("/fiscal/efd/resumo", cnpj),
  getAgregacaoResumo: (cnpj?: string | null) =>
    getResumo("/fiscal/agregacao/resumo", cnpj),
  getProdutoResumo: (cnpj?: string | null) => getResumo("/fiscal/produto/resumo", cnpj),
  getConversaoResumo: (cnpj?: string | null) => getResumo("/fiscal/conversao/resumo", cnpj),
  getEstoqueResumo: (cnpj?: string | null) => getResumo("/fiscal/estoque/resumo", cnpj),
  getDocumentosResumo: (cnpj?: string | null) =>
    getResumo("/fiscal/documentos/resumo", cnpj),
  getFiscalizacaoResumo: (cnpj?: string | null) =>
    getResumo("/fiscal/fiscalizacao/resumo", cnpj),
  getAnaliseResumo: (cnpj?: string | null) =>
    getResumo("/fiscal/analise/resumo", cnpj),
  getRessarcimentoResumo: (cnpj?: string | null) =>
    getResumo("/fiscal/ressarcimento/resumo", cnpj),

  getDatasetCatalogSummary: () =>
    api.get<DatasetCatalogSummary>("/observabilidade/dataset-catalog").then((response) => response.data),
  getDatasetCatalogForCnpj: (cnpj: string) =>
    api.get<DatasetCatalogAvailability>(`/observabilidade/dataset-catalog/${cnpj}`).then((response) => response.data),
  inspectDatasetCatalog: (cnpj: string, datasetId: string, limit = 20) =>
    api.get<DatasetInspection>(`/observabilidade/dataset-catalog/${cnpj}/${datasetId}`, { params: { limit } }).then((response) => response.data),
  getEfdC170: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/efd/c170", cnpj, options),
  getEfdBlocoH: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/efd/bloco-h", cnpj, options),
  getEfdC176: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/efd/c176", cnpj, options),
  getEfdC197: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/efd/c197", cnpj, options),
  getEfdE111: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/efd/e111", cnpj, options),
  getEfdE110: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/efd/e110", cnpj, options),
  getEfdK200: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/efd/k200", cnpj, options),
  getEfdRecords: () =>
    api.get<EfdRecordOption[]>("/fiscal/efd/records").then((response) => response.data),
  getEfdDictionary: (record: string) =>
    api
      .get<{ record: string; fields: EfdDictionaryField[] }>(`/fiscal/efd/dictionary/${record}`)
      .then((response) => response.data),
  getEfdManifest: (record: string, cnpj?: string | null) =>
    api
      .get<EfdManifest>(`/fiscal/efd/manifest/${record}`, { params: cnpj ? { cnpj } : {} })
      .then((response) => response.data),
  getEfdDatasetRecord: (
    record: string,
    options: {
      cnpj?: string | null;
      periodo?: string;
      page?: number;
      pageSize?: number;
      preferLayer?: string;
      filters?: Record<string, string>;
    } = {},
  ) =>
    api
      .get<EfdDatasetResponse>(`/fiscal/efd/dataset/${record}`, {
        params: {
          ...(options.cnpj ? { cnpj: options.cnpj } : {}),
          ...(options.periodo ? { periodo: options.periodo } : {}),
          ...(options.page ? { page: options.page } : {}),
          ...(options.pageSize ? { page_size: options.pageSize } : {}),
          ...(options.preferLayer ? { prefer_layer: options.preferLayer } : {}),
          ...(options.filters && Object.keys(options.filters).length > 0
            ? {
                filters: Object.entries(options.filters)
                  .map(([key, value]) => `${key}=${value}`)
                  .join(";"),
              }
            : {}),
        },
      })
      .then((response) => response.data),
  compareEfdRecord: (
    record: string,
    options: {
      cnpj: string;
      periodoA: string;
      periodoB: string;
      keyField?: string;
    },
  ) =>
    api
      .get<EfdCompareResponse>(`/fiscal/efd/compare/${record}`, {
        params: {
          cnpj: options.cnpj,
          periodo_a: options.periodoA,
          periodo_b: options.periodoB,
          ...(options.keyField ? { key_field: options.keyField } : {}),
        },
      })
      .then((response) => response.data),
  getEfdDocumentTree: (
    cnpj: string,
    options: { periodo?: string; chaveDocumento?: string; limitDocs?: number } = {},
  ) =>
    api
      .get<EfdDocumentTreeResponse>("/fiscal/efd/tree/documents", {
        params: {
          cnpj,
          ...(options.periodo ? { periodo: options.periodo } : {}),
          ...(options.chaveDocumento ? { chave_documento: options.chaveDocumento } : {}),
          ...(options.limitDocs ? { limit_docs: options.limitDocs } : {}),
        },
      })
      .then((response) => response.data),
  getEfdRowProvenance: (
    record: string,
    options: {
      rowIdentifier: string;
      cnpj?: string | null;
      keyField?: string;
      preferLayer?: string;
    },
  ) =>
    api
      .get<EfdRowProvenanceResponse>(`/fiscal/efd/row-provenance/${record}`, {
        params: {
          row_identifier: options.rowIdentifier,
          ...(options.cnpj ? { cnpj: options.cnpj } : {}),
          ...(options.keyField ? { key_field: options.keyField } : {}),
          ...(options.preferLayer ? { prefer_layer: options.preferLayer } : {}),
        },
      })
      .then((response) => response.data),
  getProdutoAgrupacoes: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/produto/agrupacoes", cnpj, options),
  getProdutoBase: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/produto/produtos-base", cnpj, options),
  getAgregacaoGrupos: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/agregacao/grupos", cnpj, options),
  getAgregacaoProdutosBase: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/agregacao/produtos-base", cnpj, options),
  getConversaoFatores: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/conversao/fatores", cnpj, options),
  getEstoqueMov: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/estoque/mov", cnpj, options),
  getEstoqueMensal: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/estoque/mensal", cnpj, options),
  getEstoqueAnual: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/estoque/anual", cnpj, options),
  getEstoqueBlocoH: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/estoque/bloco-h", cnpj, options),
  getDocumentosNfe: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/documentos/nfe", cnpj, options),
  getDocumentosNfce: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/documentos/nfce", cnpj, options),
  getDocumentosCte: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/documentos/cte", cnpj, options),
  getDocumentosInfoComplementar: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/documentos/info-complementar", cnpj, options),
  getDocumentosContatos: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/documentos/contatos", cnpj, options),
  getFiscalizacaoCadastro: (cnpj: string) =>
    api
      .get<FiscalizacaoCadastroRecord>("/fiscal/fiscalizacao/cadastro", { params: { cnpj } })
      .then((response) => response.data),
  getFiscalizacaoMalhas: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/fiscalizacao/malhas", cnpj, options),
  getFiscalizacaoDsfs: (cnpj: string) =>
    api
      .get<FiscalizacaoDsfRecord[]>("/fiscal/fiscalizacao/dsfs", { params: { cnpj } })
      .then((response) => response.data),
  getAnaliseEstoqueMov: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/analise/estoque-mov", cnpj, options),
  getAnaliseEstoqueMensal: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/analise/estoque-mensal", cnpj, options),
  getAnaliseEstoqueAnual: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/analise/estoque-anual", cnpj, options),
  getAnaliseAgregacao: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/analise/agregacao", cnpj, options),
  getAnaliseConversao: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/analise/conversao", cnpj, options),
  getAnaliseProdutosBase: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/analise/produtos-base", cnpj, options),
  getRessarcimentoMensal: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/ressarcimento/mensal", cnpj, options),
  getRessarcimentoItens: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/ressarcimento/itens", cnpj, options),
  getRessarcimentoConciliacao: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/ressarcimento/conciliacao", cnpj, options),
};
