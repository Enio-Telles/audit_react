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
  getDocumentosResumo: (cnpj?: string | null) =>
    getResumo("/fiscal/documentos/resumo", cnpj),
  getFiscalizacaoResumo: (cnpj?: string | null) =>
    getResumo("/fiscal/fiscalizacao/resumo", cnpj),
  getAnaliseResumo: (cnpj?: string | null) =>
    getResumo("/fiscal/analise/resumo", cnpj),
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
};
