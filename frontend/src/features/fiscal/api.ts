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
  getProdutoAgrupacoes: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/produto/agrupacoes", cnpj, options),
  getProdutoBase: (cnpj: string, options: PageQueryOptions = {}) =>
    getPage("/fiscal/produto/produtos-base", cnpj, options),
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
