import type { PageResult } from "../../api/types";
import api from "../../api/client";
import type {
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
}

function getPage(path: string, cnpj: string, options: PageQueryOptions = {}) {
  const { page = 1, pageSize = 50, sortBy, sortDesc, filterText } = options;
  return api
    .get<PageResult>(path, {
      params: {
        cnpj,
        page,
        page_size: pageSize,
        ...(sortBy ? { sort_by: sortBy } : {}),
        ...(sortDesc ? { sort_desc: true } : {}),
        ...(filterText ? { filter_text: filterText } : {}),
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
  getFiscalizacaoMalhas: (cnpj: string, page = 1, pageSize = 50) =>
    getPage("/fiscal/fiscalizacao/malhas", cnpj, { page, pageSize }),
  getFiscalizacaoDsfs: (cnpj: string) =>
    api
      .get<FiscalizacaoDsfRecord[]>("/fiscal/fiscalizacao/dsfs", { params: { cnpj } })
      .then((response) => response.data),
  getAnaliseEstoqueMov: (cnpj: string, page = 1, pageSize = 50) =>
    getPage("/fiscal/analise/estoque-mov", cnpj, { page, pageSize }),
  getAnaliseEstoqueMensal: (cnpj: string, page = 1, pageSize = 50) =>
    getPage("/fiscal/analise/estoque-mensal", cnpj, { page, pageSize }),
  getAnaliseEstoqueAnual: (cnpj: string, page = 1, pageSize = 50) =>
    getPage("/fiscal/analise/estoque-anual", cnpj, { page, pageSize }),
  getAnaliseAgregacao: (cnpj: string, page = 1, pageSize = 50) =>
    getPage("/fiscal/analise/agregacao", cnpj, { page, pageSize }),
  getAnaliseConversao: (cnpj: string, page = 1, pageSize = 50) =>
    getPage("/fiscal/analise/conversao", cnpj, { page, pageSize }),
  getAnaliseProdutosBase: (cnpj: string, page = 1, pageSize = 50) =>
    getPage("/fiscal/analise/produtos-base", cnpj, { page, pageSize }),
};
