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

function getPage(path: string, cnpj: string, page: number, pageSize: number) {
  return api
    .get<PageResult>(path, { params: { cnpj, page, page_size: pageSize } })
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
  getEfdC170: (cnpj: string, page = 1, pageSize = 50) =>
    getPage("/fiscal/efd/c170", cnpj, page, pageSize),
  getEfdBlocoH: (cnpj: string, page = 1, pageSize = 50) =>
    getPage("/fiscal/efd/bloco-h", cnpj, page, pageSize),
  getDocumentosNfe: (cnpj: string, page = 1, pageSize = 50) =>
    getPage("/fiscal/documentos/nfe", cnpj, page, pageSize),
  getDocumentosNfce: (cnpj: string, page = 1, pageSize = 50) =>
    getPage("/fiscal/documentos/nfce", cnpj, page, pageSize),
  getDocumentosCte: (cnpj: string, page = 1, pageSize = 50) =>
    getPage("/fiscal/documentos/cte", cnpj, page, pageSize),
  getDocumentosInfoComplementar: (cnpj: string, page = 1, pageSize = 50) =>
    getPage("/fiscal/documentos/info-complementar", cnpj, page, pageSize),
  getDocumentosContatos: (cnpj: string, page = 1, pageSize = 50) =>
    getPage("/fiscal/documentos/contatos", cnpj, page, pageSize),
  getFiscalizacaoCadastro: (cnpj: string) =>
    api
      .get<FiscalizacaoCadastroRecord>("/fiscal/fiscalizacao/cadastro", { params: { cnpj } })
      .then((response) => response.data),
  getFiscalizacaoMalhas: (cnpj: string, page = 1, pageSize = 50) =>
    getPage("/fiscal/fiscalizacao/malhas", cnpj, page, pageSize),
  getFiscalizacaoDsfs: (cnpj: string) =>
    api
      .get<FiscalizacaoDsfRecord[]>("/fiscal/fiscalizacao/dsfs", { params: { cnpj } })
      .then((response) => response.data),
};
