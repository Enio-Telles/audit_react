/*
 * Tipos do Sistema de Auditoria Fiscal — audit_react
 * Contratos compartilhados entre frontend e backend
 */

// === Tabelas do Sistema ===

export type NomeTabela =
  | "produtos_unidades"
  | "produtos"
  | "produtos_agrupados"
  | "produtos_final"
  | "fatores_conversao"
  | "nfe_entrada"
  | "id_agrupados"
  | "mov_estoque"
  | "aba_mensal"
  | "aba_anual"
  | "produtos_selecionados";

export interface MetadadoTabela {
  nome: NomeTabela;
  dependencias: string[];
  saida: string;
  modulo: string;
  funcao: string;
}

// === Extração ===

export interface ConsultaSQL {
  id: string;
  nome: string;
  categoria: string;
  arquivo?: string;
  parametros?: ParametroSQL[];
}

export interface ParametroSQL {
  nome: string;
  tipo: "string" | "date" | "number";
  obrigatorio: boolean;
  valor_padrao?: string;
}

export interface ExecucaoExtracao {
  id: string;
  cnpj: string;
  data_inicio: string;
  data_fim?: string;
  status: "agendada" | "executando" | "concluida" | "erro";
  etapas: EtapaPipeline[];
  erros: string[];
  arquivos_gerados: string[];
}

export interface EtapaPipeline {
  etapa?: string;
  tabela?: string;
  status: "pendente" | "executando" | "concluida" | "erro" | "pulada";
  mensagem?: string;
  duracao_ms?: number;
  registros?: number;
  registros_gerados?: number;
}

// === Produtos e Agregação ===

export interface ProdutoUnidade {
  id: number;
  descricao: string;
  ncm: string;
  cest: string;
  unidade: string;
  gtin?: string;
  qtd_nfe: number;
  tipo: "compra" | "venda" | "ambos";
}

export interface GrupoAgregacao {
  id: string;
  descricao_padrao: string;
  ncm_padrao: string;
  cest_padrao: string;
  membros: number[];
  criado_em: string;
  origem: "manual" | "automatico";
}

export interface HistoricoAgregacao {
  id: string;
  tipo: "agregar" | "desagregar" | "reprocessar";
  grupo_id: string;
  produtos_ids: number[];
  data: string;
  usuario?: string;
}

// === Conversão ===

export interface FatorConversao {
  id: number;
  id_agrupado: string;
  descricao_padrao: string;
  unid_compra: string;
  unid_venda: string;
  unid_ref: string;
  fator: number;
  status: "ok" | "pendente";
  editado_em?: string;
}

// === Estoque ===

export interface MovimentoEstoque {
  id: number;
  id_agrupado: string;
  descricao: string;
  tipo: "ENTRADA" | "SAIDA" | "INVENTARIO";
  data: string;
  quantidade: number;
  valor_unitario: number;
  valor_total: number;
  saldo: number;
  cfop?: string;
}

export interface ConsolidacaoMensal {
  mes: string;
  id_agrupado: string;
  entradas: number;
  saidas: number;
  saldo_inicial: number;
  saldo_final: number;
  custo_medio: number;
  valor_estoque: number;
}

export interface ConsolidacaoAnual {
  ano: string;
  id_agrupado: string;
  total_entradas: number;
  total_saidas: number;
  saldo_inicial_ano: number;
  saldo_final_ano: number;
  custo_medio_anual: number;
  valor_estoque_final: number;
}

// === Parquet / Tabela Genérica ===

export interface ParquetFileInfo {
  nome: string;
  caminho: string;
  registros: number;
  colunas: string[];
  tamanho_bytes: number;
  atualizado_em: string;
}

export interface ParquetReadResponse {
  colunas: string[];
  dados: Record<string, unknown>[];
  total_registros: number;
  pagina: number;
  por_pagina: number;
  schema: Record<string, string>;
}

// === Pipeline ===

export interface StatusPipeline {
  cnpj: string;
  status: "idle" | "executando" | "concluido" | "erro" | "concluido_com_erros";
  etapa_atual?: string;
  progresso?: number;
  etapas: EtapaPipeline[];
  erros: string[];
  inicio?: string;
  fim?: string;
  duracao_ms?: number;
  tabelas_geradas?: string[];
}

export interface DependenciaTabela {
  tabela: NomeTabela;
  depende_de: NomeTabela[];
  gera: NomeTabela[];
}

// === Configurações ===

export interface ConfiguracaoOracle {
  host: string;
  porta: number;
  service_name: string;
  schema: string;
  usuario: string;
  senha: string;
}

export interface ConfiguracaoSistema {
  oracle: ConfiguracaoOracle;
  diretorio_cnpjs: string;
  diretorio_consultas: string;
  diretorio_exportacoes: string;
  reprocessamento_automatico: boolean;
  logs_detalhados: boolean;
  exportacao_formatada: boolean;
}
