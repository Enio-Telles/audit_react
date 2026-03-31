export type NomeTabela =
  | "produtos_unidades"
  | "produtos"
  | "produtos_agrupados"
  | "fatores_conversao"
  | "produtos_final"
  | "id_agrupados"
  | "nfe_entrada"
  | "mov_estoque"
  | "aba_mensal"
  | "aba_anual"
  | "produtos_selecionados"
  | "ajustes_e111"
  | "st_itens";

export interface EtapaPipeline {
  tabela: string;
  status: "pendente" | "executando" | "concluida" | "erro" | "pulada";
  mensagem?: string;
  duracao_ms?: number;
  registros?: number;
  arquivo_saida?: string;
}

export interface ResultadoExtracao {
  cnpj: string;
  status: "ok" | "concluido_com_erros";
  diretorio_extraidos: string;
  consultas: Record<string, { status: "ok" | "erro"; linhas: number; arquivo?: string; mensagem?: string }>;
  erros: string[];
  total_linhas: number;
}

export interface StatusPipeline {
  cnpj: string;
  status: "concluido" | "concluido_com_erros" | "erro";
  duracao_ms: number;
  tabelas_geradas: string[];
  erros: string[];
  erros_pipeline: string[];
  erros_extracao: string[];
  erros_total: string[];
  etapas: EtapaPipeline[];
  extracao?: ResultadoExtracao | null;
  mensagem?: string;
  detalhe?: string | Record<string, unknown> | unknown[];
}

export interface ParquetFileInfo {
  nome: string;
  caminho: string;
  registros: number;
  colunas: string[];
  tamanho_bytes: number;
  atualizado_em: string;
  descricao?: string;
  camada?: string;
}

export interface ParquetReadResponse {
  status: string;
  camada?: string;
  colunas: string[];
  dados: Record<string, unknown>[];
  total_registros: number;
  pagina: number;
  por_pagina: number;
  total_paginas: number;
  schema: Record<string, string>;
}

export type CamadaTabela = "extraidos" | "silver" | "parquets";

export interface FatorConversao {
  id_agrupado: string;
  descricao_padrao: string;
  unid_compra?: string | null;
  unid_venda?: string | null;
  unid_ref: string;
  fator_compra_ref: number;
  fator_venda_ref: number;
  origem_fator: string;
  status: "ok" | "pendente" | "erro";
  editado_em?: string | null;
}

export interface ConfiguracaoSistema {
  reprocessamento_automatico: boolean;
  logs_detalhados: boolean;
  exportacao_formatada: boolean;
  diretorio_consultas_sql: string;
  oracle_indice_ativo: number;
}

export interface OracleConexaoDisponivel {
  indice: number;
  host: string;
  porta: number | null;
  servico: string;
  configurada: boolean;
  ativa: boolean;
  erro: string | null;
}

export interface SistemaStatus {
  status: string;
  api: string;
  oracle_configurada: boolean;
  oracle_conectada: boolean;
  oracle_indice_ativo: number;
  erro_oracle: string | null;
  conexoes_oracle: OracleConexaoDisponivel[];
  diretorio_base_cnpj: string;
  consultas_disponiveis: string[];
  fontes_oracle: Record<string, string>;
  fontes_oracle_detalhadas: OracleMapeamentoFonte[];
}

export interface AlvoAnalise {
  cnpj: string;
  contribuinte: string | null;
  ie: string | null;
  dsf: string | null;
  cpfs_vinculados: string[];
  possui_relatorio: boolean;
  possui_extraidos: boolean;
  total_parquets: number;
  total_tabelas_ok: number;
  total_tabelas_esperadas: number;
  status_pipeline: "completo" | "parcial" | "nao_iniciado";
  atualizado_em: string | null;
}

export interface ResumoAlvosAnalise {
  total_cnpjs: number;
  total_cpfs_mapeados: number;
  total_cnpjs_com_pipeline: number;
  total_cnpjs_com_pipeline_completo: number;
  total_cnpjs_com_relatorio: number;
}

export interface DadosCadastrais {
  documento: string;
  ie: string | null;
  nome: string | null;
  nome_fantasia: string | null;
  endereco: string | null;
  municipio: string | null;
  uf: string | null;
  regime_pagamento: string | null;
  situacao_ie: string | null;
  data_inicio_atividade: string | null;
  data_ultima_situacao: string | null;
  periodo_atividade: string | null;
  url_redesim: string | null;
}

export interface ResultadoConsultaCadastral {
  status: "ok" | "erro" | "invalido";
  tipo_documento: "cpf" | "cnpj" | "desconhecido";
  documento_consultado: string;
  origem: "oracle" | "storage" | "misto" | "entrada";
  encontrado: boolean;
  mensagem: string | null;
  registros: DadosCadastrais[];
}

export interface SelecaoOperacional {
  tipo_selecao: "cnpj" | "cpf" | "lote" | "indefinido";
  documento_principal: string;
  documentos_origem: string[];
  cnpjs_resolvidos: string[];
  resultados_cadastrais: ResultadoConsultaCadastral[];
}

export interface OracleMapeamentoFonte {
  chave: string;
  env_var: string;
  fonte_padrao: string;
  fonte_configurada: string;
  origem: "persistido" | "env" | "padrao" | string;
  owner: string;
  objeto: string;
}

export interface OracleObjeto {
  owner: string;
  object_name: string;
  object_type: string;
}

export interface OracleColuna {
  owner: string;
  object_name: string;
  column_name: string;
  data_type: string;
  data_length: number | null;
  data_precision: number | null;
  data_scale: number | null;
  nullable: string | null;
}

export interface OracleValidacaoMapeamento extends OracleMapeamentoFonte {
  existe: boolean;
  total_colunas_amostra: number;
  colunas_amostra: string[];
  erro: string | null;
}

export interface OracleDiretorioSqlSugerido {
  chave: string;
  rotulo: string;
  caminho: string;
}

export interface OracleArquivoSqlAnalise {
  arquivo: string;
  caminho: string;
  objetivo_real: string;
  categorias: string[];
  ctes: string[];
  tabelas_raiz: string[];
  binds: string[];
  tem_bind_cnpj: boolean;
  tem_bind_periodo: boolean;
  tem_window_function: boolean;
  tem_group_by: boolean;
  tem_distinct: boolean;
  tem_union: boolean;
  dimensoes_fiscais: string[];
  gargalos: string[];
}

export interface OracleFonteRaizMapeada {
  fonte_oracle: string;
  owner: string;
  objeto: string;
  dominio: string;
  arquivos_sql: string[];
  camada_bronze: string;
  camada_silver: string;
  camada_gold: string;
  chave_principal: string[];
  chave_recorte: string;
  filtro_cnpj: string;
  filtro_temporal: string;
  colunas_fiscais_obrigatorias: string[];
  particoes_sugeridas: string[];
  paralelizavel: boolean;
  justificativa_tecnica: string;
  recomposicao_polars: string;
  dimensoes_fiscais: string[];
  ocorrencias: number;
}

export interface OracleBlocoExtracao {
  nome_bloco: string;
  fontes_oracle: string[];
  filtro_cnpj: string;
  filtro_temporal: string;
  chave_principal: string[];
  colunas_fiscais_obrigatorias: string[];
  paralelizavel: boolean;
  parquet_saida: string;
  justificativa: string;
}

export interface OracleMapeamentoRaizResponse {
  status: string;
  diretorio_analisado: string;
  diretorio_ativo: string;
  diretorios_sugeridos: OracleDiretorioSqlSugerido[];
  resumo: {
    total_sqls: number;
    total_fontes_raiz: number;
    total_blocos_extracao: number;
    total_sqls_com_bind_cnpj: number;
    total_sqls_com_window: number;
  };
  arquivos_sql: OracleArquivoSqlAnalise[];
  fontes_raiz: OracleFonteRaizMapeada[];
  blocos_extracao: OracleBlocoExtracao[];
  estrategia_polars: string[];
}

export interface ApiErrorResponse {
  status?: string;
  mensagem?: string;
  detalhe?: string | Record<string, unknown> | unknown[];
  detail?: string | Record<string, unknown> | unknown[];
  cnpj?: string;
  indice_oracle?: number;
}

// =============================================================================
// TIPOS DE REFERÊNCIAS FISCAIS
// =============================================================================

export interface ReferenciaNCM {
  Codigo_NCM: string;
  Capitulo: string;
  Descr_Capitulo: string;
  Posicao: string;
  Descr_Posicao: string;
  Descricao: string;
  Data_Inicio: string;
  Data_Fim: string | null;
  Ato_Legal: string | null;
}

export interface ReferenciaCEST {
  ITEM: number;
  CEST: string;
  NCM: string;
  DESCRICAO: string;
}

export interface ReferenciaCFOP {
  id: number;
  co_cfop: string;
  descricao: string;
  codigo_tributacao: string;
  finalidade: string;
  excluir_estorno: number;
  excluir_estoque: number;
  saida_faturamento: number;
  ciap: number;
  fat_simples: number;
  dev_simples: number;
  ativ_simples: number;
}

export interface ReferenciaCST {
  cst: string;
  descricao_cst: string;
  in_trib: string;
}

export interface DominioNFe {
  total_registros: number;
  colunas: string[];
}

export interface RespostaReferencia<T> {
  status: string;
  dados: T | T[];
  total: number;
  valido?: boolean;
}
