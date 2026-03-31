export interface DadosAuditor {
  nome: string;
  cargo: string;
  matricula: string;
  orgao: string;
  endereco: string;
  local_data: string;
}

export interface ManifestacoesRelatorio {
  regularizou_integralmente: boolean;
  apresentou_contestacao: boolean;
  solicitou_prorrogacao: boolean;
  nao_apresentou_manifestacao: boolean;
}

export interface DadosRelatorio {
  cnpj: string;
  contribuinte: string;
  ie: string;
  dsf: string;
  notificacao_det: string;
  manifestacao: string;
  manifestacoes: ManifestacoesRelatorio;
  contatos_realizados: string;
  decisao_fiscal: string;
  desfecho: string;
  pdf_det?: string;
  arquivos_notificacao_incluidos: string[];
  aviso_det?: string;
}

export interface Dsf {
  numero: string;
  descricao: string;
  cnpjs: string[];
  origem?: "persistida" | "inferida" | "mesclada";
}

export interface CnpjComRelatorio {
  cnpj: string;
  contribuinte: string;
  dsf: string;
  notificacao_det: string;
  tem_det: boolean;
  manifestacao: string;
  aviso_det?: string;
}

export interface DetInfo {
  nome: string;
  caminho: string;
  tamanho_bytes: number;
  selecionado: boolean;
}

export interface DependenciaRelatorio {
  nome: string;
  instalado: boolean;
  mensagem: string;
}

export interface ModeloDocxDiagnostico {
  tipo: "individual" | "geral";
  variavel_ambiente: string;
  caminho_resolvido: string;
  existe: boolean;
  pronto: boolean;
  mensagem: string;
}

export interface DiagnosticoPipelineLocal {
  cnpj_referencia: string;
  completo: boolean;
  tabelas: Record<string, boolean>;
  total_tabelas_ok: number;
  total_tabelas: number;
}

export interface DiagnosticoRelatorios {
  pronto_pdf: boolean;
  dependencias: DependenciaRelatorio[];
  dependencias_faltantes: string[];
  modelos_docx: {
    individual: ModeloDocxDiagnostico;
    geral: ModeloDocxDiagnostico;
  };
  total_cnpjs_com_relatorio: number;
  total_dsfs: number;
  cnpjs_com_relatorio: CnpjComRelatorio[];
  dsfs: Dsf[];
  pipeline_local: DiagnosticoPipelineLocal;
}
