import { useCallback, useState } from "react";
import type {
  AlvoAnalise,
  ApiErrorResponse,
  CamadaTabela,
  ConfiguracaoSistema,
  DadosCadastrais,
  DominioNFe,
  FatorConversao,
  OracleColuna,
  OracleMapeamentoRaizResponse,
  OracleMapeamentoFonte,
  OracleObjeto,
  OracleValidacaoMapeamento,
  ParquetFileInfo,
  ParquetReadResponse,
  ReferenciaNCM,
  ReferenciaCEST,
  ReferenciaCFOP,
  ReferenciaCST,
  RespostaReferencia,
  ResultadoConsultaCadastral,
  ResumoAlvosAnalise,
  SistemaStatus,
  StatusPipeline,
} from "@/types/audit";

const API_BASE = "/api";

interface ApiState<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
}

function normalizarFatorConversao(payload: Record<string, unknown>): FatorConversao {
  const descricaoCanonica =
    typeof payload.descricao_padrao === "string" && payload.descricao_padrao.trim()
      ? payload.descricao_padrao.trim()
      : typeof payload.descr_padrao === "string" && payload.descr_padrao.trim()
        ? payload.descr_padrao.trim()
        : "";

  return {
    id_agrupado: String(payload.id_agrupado ?? ""),
    descricao_padrao: descricaoCanonica,
    unid_compra: payload.unid_compra == null ? null : String(payload.unid_compra),
    unid_venda: payload.unid_venda == null ? null : String(payload.unid_venda),
    unid_ref: String(payload.unid_ref ?? ""),
    fator_compra_ref: Number(payload.fator_compra_ref ?? 0),
    fator_venda_ref: Number(payload.fator_venda_ref ?? 0),
    origem_fator: String(payload.origem_fator ?? ""),
    status: (payload.status as FatorConversao["status"]) ?? "pendente",
    editado_em: payload.editado_em == null ? null : String(payload.editado_em),
  };
}

export function serializarDetalheErroApi(valor: unknown): string {
  if (typeof valor === "string") {
    return valor;
  }

  if (Array.isArray(valor)) {
    const mensagens = valor
      .map((item) => {
        if (typeof item === "string") {
          return item;
        }
        if (item && typeof item === "object") {
          const registro = item as Record<string, unknown>;
          const local = Array.isArray(registro.loc) ? registro.loc.join(".") : null;
          const mensagem = typeof registro.msg === "string" ? registro.msg : null;
          return [local, mensagem].filter(Boolean).join(": ");
        }
        return String(item);
      })
      .filter(Boolean);

    return mensagens.join(" | ");
  }

  if (valor && typeof valor === "object") {
    const registro = valor as Record<string, unknown>;
    if (typeof registro.mensagem === "string") {
      return registro.mensagem;
    }
    if (typeof registro.detail === "string") {
      return registro.detail;
    }
    if (typeof registro.detalhe === "string") {
      return registro.detalhe;
    }

    try {
      return JSON.stringify(registro);
    } catch {
      return "Erro nao estruturado retornado pela API";
    }
  }

  return "Erro inesperado ao processar resposta da API";
}

export function construirMensagemErroApi(
  status: number,
  payload?: ApiErrorResponse | Record<string, unknown> | null,
  textoResposta?: string,
): string {
  if (payload) {
    if (typeof payload.mensagem === "string" && payload.mensagem.trim()) {
      const detalhe = payload.detalhe ?? payload.detail;
      if (detalhe) {
        return `${payload.mensagem}: ${serializarDetalheErroApi(detalhe)}`;
      }
      return payload.mensagem;
    }

    const detalhe = payload.detalhe ?? payload.detail;
    if (detalhe) {
      return serializarDetalheErroApi(detalhe);
    }
  }

  if (textoResposta && textoResposta.trim()) {
    return textoResposta.trim();
  }

  return `Erro ${status}`;
}

async function fetchApi<T>(endpoint: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${endpoint}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });

  if (!response.ok) {
    const textoResposta = await response.text();
    let errorData: ApiErrorResponse | null = null;

    if (textoResposta) {
      try {
        errorData = JSON.parse(textoResposta) as ApiErrorResponse;
      } catch {
        errorData = null;
      }
    }

    throw new Error(construirMensagemErroApi(response.status, errorData, textoResposta));
  }

  return response.json();
}

export function useHealthCheck() {
  const [state, setState] = useState<ApiState<{ status: string; version: string }>>({
    data: null,
    loading: false,
    error: null,
  });

  const check = useCallback(async () => {
    setState({ data: null, loading: true, error: null });
    try {
      const data = await fetchApi<{ status: string; version: string }>("/health");
      setState({ data, loading: false, error: null });
    } catch (error) {
      setState({ data: null, loading: false, error: (error as Error).message });
    }
  }, []);

  return { ...state, check };
}

export function useSistema() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const carregarStatus = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      return await fetchApi<SistemaStatus>("/sistema/status");
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  const carregarConfiguracoes = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      return await fetchApi<{ status: string; configuracoes: ConfiguracaoSistema }>("/configuracoes");
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  const salvarConfiguracoes = useCallback(async (configuracoes: Partial<ConfiguracaoSistema>) => {
    setLoading(true);
    setError(null);
    try {
      return await fetchApi<{ status: string; mensagem: string; configuracoes: ConfiguracaoSistema }>(
        "/configuracoes",
        {
          method: "PUT",
          body: JSON.stringify(configuracoes),
        },
      );
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  const listarConsultas = useCallback(async () => {
    return fetchApi<{
      status: string;
      consultas: string[];
      diretorio_consultas_sql: string;
      diretorios_sugeridos: { chave: string; rotulo: string; caminho: string }[];
    }>("/consultas");
  }, []);

  const testarConexaoOracle = useCallback(async (indice?: number) => {
    const sufixo = indice === undefined ? "" : `?indice=${indice}`;
    return fetchApi<{
      status: string;
      conexao: { status: string; usuario: string; banco: string; host: string };
    }>(`/oracle/conexao${sufixo}`);
  }, []);

  const listarMapeamentosOracle = useCallback(async () => {
    return fetchApi<{ status: string; mapeamentos: OracleMapeamentoFonte[] }>("/oracle/mapeamentos");
  }, []);

  const salvarMapeamentosOracle = useCallback(async (mapeamentos: Record<string, string | null>) => {
    setLoading(true);
    setError(null);
    try {
      return await fetchApi<{ status: string; mensagem: string; mapeamentos: OracleMapeamentoFonte[] }>(
        "/oracle/mapeamentos",
        {
          method: "PUT",
          body: JSON.stringify({ mapeamentos }),
        },
      );
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  const validarMapeamentosOracle = useCallback(async (indice?: number) => {
    const sufixo = indice === undefined ? "" : `?indice=${indice}`;
    return fetchApi<{
      status: string;
      validacoes: OracleValidacaoMapeamento[];
      total_ok: number;
      total_erro: number;
    }>(`/oracle/mapeamentos/validacao${sufixo}`);
  }, []);

  const buscarFontesOracle = useCallback(async (termo: string, indice?: number, limite = 50) => {
    const params = new URLSearchParams({
      termo,
      limite: String(limite),
    });
    if (indice !== undefined) params.set("indice", String(indice));
    return fetchApi<{ status: string; objetos: OracleObjeto[] }>(`/oracle/fontes?${params}`);
  }, []);

  const listarColunasOracle = useCallback(async (objeto: string, owner?: string, indice?: number) => {
    const params = new URLSearchParams();
    if (indice !== undefined) params.set("indice", String(indice));
    if (owner) params.set("owner", owner);
    const sufixo = params.toString() ? `?${params}` : "";
    return fetchApi<{ status: string; colunas: OracleColuna[] }>(`/oracle/colunas/${objeto}${sufixo}`);
  }, []);

  const carregarMapeamentoRaizOracle = useCallback(async (diretorio?: string) => {
    const params = new URLSearchParams();
    if (diretorio?.trim()) params.set("diretorio", diretorio.trim());
    const sufixo = params.toString() ? `?${params}` : "";
    return fetchApi<OracleMapeamentoRaizResponse>(`/oracle/mapeamento-raiz${sufixo}`);
  }, []);

  return {
    loading,
    error,
    carregarStatus,
    carregarConfiguracoes,
    salvarConfiguracoes,
    listarConsultas,
    testarConexaoOracle,
    listarMapeamentosOracle,
    salvarMapeamentosOracle,
    validarMapeamentosOracle,
    buscarFontesOracle,
    listarColunasOracle,
    carregarMapeamentoRaizOracle,
  };
}

export function useAlvosAnalise() {
  const [state, setState] = useState<ApiState<{ status: string; resumo: ResumoAlvosAnalise; alvos: AlvoAnalise[] }>>({
    data: null,
    loading: false,
    error: null,
  });

  const listar = useCallback(async () => {
    setState({ data: null, loading: true, error: null });
    try {
      const data = await fetchApi<{ status: string; resumo: ResumoAlvosAnalise; alvos: AlvoAnalise[] }>("/sistema/alvos");
      setState({ data, loading: false, error: null });
      return data;
    } catch (erro) {
      setState({ data: null, loading: false, error: (erro as Error).message });
      throw erro;
    }
  }, []);

  return { ...state, listar };
}

export function useCadastro() {
  const [state, setState] = useState<
    ApiState<{ status: string; documentos_processados: number; resultados: ResultadoConsultaCadastral[] }>
  >({
    data: null,
    loading: false,
    error: null,
  });

  const consultar = useCallback(async (documentos: string[], indiceOracle?: number) => {
    setState({ data: null, loading: true, error: null });
    try {
      const data = await fetchApi<{ status: string; documentos_processados: number; resultados: ResultadoConsultaCadastral[] }>(
        "/cadastro/consultar",
        {
          method: "POST",
          body: JSON.stringify({
            documentos,
            indice_oracle: indiceOracle,
          }),
        },
      );
      setState({ data, loading: false, error: null });
      return data;
    } catch (erro) {
      setState({ data: null, loading: false, error: (erro as Error).message });
      throw erro;
    }
  }, []);

  const criarResultadoLocal = useCallback(
    (
      documento: string,
      tipoDocumento: "cpf" | "cnpj",
      dados: Partial<DadosCadastrais> & Pick<DadosCadastrais, "documento">,
      origem: "storage" | "misto" = "storage",
    ): ResultadoConsultaCadastral => ({
      status: "ok",
      tipo_documento: tipoDocumento,
      documento_consultado: documento,
      origem,
      encontrado: true,
      mensagem: null,
      registros: [
        {
          documento: dados.documento,
          ie: dados.ie ?? null,
          nome: dados.nome ?? null,
          nome_fantasia: dados.nome_fantasia ?? null,
          endereco: dados.endereco ?? null,
          municipio: dados.municipio ?? null,
          uf: dados.uf ?? null,
          regime_pagamento: dados.regime_pagamento ?? null,
          situacao_ie: dados.situacao_ie ?? null,
          data_inicio_atividade: dados.data_inicio_atividade ?? null,
          data_ultima_situacao: dados.data_ultima_situacao ?? null,
          periodo_atividade: dados.periodo_atividade ?? null,
          url_redesim: dados.url_redesim ?? null,
        },
      ],
    }),
    [],
  );

  return { ...state, consultar, criarResultadoLocal };
}

export function usePipeline() {
  const [state, setState] = useState<ApiState<StatusPipeline>>({
    data: null,
    loading: false,
    error: null,
  });

  const executar = useCallback(async (cnpj: string, consultas: string[], dataLimite?: string, indiceOracle?: number) => {
    setState({ data: null, loading: true, error: null });
    try {
      const data = await fetchApi<StatusPipeline>("/pipeline/executar", {
        method: "POST",
        body: JSON.stringify({
          cnpj,
          consultas,
          data_limite: dataLimite,
          executar_extracao: consultas.length > 0,
          indice_oracle: indiceOracle,
        }),
      });
      setState({ data, loading: false, error: null });
      return data;
    } catch (erro) {
      setState({ data: null, loading: false, error: (erro as Error).message });
      throw erro;
    }
  }, []);

  const reprocessar = useCallback(async (cnpj: string, tabelaEditada: string) => {
    setState({ data: null, loading: true, error: null });
    try {
      const data = await fetchApi<StatusPipeline>(
        `/pipeline/reprocessar?cnpj=${cnpj}&tabela_editada=${tabelaEditada}`,
        { method: "POST" },
      );
      setState({ data, loading: false, error: null });
      return data;
    } catch (erro) {
      setState({ data: null, loading: false, error: (erro as Error).message });
      throw erro;
    }
  }, []);

  const verificarStatus = useCallback(async (cnpj: string) => {
    return fetchApi<{ status: string; cnpj: string; tabelas: Record<string, boolean>; completo: boolean }>(
      `/pipeline/status/${cnpj}`,
    );
  }, []);

  return { ...state, executar, reprocessar, verificarStatus };
}

export function useTabelas() {
  const [tabelas, setTabelas] = useState<ParquetFileInfo[]>([]);
  const [dadosTabela, setDadosTabela] = useState<ParquetReadResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const listar = useCallback(async (cnpj: string, camada: CamadaTabela = "parquets") => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchApi<{ status: string; camada: string; tabelas: ParquetFileInfo[] }>(
        `/tabelas/${cnpj}?camada=${camada}`,
      );
      setTabelas(data.tabelas);
      return data.tabelas;
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  const ler = useCallback(
    async (
      cnpj: string,
      nomeTabela: string,
      camada: CamadaTabela = "parquets",
      pagina = 1,
      porPagina = 50,
      filtroColuna?: string,
      filtroValor?: string,
      ordenarPor?: string,
      ordem: "asc" | "desc" = "asc",
    ) => {
      setLoading(true);
      setError(null);
      try {
        const params = new URLSearchParams({
          pagina: String(pagina),
          por_pagina: String(porPagina),
          ordem,
        });
        if (filtroColuna) params.set("filtro_coluna", filtroColuna);
        if (filtroValor) params.set("filtro_valor", filtroValor);
        if (ordenarPor) params.set("ordenar_por", ordenarPor);
        params.set("camada", camada);

        const data = await fetchApi<ParquetReadResponse>(`/tabelas/${cnpj}/${nomeTabela}?${params}`);
        setDadosTabela(data);
        return data;
      } catch (erro) {
        setError((erro as Error).message);
        throw erro;
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  const lerTodasPaginas = useCallback(
    async (
      cnpj: string,
      nomeTabela: string,
      camada: CamadaTabela = "parquets",
      filtroColuna?: string,
      filtroValor?: string,
      ordenarPor?: string,
      ordem: "asc" | "desc" = "asc",
      porPagina = 1000,
    ) => {
      let paginaAtual = 1;
      let totalPaginas = 1;
      let respostaInicial: ParquetReadResponse | null = null;
      const dadosAcumulados: Record<string, unknown>[] = [];

      while (paginaAtual <= totalPaginas) {
        try {
          const resposta = await ler(
            cnpj,
            nomeTabela,
            camada,
            paginaAtual,
            porPagina,
            filtroColuna,
            filtroValor,
            ordenarPor,
            ordem,
          );

          if (!respostaInicial) {
            respostaInicial = resposta;
          }

          dadosAcumulados.push(...resposta.dados);
          totalPaginas = resposta.total_paginas || 1;
          paginaAtual += 1;
        } catch (erro) {
          const erroFinal = erro instanceof Error ? erro : new Error("Falha ao ler tabela paginada");
          Object.assign(erroFinal, { dadosParciais: dadosAcumulados });
          throw erroFinal;
        }
      }

      if (!respostaInicial) {
        return null;
      }

      const respostaCompleta: ParquetReadResponse = {
        ...respostaInicial,
        dados: dadosAcumulados,
        total_registros: dadosAcumulados.length,
        pagina: 1,
        por_pagina: porPagina,
        total_paginas: totalPaginas,
      };

      setDadosTabela(respostaCompleta);
      return respostaCompleta;
    },
    [ler],
  );

  const exportar = useCallback(async (cnpj: string, nomeTabela: string, formato: "xlsx" | "csv" | "parquet") => {
    const endpoint = `${API_BASE}/exportar/${cnpj}/${nomeTabela}?formato=${formato}`;
    const resposta = await fetch(endpoint);
    if (!resposta.ok) {
      const textoResposta = await resposta.text();
      let payloadErro: ApiErrorResponse | null = null;

      if (textoResposta) {
        try {
          payloadErro = JSON.parse(textoResposta) as ApiErrorResponse;
        } catch {
          payloadErro = null;
        }
      }

      throw new Error(construirMensagemErroApi(resposta.status, payloadErro, textoResposta));
    }

    const blob = await resposta.blob();
    const url = window.URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${nomeTabela}.${formato}`;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    window.URL.revokeObjectURL(url);
  }, []);

  return { tabelas, dadosTabela, loading, error, listar, ler, lerTodasPaginas, exportar };
}

export function useAgregacao() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const agregar = useCallback(async (cnpj: string, idsProdutos: string[], descricaoPadrao?: string) => {
    setLoading(true);
    setError(null);
    try {
      return await fetchApi<{ status: string; mensagem: string }>(`/agregacao/agregar?cnpj=${cnpj}`, {
        method: "POST",
        body: JSON.stringify({
          ids_produtos: idsProdutos,
          descricao_padrao: descricaoPadrao,
        }),
      });
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  const desagregar = useCallback(async (cnpj: string, idGrupo: string) => {
    setLoading(true);
    setError(null);
    try {
      return await fetchApi<{ status: string; mensagem: string }>(`/agregacao/desagregar?cnpj=${cnpj}`, {
        method: "POST",
        body: JSON.stringify({ id_grupo: idGrupo }),
      });
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  return { loading, error, agregar, desagregar };
}

export function useConversao() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const editarFator = useCallback(
    async (
      cnpj: string,
      idAgrupado: string,
      payload: {
        unid_ref?: string;
        fator?: number;
        fator_compra_ref?: number;
        fator_venda_ref?: number;
      },
    ) => {
      setLoading(true);
      setError(null);
      try {
        return await fetchApi<{ status: string; mensagem: string }>(`/conversao/fator?cnpj=${cnpj}`, {
          method: "PUT",
          body: JSON.stringify({ id_agrupado: idAgrupado, ...payload }),
        });
      } catch (erro) {
        setError((erro as Error).message);
        throw erro;
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  const recalcular = useCallback(async (cnpj: string) => {
    setLoading(true);
    setError(null);
    try {
      return await fetchApi<{ status: string; mensagem: string }>(`/conversao/recalcular?cnpj=${cnpj}`, {
        method: "POST",
      });
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  const listarFatores = useCallback(async (cnpj: string): Promise<FatorConversao[]> => {
    setLoading(true);
    setError(null);

    let paginaAtual = 1;
    let totalPaginas = 1;
    const fatoresAcumulados: FatorConversao[] = [];

    try {
      while (paginaAtual <= totalPaginas) {
        const resposta = await fetchApi<ParquetReadResponse>(
          `/tabelas/${cnpj}/fatores_conversao?pagina=${paginaAtual}&por_pagina=1000`,
        );

        fatoresAcumulados.push(
          ...(resposta.dados as Record<string, unknown>[]).map((registro) => normalizarFatorConversao(registro)),
        );
        totalPaginas = resposta.total_paginas || 1;
        paginaAtual += 1;
      }

      return fatoresAcumulados;
    } catch (erro) {
      const erroFinal = erro instanceof Error ? erro : new Error("Falha ao listar fatores");
      Object.assign(erroFinal, { dadosParciais: fatoresAcumulados });
      setError(erroFinal.message);
      throw erroFinal;
    } finally {
      setLoading(false);
    }
  }, []);

  return { loading, error, editarFator, recalcular, listarFatores };
}

// =============================================================================
// HOOKS DE REFERÊNCIAS FISCAIS
// =============================================================================

export function useReferencias() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const listarNCM = useCallback(async (params?: { codigo?: string; limite?: number }): Promise<ReferenciaNCM[]> => {
    setLoading(true);
    setError(null);
    try {
      const searchParams = new URLSearchParams();
      if (params?.codigo) searchParams.set("codigo", params.codigo);
      if (params?.limite) searchParams.set("limite", params.limite.toString());

      const response = await fetch(`${API_BASE}/referencias/ncm?${searchParams}`);
      if (!response.ok) throw new Error("Falha ao carregar NCM");
      const data: RespostaReferencia<ReferenciaNCM[]> = await response.json();
      return Array.isArray(data.dados) ? data.dados : [];
    } catch (err) {
      const erro = err as Error;
      setError(erro.message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  const buscarNCM = useCallback(async (codigo: string): Promise<ReferenciaNCM | null> => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${API_BASE}/referencias/ncm/${codigo}`);
      if (response.status === 404) return null;
      if (!response.ok) throw new Error("Falha ao buscar NCM");
      const data: RespostaReferencia<ReferenciaNCM> = await response.json();
      return data.dados as ReferenciaNCM;
    } catch (err) {
      const erro = err as Error;
      setError(erro.message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  const listarCEST = useCallback(async (params?: { codigo?: string; limite?: number }): Promise<ReferenciaCEST[]> => {
    setLoading(true);
    setError(null);
    try {
      const searchParams = new URLSearchParams();
      if (params?.codigo) searchParams.set("codigo", params.codigo);
      if (params?.limite) searchParams.set("limite", params.limite.toString());

      const response = await fetch(`${API_BASE}/referencias/cest?${searchParams}`);
      if (!response.ok) throw new Error("Falha ao carregar CEST");
      const data: RespostaReferencia<ReferenciaCEST[]> = await response.json();
      return Array.isArray(data.dados) ? data.dados : [];
    } catch (err) {
      const erro = err as Error;
      setError(erro.message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  const buscarCEST = useCallback(async (codigo: string): Promise<ReferenciaCEST | null> => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${API_BASE}/referencias/cest/${codigo}`);
      if (response.status === 404) return null;
      if (!response.ok) throw new Error("Falha ao buscar CEST");
      const data: RespostaReferencia<ReferenciaCEST> = await response.json();
      return data.dados as ReferenciaCEST;
    } catch (err) {
      const erro = err as Error;
      setError(erro.message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  const listarCFOP = useCallback(async (params?: { codigo?: string; limite?: number }): Promise<ReferenciaCFOP[]> => {
    setLoading(true);
    setError(null);
    try {
      const searchParams = new URLSearchParams();
      if (params?.codigo) searchParams.set("codigo", params.codigo);
      if (params?.limite) searchParams.set("limite", params.limite.toString());

      const response = await fetch(`${API_BASE}/referencias/cfop?${searchParams}`);
      if (!response.ok) throw new Error("Falha ao carregar CFOP");
      const data: RespostaReferencia<ReferenciaCFOP[]> = await response.json();
      return Array.isArray(data.dados) ? data.dados : [];
    } catch (err) {
      const erro = err as Error;
      setError(erro.message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  const buscarCFOP = useCallback(async (codigo: string): Promise<ReferenciaCFOP | null> => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${API_BASE}/referencias/cfop/${codigo}`);
      if (response.status === 404) return null;
      if (!response.ok) throw new Error("Falha ao buscar CFOP");
      const data: RespostaReferencia<ReferenciaCFOP> = await response.json();
      return data.dados as ReferenciaCFOP;
    } catch (err) {
      const erro = err as Error;
      setError(erro.message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  const listarCST = useCallback(async (params?: { codigo?: string; limite?: number }): Promise<ReferenciaCST[]> => {
    setLoading(true);
    setError(null);
    try {
      const searchParams = new URLSearchParams();
      if (params?.codigo) searchParams.set("codigo", params.codigo);
      if (params?.limite) searchParams.set("limite", params.limite.toString());

      const response = await fetch(`${API_BASE}/referencias/cst?${searchParams}`);
      if (!response.ok) throw new Error("Falha ao carregar CST");
      const data: RespostaReferencia<ReferenciaCST[]> = await response.json();
      return Array.isArray(data.dados) ? data.dados : [];
    } catch (err) {
      const erro = err as Error;
      setError(erro.message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  const listarDominiosNFe = useCallback(async (): Promise<Record<string, DominioNFe>> => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${API_BASE}/referencias/nfe/dominios`);
      if (!response.ok) throw new Error("Falha ao carregar domínios NFe");
      const data = await response.json();
      return data.dominios || {};
    } catch (err) {
      const erro = err as Error;
      setError(erro.message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  return {
    listarNCM,
    buscarNCM,
    listarCEST,
    buscarCEST,
    listarCFOP,
    buscarCFOP,
    listarCST,
    listarDominiosNFe,
    loading,
    error,
  };
}
