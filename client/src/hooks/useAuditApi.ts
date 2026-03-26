/*
 * useAuditApi — Hook para comunicação com o backend audit_engine
 * Centraliza chamadas à API Python (FastAPI)
 */
import { useState, useCallback } from "react";
import type {
  ParquetFileInfo,
  ParquetReadResponse,
  StatusPipeline,
  FatorConversao,
} from "@/types/audit";

const API_BASE = "/api";

interface ApiState<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
}

async function fetchApi<T>(
  endpoint: string,
  options?: RequestInit
): Promise<T> {
  const response = await fetch(`${API_BASE}${endpoint}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });

  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    throw new Error(errorData.detail || `Erro ${response.status}`);
  }

  return response.json();
}

// === Hook: Health Check ===

export function useHealthCheck() {
  const [state, setState] = useState<ApiState<{ status: string }>>({
    data: null,
    loading: false,
    error: null,
  });

  const check = useCallback(async () => {
    setState({ data: null, loading: true, error: null });
    try {
      const data = await fetchApi<{ status: string }>("/health");
      setState({ data, loading: false, error: null });
    } catch (e) {
      setState({ data: null, loading: false, error: (e as Error).message });
    }
  }, []);

  return { ...state, check };
}

// === Hook: Pipeline ===

export function usePipeline() {
  const [state, setState] = useState<ApiState<StatusPipeline>>({
    data: null,
    loading: false,
    error: null,
  });

  const executar = useCallback(
    async (cnpj: string, consultas: string[], dataLimite?: string) => {
      setState({ data: null, loading: true, error: null });
      try {
        const data = await fetchApi<StatusPipeline>("/pipeline/executar", {
          method: "POST",
          body: JSON.stringify({ cnpj, consultas, data_limite: dataLimite }),
        });
        setState({ data, loading: false, error: null });
        return data;
      } catch (e) {
        setState({ data: null, loading: false, error: (e as Error).message });
        throw e;
      }
    },
    []
  );

  const reprocessar = useCallback(
    async (cnpj: string, tabelaEditada: string) => {
      setState({ data: null, loading: true, error: null });
      try {
        const data = await fetchApi<StatusPipeline>(
          `/pipeline/reprocessar?cnpj=${cnpj}&tabela_editada=${tabelaEditada}`,
          { method: "POST" }
        );
        setState({ data, loading: false, error: null });
        return data;
      } catch (e) {
        setState({ data: null, loading: false, error: (e as Error).message });
        throw e;
      }
    },
    []
  );

  const verificarStatus = useCallback(async (cnpj: string) => {
    try {
      return await fetchApi<StatusPipeline>(`/pipeline/status/${cnpj}`);
    } catch {
      return null;
    }
  }, []);

  return { ...state, executar, reprocessar, verificarStatus };
}

// === Hook: Tabelas ===

export function useTabelas() {
  const [tabelas, setTabelas] = useState<ParquetFileInfo[]>([]);
  const [dadosTabela, setDadosTabela] = useState<ParquetReadResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const listar = useCallback(async (cnpj: string) => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchApi<{ tabelas: ParquetFileInfo[] }>(
        `/tabelas/${cnpj}`
      );
      setTabelas(data.tabelas);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, []);

  const ler = useCallback(
    async (
      cnpj: string,
      nomeTabela: string,
      pagina = 1,
      porPagina = 50,
      filtroColuna?: string,
      filtroValor?: string,
      ordenarPor?: string,
      ordem: "asc" | "desc" = "asc"
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

        const data = await fetchApi<ParquetReadResponse>(
          `/tabelas/${cnpj}/${nomeTabela}?${params}`
        );
        setDadosTabela(data);
        return data;
      } catch (e) {
        setError((e as Error).message);
        return null;
      } finally {
        setLoading(false);
      }
    },
    []
  );

  return { tabelas, dadosTabela, loading, error, listar, ler };
}

// === Hook: Agregação ===

export function useAgregacao() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const agregar = useCallback(
    async (cnpj: string, idsProdutos: number[], descricaoPadrao?: string) => {
      setLoading(true);
      setError(null);
      try {
        return await fetchApi(`/agregacao/agregar?cnpj=${cnpj}`, {
          method: "POST",
          body: JSON.stringify({
            ids_produtos: idsProdutos,
            descricao_padrao: descricaoPadrao,
          }),
        });
      } catch (e) {
        setError((e as Error).message);
        throw e;
      } finally {
        setLoading(false);
      }
    },
    []
  );

  const desagregar = useCallback(
    async (cnpj: string, idGrupo: string) => {
      setLoading(true);
      setError(null);
      try {
        return await fetchApi(`/agregacao/desagregar?cnpj=${cnpj}`, {
          method: "POST",
          body: JSON.stringify({ id_grupo: idGrupo }),
        });
      } catch (e) {
        setError((e as Error).message);
        throw e;
      } finally {
        setLoading(false);
      }
    },
    []
  );

  return { loading, error, agregar, desagregar };
}

// === Hook: Conversão ===

export function useConversao() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const editarFator = useCallback(
    async (
      cnpj: string,
      idAgrupado: string,
      unidRef?: string,
      fator?: number
    ) => {
      setLoading(true);
      setError(null);
      try {
        return await fetchApi(`/conversao/fator?cnpj=${cnpj}`, {
          method: "PUT",
          body: JSON.stringify({
            id_agrupado: idAgrupado,
            unid_ref: unidRef,
            fator,
          }),
        });
      } catch (e) {
        setError((e as Error).message);
        throw e;
      } finally {
        setLoading(false);
      }
    },
    []
  );

  const recalcular = useCallback(async (cnpj: string) => {
    setLoading(true);
    setError(null);
    try {
      return await fetchApi(`/conversao/recalcular?cnpj=${cnpj}`, {
        method: "POST",
      });
    } catch (e) {
      setError((e as Error).message);
      throw e;
    } finally {
      setLoading(false);
    }
  }, []);

  return { loading, error, editarFator, recalcular };
}
