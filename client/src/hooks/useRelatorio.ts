import { useCallback, useState } from "react";
import type {
  DadosAuditor,
  DadosRelatorio,
  Dsf,
  CnpjComRelatorio,
  DetInfo,
  DiagnosticoRelatorios,
} from "@/types/relatorio";

const API_BASE = "/api";

async function extrairMensagemErro(response: Response): Promise<string> {
  const texto = await response.text();
  let mensagem = `Erro ${response.status}`;

  try {
    const payload = JSON.parse(texto);
    mensagem =
      payload.erro?.detalhe ||
      payload.erro?.mensagem ||
      payload.detail ||
      payload.mensagem ||
      mensagem;
  } catch {
    if (texto) mensagem = texto;
  }

  return mensagem;
}

async function fetchApi<T>(endpoint: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${endpoint}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });

  if (!response.ok) {
    throw new Error(await extrairMensagemErro(response));
  }

  return response.json();
}

export function useRelatorio() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const baixarArquivo = useCallback(
    async (endpoint: string, nomePadrao: string) => {
      const response = await fetch(`${API_BASE}${endpoint}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });

      if (!response.ok) {
        throw new Error(await extrairMensagemErro(response));
      }

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;

      const cd = response.headers.get("content-disposition");
      const match = cd?.match(/filename="?([^";\n]+)"?/);
      anchor.download = match?.[1] || nomePadrao;

      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      window.URL.revokeObjectURL(url);
    },
    [],
  );

  // ---- Auditor ----

  const carregarAuditor = useCallback(async () => {
    setError(null);
    try {
      const data = await fetchApi<{ status: string; auditor: DadosAuditor }>("/relatorio/auditor");
      return data.auditor;
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    }
  }, []);

  const salvarAuditor = useCallback(async (auditor: Partial<DadosAuditor>) => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchApi<{ status: string; auditor: DadosAuditor }>(
        "/relatorio/auditor",
        { method: "PUT", body: JSON.stringify(auditor) },
      );
      return data.auditor;
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  // ---- DSF ----

  const listarDsfs = useCallback(async () => {
    try {
      const data = await fetchApi<{ status: string; dsfs: Dsf[] }>("/relatorio/dsf");
      return data.dsfs;
    } catch {
      return [];
    }
  }, []);

  const obterDsf = useCallback(async (numero: string) => {
    try {
      const data = await fetchApi<{ status: string; dsf: Dsf | null }>(`/relatorio/dsf/${numero}`);
      return data.dsf;
    } catch {
      return null;
    }
  }, []);

  const salvarDsf = useCallback(async (numero: string, descricao: string, cnpjs: string[]) => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchApi<{ status: string; dsf: Dsf }>(
        `/relatorio/dsf/${numero}`,
        { method: "PUT", body: JSON.stringify({ numero, descricao, cnpjs }) },
      );
      return data.dsf;
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  const excluirDsf = useCallback(async (numero: string) => {
    setLoading(true);
    setError(null);
    try {
      await fetchApi(`/relatorio/dsf/${numero}`, { method: "DELETE" });
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  // ---- Relatorio por CNPJ ----

  const carregarRelatorio = useCallback(async (cnpj: string) => {
    setError(null);
    try {
      const data = await fetchApi<{ status: string; dados: DadosRelatorio }>(`/relatorio/cnpj/${cnpj}`);
      return data.dados;
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    }
  }, []);

  const salvarRelatorio = useCallback(async (cnpj: string, dados: Partial<DadosRelatorio>) => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchApi<{ status: string; dados: DadosRelatorio }>(
        `/relatorio/cnpj/${cnpj}`,
        { method: "PUT", body: JSON.stringify(dados) },
      );
      return data.dados;
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  const listarDets = useCallback(async (cnpj: string) => {
    try {
      const data = await fetchApi<{ status: string; dets: DetInfo[] }>(`/relatorio/cnpj/${cnpj}/listar-dets`);
      return data.dets;
    } catch {
      return [];
    }
  }, []);

  const uploadDet = useCallback(async (cnpj: string, file: File) => {
    setLoading(true);
    setError(null);
    try {
      const formData = new FormData();
      formData.append("arquivo", file);

      const response = await fetch(`${API_BASE}/relatorio/cnpj/${cnpj}/upload-det`, {
        method: "POST",
        body: formData,
      });

      if (!response.ok) {
        const texto = await response.text();
        throw new Error(texto || `Erro ${response.status}`);
      }

      return await response.json();
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  // ---- Geracao de PDFs ----

  const gerarDocxIndividual = useCallback(async (cnpj: string) => {
    setLoading(true);
    setError(null);
    try {
      await baixarArquivo(`/relatorio/cnpj/${cnpj}/gerar-docx`, `Relatorio_${cnpj}.docx`);
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, [baixarArquivo]);

  const gerarPdfIndividual = useCallback(async (cnpj: string) => {
    setLoading(true);
    setError(null);
    try {
      await baixarArquivo(`/relatorio/cnpj/${cnpj}/gerar-pdf`, `Relatorio_${cnpj}.pdf`);
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, [baixarArquivo]);

  const gerarPdfGeral = useCallback(async (dsf?: string, cnpjs?: string[], incluirDets = true) => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${API_BASE}/relatorio/gerar-geral`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ dsf: dsf || null, cnpjs: cnpjs || [], incluir_dets: incluirDets }),
      });

      if (!response.ok) {
        throw new Error(await extrairMensagemErro(response));
      }

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;

      const cd = response.headers.get("content-disposition");
      const match = cd?.match(/filename="?([^";\n]+)"?/);
      anchor.download = match?.[1] || "Relatorio_Geral_Consolidado.pdf";

      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      window.URL.revokeObjectURL(url);
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    } finally {
      setLoading(false);
    }
  }, []);

  const listarCnpjsComRelatorio = useCallback(async () => {
    try {
      const data = await fetchApi<{ status: string; cnpjs: CnpjComRelatorio[] }>(
        "/relatorio/listar-cnpjs-com-relatorio",
      );
      return data.cnpjs;
    } catch {
      return [];
    }
  }, []);

  const carregarDiagnostico = useCallback(async () => {
    setError(null);
    try {
      const data = await fetchApi<{ status: string } & DiagnosticoRelatorios>("/relatorio/diagnostico");
      return data;
    } catch (erro) {
      setError((erro as Error).message);
      throw erro;
    }
  }, []);

  return {
    loading,
    error,
    carregarAuditor,
    salvarAuditor,
    listarDsfs,
    obterDsf,
    salvarDsf,
    excluirDsf,
    carregarRelatorio,
    salvarRelatorio,
    listarDets,
    uploadDet,
    gerarDocxIndividual,
    gerarPdfIndividual,
    gerarPdfGeral,
    listarCnpjsComRelatorio,
    carregarDiagnostico,
  };
}
