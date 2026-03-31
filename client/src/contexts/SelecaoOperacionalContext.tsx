import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import type { ResultadoConsultaCadastral, SelecaoOperacional } from "@/types/audit";

interface SelecaoOperacionalContextValue {
  selecaoOperacional: SelecaoOperacional | null;
  definirSelecaoOperacional: (selecao: SelecaoOperacional | null) => void;
  atualizarResultadoCadastral: (documento: string, resultado: ResultadoConsultaCadastral) => void;
  limparSelecaoOperacional: () => void;
}

const CHAVE_STORAGE = "audit_react_selecao_operacional";

const SelecaoOperacionalContext = createContext<SelecaoOperacionalContextValue | undefined>(undefined);

function normalizarListaDocumentos(documentos: string[]): string[] {
  return Array.from(
    new Set(
      documentos
        .map((item) => String(item || "").replace(/\D/g, ""))
        .filter(Boolean),
    ),
  );
}

function normalizarSelecao(selecao: SelecaoOperacional): SelecaoOperacional {
  return {
    ...selecao,
    documento_principal: String(selecao.documento_principal || "").replace(/\D/g, ""),
    documentos_origem: normalizarListaDocumentos(selecao.documentos_origem),
    cnpjs_resolvidos: normalizarListaDocumentos(selecao.cnpjs_resolvidos).filter((item) => item.length === 14),
  };
}

export function SelecaoOperacionalProvider({ children }: { children: ReactNode }) {
  const [selecaoOperacional, setSelecaoOperacional] = useState<SelecaoOperacional | null>(null);

  useEffect(() => {
    const valorSalvo = sessionStorage.getItem(CHAVE_STORAGE);
    if (!valorSalvo) return;

    try {
      const selecao = JSON.parse(valorSalvo) as SelecaoOperacional;
      setSelecaoOperacional(normalizarSelecao(selecao));
    } catch {
      sessionStorage.removeItem(CHAVE_STORAGE);
    }
  }, []);

  const definirSelecaoOperacional = (selecao: SelecaoOperacional | null) => {
    if (!selecao) {
      setSelecaoOperacional(null);
      sessionStorage.removeItem(CHAVE_STORAGE);
      return;
    }

    const selecaoNormalizada = normalizarSelecao(selecao);
    setSelecaoOperacional(selecaoNormalizada);
    sessionStorage.setItem(CHAVE_STORAGE, JSON.stringify(selecaoNormalizada));
  };

  const atualizarResultadoCadastral = (documento: string, resultado: ResultadoConsultaCadastral) => {
    const documentoNormalizado = String(documento || "").replace(/\D/g, "");
    if (!documentoNormalizado) return;

    setSelecaoOperacional((atual) => {
      if (!atual) return atual;

      const resultadosAtualizados = atual.resultados_cadastrais.some(
        (item) => item.documento_consultado === documentoNormalizado,
      )
        ? atual.resultados_cadastrais.map((item) =>
            item.documento_consultado === documentoNormalizado ? resultado : item,
          )
        : [...atual.resultados_cadastrais, resultado];

      const proximaSelecao = normalizarSelecao({
        ...atual,
        resultados_cadastrais: resultadosAtualizados,
      });

      sessionStorage.setItem(CHAVE_STORAGE, JSON.stringify(proximaSelecao));
      return proximaSelecao;
    });
  };

  const limparSelecaoOperacional = () => {
    setSelecaoOperacional(null);
    sessionStorage.removeItem(CHAVE_STORAGE);
  };

  const valor = useMemo(
    () => ({
      selecaoOperacional,
      definirSelecaoOperacional,
      atualizarResultadoCadastral,
      limparSelecaoOperacional,
    }),
    [selecaoOperacional],
  );

  return <SelecaoOperacionalContext.Provider value={valor}>{children}</SelecaoOperacionalContext.Provider>;
}

export function useSelecaoOperacional() {
  const contexto = useContext(SelecaoOperacionalContext);
  if (!contexto) {
    throw new Error("useSelecaoOperacional deve ser usado dentro de SelecaoOperacionalProvider");
  }
  return contexto;
}
