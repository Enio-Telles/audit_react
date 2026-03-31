import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from "react";

interface CnpjContextValue {
  cnpjAtivo: string;
  definirCnpjAtivo: (cnpj: string) => void;
}

const CHAVE_STORAGE = "audit_react_cnpj_ativo";

const CnpjContext = createContext<CnpjContextValue | undefined>(undefined);

function normalizarCnpj(valor: string): string {
  return valor.replace(/\D/g, "").slice(0, 14);
}

export function CnpjProvider({ children }: { children: ReactNode }) {
  const [cnpjAtivo, setCnpjAtivo] = useState<string>("");

  useEffect(() => {
    const salvo = localStorage.getItem(CHAVE_STORAGE);
    if (salvo) {
      setCnpjAtivo(normalizarCnpj(salvo));
    }
  }, []);

  const definirCnpjAtivo = (cnpj: string) => {
    const cnpjNormalizado = normalizarCnpj(cnpj);
    setCnpjAtivo(cnpjNormalizado);
    if (cnpjNormalizado) {
      localStorage.setItem(CHAVE_STORAGE, cnpjNormalizado);
    } else {
      localStorage.removeItem(CHAVE_STORAGE);
    }
  };

  const valor = useMemo(
    () => ({
      cnpjAtivo,
      definirCnpjAtivo,
    }),
    [cnpjAtivo],
  );

  return <CnpjContext.Provider value={valor}>{children}</CnpjContext.Provider>;
}

export function useCnpj() {
  const contexto = useContext(CnpjContext);
  if (!contexto) {
    throw new Error("useCnpj deve ser usado dentro de CnpjProvider");
  }
  return contexto;
}

export function formatarCnpj(cnpj: string): string {
  const digits = normalizarCnpj(cnpj);
  if (digits.length <= 2) return digits;
  if (digits.length <= 5) return `${digits.slice(0, 2)}.${digits.slice(2)}`;
  if (digits.length <= 8) return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5)}`;
  if (digits.length <= 12) return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8)}`;
  return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8, 12)}-${digits.slice(12)}`;
}
