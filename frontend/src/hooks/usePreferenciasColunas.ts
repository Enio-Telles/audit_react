import { useCallback, useEffect, useMemo, useState } from "react";

const LARGURA_PADRAO_COLUNA = 120;

interface PreferenciasColunasSalvas {
  ordemColunas: string[];
  largurasColunas: Record<string, number>;
  colunasOcultas: string[];
  perfilAtivo: string;
}

interface PerfilSalvo {
  ordemColunas: string[];
  largurasColunas: Record<string, number>;
  colunasOcultas: string[];
}

/** Colunas que perfis do sistema ocultam por padrão */
const COLUNAS_TECNICAS = [
  "origem_dado",
  "tabela_origem",
  "sql_id_origem",
  "ordem_exibicao",
];

const PERFIS_SISTEMA: Record<string, { nome: string; colunasOcultas: (colunas: string[]) => string[] }> = {
  resumido: {
    nome: "Visão Resumida",
    colunasOcultas: (colunas) =>
      colunas.filter(
        (c) =>
          COLUNAS_TECNICAS.includes(c.toLowerCase()) ||
          [
            "cnpj_raiz",
            "cnpj_consultado",
            "indicador_matriz_filial",
            "fontes_contato",
            "telefones_por_fonte",
            "emails_por_fonte",
          ].includes(c.toLowerCase()),
      ),
  },
  auditoria: {
    nome: "Visão Auditoria",
    colunasOcultas: () => [],
  },
};

interface RetornoPreferenciasColunas {
  ordemColunas: string[];
  largurasColunas: Record<string, number>;
  colunasOcultas: Set<string>;
  perfilAtivo: string;
  definirOrdemColunas: (ordemColunas: string[]) => void;
  definirLarguraColuna: (coluna: string, largura: number) => void;
  definirColunasOcultas: (cols: string[]) => void;
  toggleColunaVisibilidade: (col: string) => void;
  mostrarTodasColunas: () => void;
  ocultarTodasColunas: () => void;
  carregarPerfil: (nome: string) => void;
  listarPerfis: () => string[];
  redefinirPreferenciasColunas: () => void;
}

function carregarPreferenciasColunas(
  chavePersistencia: string,
): PreferenciasColunasSalvas {
  if (typeof window === "undefined") {
    return { ordemColunas: [], largurasColunas: {}, colunasOcultas: [], perfilAtivo: "resumido" };
  }

  try {
    const bruto = window.localStorage.getItem(chavePersistencia);
    if (!bruto) return { ordemColunas: [], largurasColunas: {}, colunasOcultas: [], perfilAtivo: "resumido" };

    const preferencias = JSON.parse(bruto) as Partial<PreferenciasColunasSalvas>;
    return {
      ordemColunas: Array.isArray(preferencias.ordemColunas)
        ? preferencias.ordemColunas.filter(
            (coluna): coluna is string => typeof coluna === "string",
          )
        : [],
      largurasColunas:
        preferencias.largurasColunas &&
        typeof preferencias.largurasColunas === "object"
          ? Object.fromEntries(
              Object.entries(preferencias.largurasColunas).filter(
                ([, largura]) =>
                  typeof largura === "number" && Number.isFinite(largura),
              ),
            )
          : {},
      colunasOcultas: Array.isArray(preferencias.colunasOcultas)
        ? preferencias.colunasOcultas.filter(
            (c): c is string => typeof c === "string",
          )
        : [],
      perfilAtivo:
        typeof preferencias.perfilAtivo === "string"
          ? preferencias.perfilAtivo
          : "resumido",
    };
  } catch {
    return { ordemColunas: [], largurasColunas: {}, colunasOcultas: [], perfilAtivo: "resumido" };
  }
}

function normalizarOrdemColunas(
  colunasDisponiveis: string[],
  ordemSalva: string[],
): string[] {
  if (!ordemSalva.length) return [...colunasDisponiveis];

  return [
    ...ordemSalva.filter((coluna) => colunasDisponiveis.includes(coluna)),
    ...colunasDisponiveis.filter((coluna) => !ordemSalva.includes(coluna)),
  ];
}

function normalizarLargurasColunas(
  colunasDisponiveis: string[],
  largurasSalvas: Record<string, number>,
): Record<string, number> {
  return Object.fromEntries(
    colunasDisponiveis.map((coluna) => [
      coluna,
      Math.max(80, largurasSalvas[coluna] ?? LARGURA_PADRAO_COLUNA),
    ]),
  );
}

export function usePreferenciasColunas(
  chavePersistencia: string,
  colunasDisponiveis: string[],
  largurasIniciais: Record<string, number> = {},
): RetornoPreferenciasColunas {
  const [preferenciasSalvas, setPreferenciasSalvas] =
    useState<PreferenciasColunasSalvas>(() =>
      carregarPreferenciasColunas(chavePersistencia),
    );

  const ordemColunas = useMemo(
    () =>
      normalizarOrdemColunas(
        colunasDisponiveis,
        preferenciasSalvas.ordemColunas,
      ),
    [colunasDisponiveis, preferenciasSalvas.ordemColunas],
  );

  const largurasColunas = useMemo(
    () =>
      normalizarLargurasColunas(
        colunasDisponiveis,
        { ...largurasIniciais, ...preferenciasSalvas.largurasColunas },
      ),
    [colunasDisponiveis, largurasIniciais, preferenciasSalvas.largurasColunas],
  );

  const colunasOcultas = useMemo(
    () => new Set(preferenciasSalvas.colunasOcultas.filter((c) => colunasDisponiveis.includes(c))),
    [preferenciasSalvas.colunasOcultas, colunasDisponiveis],
  );

  const perfilAtivo = preferenciasSalvas.perfilAtivo;

  useEffect(() => {
    if (typeof window === "undefined") return;

    const preferenciasAtualizadas: PreferenciasColunasSalvas = {
      ordemColunas,
      largurasColunas,
      colunasOcultas: [...colunasOcultas],
      perfilAtivo,
    };

    window.localStorage.setItem(
      chavePersistencia,
      JSON.stringify(preferenciasAtualizadas),
    );
  }, [chavePersistencia, largurasColunas, ordemColunas, colunasOcultas, perfilAtivo]);

  function definirOrdemColunas(ordemAtualizada: string[]) {
    setPreferenciasSalvas((anterior) => ({
      ...anterior,
      ordemColunas: ordemAtualizada,
      perfilAtivo: "personalizado",
    }));
  }

  function definirLarguraColuna(coluna: string, largura: number) {
    setPreferenciasSalvas((anterior) => ({
      ...anterior,
      largurasColunas: {
        ...anterior.largurasColunas,
        [coluna]: Math.max(80, Math.min(600, largura)),
      },
    }));
  }

  function definirColunasOcultas(cols: string[]) {
    setPreferenciasSalvas((anterior) => ({
      ...anterior,
      colunasOcultas: cols,
      perfilAtivo: "personalizado",
    }));
  }

  const toggleColunaVisibilidade = useCallback(
    (col: string) => {
      setPreferenciasSalvas((anterior) => {
        const ocultas = new Set(anterior.colunasOcultas);
        if (ocultas.has(col)) {
          ocultas.delete(col);
        } else {
          ocultas.add(col);
        }
        return {
          ...anterior,
          colunasOcultas: [...ocultas],
          perfilAtivo: "personalizado",
        };
      });
    },
    [],
  );

  function mostrarTodasColunas() {
    setPreferenciasSalvas((anterior) => ({
      ...anterior,
      colunasOcultas: [],
      perfilAtivo: "personalizado",
    }));
  }

  function ocultarTodasColunas() {
    setPreferenciasSalvas((anterior) => ({
      ...anterior,
      colunasOcultas: [...colunasDisponiveis],
      perfilAtivo: "personalizado",
    }));
  }

  function carregarPerfil(nome: string) {
    const perfilSistema = PERFIS_SISTEMA[nome];
    if (perfilSistema) {
      setPreferenciasSalvas((anterior) => ({
        ...anterior,
        colunasOcultas: perfilSistema.colunasOcultas(colunasDisponiveis),
        perfilAtivo: nome,
      }));
      return;
    }

    // Try loading custom profile from localStorage
    try {
      const bruto = window.localStorage.getItem(`${chavePersistencia}__perfil__${nome}`);
      if (bruto) {
        const perfil = JSON.parse(bruto) as PerfilSalvo;
        setPreferenciasSalvas({
          ordemColunas: perfil.ordemColunas ?? [],
          largurasColunas: perfil.largurasColunas ?? {},
          colunasOcultas: perfil.colunasOcultas ?? [],
          perfilAtivo: nome,
        });
      }
    } catch {
      // ignore
    }
  }

  function listarPerfis(): string[] {
    const perfis = Object.keys(PERFIS_SISTEMA);
    // Enumerate custom profiles from localStorage
    if (typeof window !== "undefined") {
      const prefixo = `${chavePersistencia}__perfil__`;
      for (let i = 0; i < window.localStorage.length; i++) {
        const chave = window.localStorage.key(i);
        if (chave?.startsWith(prefixo)) {
          perfis.push(chave.slice(prefixo.length));
        }
      }
    }
    return perfis;
  }

  function redefinirPreferenciasColunas() {
    setPreferenciasSalvas({
      ordemColunas: [...colunasDisponiveis],
      largurasColunas: normalizarLargurasColunas(
        colunasDisponiveis,
        largurasIniciais,
      ),
      colunasOcultas: [],
      perfilAtivo: "resumido",
    });
  }

  return {
    ordemColunas,
    largurasColunas,
    colunasOcultas,
    perfilAtivo,
    definirOrdemColunas,
    definirLarguraColuna,
    definirColunasOcultas,
    toggleColunaVisibilidade,
    mostrarTodasColunas,
    ocultarTodasColunas,
    carregarPerfil,
    listarPerfis,
    redefinirPreferenciasColunas,
  };
}
