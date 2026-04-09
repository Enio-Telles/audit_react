import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { dossieApi } from "../../../api/client";
import type {
  DossieSectionSummary,
  DossieSyncResponse,
  DossieTabProps,
} from "../types";

type EstadoSincronizacaoSecao = {
  mensagem?: string;
  tipo?: "sucesso" | "erro";
};

function obter_rotulo_status(status: DossieSectionSummary["status"]): string {
  switch (status) {
    case "cached":
      return "Cache disponivel";
    case "loading":
      return "Sincronizando";
    case "fresh":
      return "Atualizado";
    case "error":
      return "Erro";
    default:
      return "Aguardando";
  }
}

function obter_classes_status(status: DossieSectionSummary["status"]): string {
  if (status === "cached" || status === "fresh") {
    return "border-green-800 bg-green-900/30 text-green-300";
  }

  if (status === "loading") {
    return "border-amber-700 bg-amber-900/30 text-amber-200";
  }

  if (status === "error") {
    return "border-rose-800 bg-rose-900/30 text-rose-300";
  }

  return "border-slate-600 text-slate-300";
}

function formatar_quantidade_linhas(rowCount?: number): string {
  if (rowCount === undefined || rowCount === null) {
    return "sem carga";
  }

  return `${rowCount} ${rowCount === 1 ? "linha" : "linhas"}`;
}

// ⚡ Bolt Optimization: Use cached Intl.DateTimeFormat instance instead of Date.prototype.toLocaleString()
const dateTimeFormatter = new Intl.DateTimeFormat("pt-BR", {
  day: "2-digit",
  month: "2-digit",
  year: "numeric",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

function formatar_data_atualizacao(updatedAt?: string | null): string | null {
  if (!updatedAt) {
    return null;
  }

  const data = new Date(updatedAt);
  if (Number.isNaN(data.getTime())) {
    return null;
  }

  return dateTimeFormatter.format(data);
}

function extrair_mensagem_erro(error: unknown): string {
  if (typeof error === "object" && error !== null && "response" in error) {
    const erroHttp = error as {
      response?: { data?: { detail?: string } };
      message?: string;
    };

    if (erroHttp.response?.data?.detail) {
      return erroHttp.response.data.detail;
    }

    if (erroHttp.message) {
      return erroHttp.message;
    }
  }

  if (error instanceof Error) {
    return error.message;
  }

  return "Falha ao sincronizar a secao.";
}

function montar_status_apresentado(
  secao: DossieSectionSummary,
  secaoEmSincronizacao: string | null,
  estadoLocal?: EstadoSincronizacaoSecao,
): DossieSectionSummary["status"] {
  if (secaoEmSincronizacao === secao.id) {
    return "loading";
  }

  if (estadoLocal?.tipo === "erro") {
    return "error";
  }

  if (estadoLocal?.tipo === "sucesso") {
    return "fresh";
  }

  return secao.status;
}

export function DossieTab({ cnpj, razaoSocial }: DossieTabProps) {
  const queryClient = useQueryClient();
  const [secaoEmSincronizacao, setSecaoEmSincronizacao] = useState<
    string | null
  >(null);
  const [estadoPorSecao, setEstadoPorSecao] = useState<
    Record<string, EstadoSincronizacaoSecao>
  >({});

  const {
    data: sections,
    isLoading,
    isError,
  } = useQuery<DossieSectionSummary[]>({
    queryKey: ["dossie_sections", cnpj],
    queryFn: () => dossieApi.getSecoes(cnpj!),
    enabled: !!cnpj,
  });

  const mutacaoSincronizacao = useMutation({
    mutationFn: async (
      secao: DossieSectionSummary,
    ): Promise<DossieSyncResponse> => {
      return dossieApi.syncSecao(cnpj!, secao.id);
    },
    onMutate: (secao) => {
      setSecaoEmSincronizacao(secao.id);
      setEstadoPorSecao((estadoAnterior) => ({
        ...estadoAnterior,
        [secao.id]: {},
      }));
    },
    onSuccess: async (resultado, secao) => {
      setEstadoPorSecao((estadoAnterior) => ({
        ...estadoAnterior,
        [secao.id]: {
          tipo: "sucesso",
          mensagem: `${resultado.linhas_extraidas} ${resultado.linhas_extraidas === 1 ? "linha atualizada" : "linhas atualizadas"}`,
        },
      }));

      await queryClient.invalidateQueries({
        queryKey: ["dossie_sections", cnpj],
      });
    },
    onError: (error, secao) => {
      setEstadoPorSecao((estadoAnterior) => ({
        ...estadoAnterior,
        [secao.id]: {
          tipo: "erro",
          mensagem: extrair_mensagem_erro(error),
        },
      }));
    },
    onSettled: () => {
      setSecaoEmSincronizacao(null);
    },
  });

  if (!cnpj) {
    return (
      <div className="flex h-full w-full items-center justify-center p-6 text-slate-300">
        <div className="max-w-xl rounded-2xl border border-slate-700 bg-slate-900/60 p-6">
          <h2 className="mb-2 text-lg font-semibold text-white">
            Dossie indisponivel
          </h2>
          <p className="text-sm text-slate-400">
            Selecione um CNPJ para abrir o dossie, reaproveitar extracoes ja
            existentes e evitar duplicacao de dados.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full overflow-auto p-4 text-slate-200">
      <div className="mb-4 rounded-2xl border border-slate-700 bg-slate-900/60 p-4">
        <div className="mb-1 text-xs uppercase tracking-wide text-slate-400">
          Dossie principal
        </div>
        <h2 className="text-lg font-semibold text-white">{cnpj}</h2>
        {razaoSocial && (
          <div className="mt-1 text-sm text-slate-400">{razaoSocial}</div>
        )}
        <p className="mt-3 text-sm text-slate-400">
          Esta area concentra a navegacao do dossie por CNPJ, priorizando reuso
          de consultas SQL, persistencia por secao e leitura amigavel dos dados.
        </p>
      </div>

      {isLoading && (
        <div className="text-sm text-slate-400">
          Carregando secoes do dossie...
        </div>
      )}
      {isError && (
        <div className="text-sm text-red-400">
          Erro ao carregar as secoes do dossie.
        </div>
      )}

      {!isLoading && !isError && sections && (
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          {sections.map((section) => {
            const dataAtualizacao = formatar_data_atualizacao(
              section.updatedAt,
            );
            const estadoLocal = estadoPorSecao[section.id];
            const statusApresentado = montar_status_apresentado(
              section,
              secaoEmSincronizacao,
              estadoLocal,
            );
            const estaSincronizando = secaoEmSincronizacao === section.id;

            return (
              <div
                key={section.id}
                className="rounded-2xl border border-slate-700 border-t-2 bg-slate-900/50 p-4 transition-colors hover:border-t-blue-500 hover:bg-slate-800/80"
              >
                <div className="mb-2 flex items-center justify-between gap-2">
                  <h3 className="text-sm font-semibold text-white">
                    {section.title}
                  </h3>
                  <span
                    className={`rounded-full border px-2 py-0.5 text-[10px] ${obter_classes_status(statusApresentado)}`}
                  >
                    {obter_rotulo_status(statusApresentado)}
                  </span>
                </div>
                <p className="mb-3 text-sm text-slate-400">
                  {section.description}
                </p>
                <div className="flex items-center justify-between text-xs text-slate-500">
                  <span>Fonte: {section.sourceType}</span>
                  <span>{formatar_quantidade_linhas(section.rowCount)}</span>
                </div>
                {dataAtualizacao && (
                  <div className="mt-2 text-[11px] text-slate-500">
                    Atualizado em {dataAtualizacao}
                  </div>
                )}

                <div className="mt-4 flex items-center justify-between gap-3">
                  <button
                    type="button"
                    onClick={() => mutacaoSincronizacao.mutate(section)}
                    disabled={
                      estaSincronizando || mutacaoSincronizacao.isPending
                    }
                    className="rounded-lg border border-blue-700 bg-blue-900/30 px-3 py-1.5 text-xs font-medium text-blue-200 transition-colors hover:bg-blue-800/50 disabled:cursor-not-allowed disabled:border-slate-700 disabled:bg-slate-800 disabled:text-slate-500"
                  >
                    {estaSincronizando ? "Sincronizando..." : "Sincronizar"}
                  </button>
                  <div className="min-h-[20px] flex-1 text-right text-[11px]">
                    {estadoLocal?.mensagem && (
                      <span
                        className={
                          estadoLocal.tipo === "erro"
                            ? "text-rose-300"
                            : "text-emerald-300"
                        }
                      >
                        {estadoLocal.mensagem}
                      </span>
                    )}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

export default DossieTab;
