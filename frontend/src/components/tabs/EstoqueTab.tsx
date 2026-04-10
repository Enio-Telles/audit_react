import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { estoqueApi } from "../../api/client";

// ⚡ Bolt Optimization: Use cached Intl.NumberFormat instance instead of Number.prototype.toLocaleString()
// This avoids repeatedly allocating locale data and parsing options on every render, improving performance.
const intlInteger = new Intl.NumberFormat("pt-BR");
const intlCurrency = new Intl.NumberFormat("pt-BR", {
  style: "currency",
  currency: "BRL",
});
const intlDateTime = new Intl.DateTimeFormat("pt-BR", {
  dateStyle: "short",
  timeStyle: "medium",
});
import { useAppStore } from "../../store/appStore";
import { DataTable } from "../table/DataTable";
import { ColumnToggle } from "../table/ColumnToggle";
import { usePreferenciasColunas } from "../../hooks/usePreferenciasColunas";

type SubTab =
  | "mov_estoque"
  | "tabela_mensal"
  | "tabela_anual"
  | "resumo"
  | "produtos"
  | "id_agrupados"
  | "bloco_h";

type BlocoHSubTab = "h005_resumo" | "todos_dados";
type TableSortState = { col: string; desc: boolean } | null;

function escapeHtml(value: unknown): string {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function getVisibleOrderedColumns(
  allColumns: string[],
  orderedColumns: string[],
  hiddenColumns: Set<string>,
): string[] {
  const fallback = allColumns.filter((c) => !hiddenColumns.has(c));
  if (!orderedColumns.length) return fallback;

  const orderedVisible = orderedColumns.filter(
    (c) => allColumns.includes(c) && !hiddenColumns.has(c),
  );
  const remaining = fallback.filter((c) => !orderedVisible.includes(c));
  return [...orderedVisible, ...remaining];
}

function exportTableAsExcel(
  rows: Array<Record<string, unknown>>,
  columns: string[],
  fileName: string,
) {
  const headerHtml = columns.map((c) => `<th>${escapeHtml(c)}</th>`).join("");
  const rowsHtml = rows
    .map((row) => {
      const tds = columns
        .map((col) => `<td>${escapeHtml(row[col])}</td>`)
        .join("");
      return `<tr>${tds}</tr>`;
    })
    .join("");

  const html = `\n<html><head><meta charset="utf-8" /></head><body><table border="1"><thead><tr>${headerHtml}</tr></thead><tbody>${rowsHtml}</tbody></table></body></html>`;

  const blob = new Blob([html], {
    type: "application/vnd.ms-excel;charset=utf-8;",
  });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `${fileName}.xls`;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function openTableInNewTab(
  title: string,
  rows: Array<Record<string, unknown>>,
  columns: string[],
  appearanceKey?: string,
) {
  let appearance = {
    headerBg: "#1b2943",
    headerText: "#dbe7ff",
    filterBg: "#131d31",
    rowEvenBg: "#101927",
    rowOddBg: "#0b1422",
    rowText: "#f8fafc",
  };

  if (appearanceKey) {
    try {
      const raw = window.localStorage.getItem(
        `datatable_appearance_${appearanceKey}`,
      );
      if (raw) {
        appearance = {
          ...appearance,
          ...(JSON.parse(raw) as Partial<typeof appearance>),
        };
      }
    } catch {
      // fallback silencioso para o tema padrao
    }
  }

  const headerHtml = columns.map((c) => `<th>${escapeHtml(c)}</th>`).join("");
  const rowsHtml = rows
    .map((row, index) => {
      const tds = columns
        .map((col) => `<td>${escapeHtml(row[col])}</td>`)
        .join("");
      const bg = index % 2 === 0 ? appearance.rowEvenBg : appearance.rowOddBg;
      return `<tr style="background:${bg};color:${appearance.rowText}">${tds}</tr>`;
    })
    .join("");

  const html = `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>${escapeHtml(title)}</title>
    <style>
      :root {
        color-scheme: dark;
      }
      * { box-sizing: border-box; }
      body {
        font-family: Inter, "Segoe UI", sans-serif;
        margin: 0;
        padding: 20px;
        background: #0b1220;
        color: ${appearance.rowText};
      }
      .shell {
        border-radius: 18px;
        overflow: hidden;
        background: #0f172a;
        box-shadow: 0 24px 80px rgba(0, 0, 0, 0.45);
      }
      .hero {
        padding: 18px 20px;
        background: linear-gradient(135deg, #0f2745 0%, #162c52 100%);
        border-bottom: 1px solid rgba(148, 163, 184, 0.18);
      }
      h1 { margin: 0 0 6px 0; font-size: 18px; color: ${appearance.headerText}; }
      .meta { color: #9fb3d9; font-size: 12px; }
      .table-wrap {
        overflow: auto;
        max-height: calc(100vh - 130px);
      }
      table { border-collapse: collapse; width: 100%; font-size: 12px; }
      th, td {
        border-right: 1px solid rgba(148, 163, 184, 0.16);
        border-bottom: 1px solid rgba(148, 163, 184, 0.1);
        padding: 7px 10px;
        text-align: left;
        vertical-align: top;
      }
      th {
        background: ${appearance.headerBg};
        color: ${appearance.headerText};
        position: sticky;
        top: 0;
        z-index: 1;
      }
      td {
        color: ${appearance.rowText};
      }
    </style>
  </head>
  <body>
    <div class="shell">
      <div class="hero">
        <h1>${escapeHtml(title)}</h1>
        <div class="meta">Gerado em ${escapeHtml(intlDateTime.format(new Date()))} | Registros: ${escapeHtml(rows.length)}</div>
      </div>
      <div class="table-wrap">
        <table>
          <thead><tr>${headerHtml}</tr></thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      </div>
    </div>
  </body>
</html>`;

  const blob = new Blob([html], { type: "text/html;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const openedWindow = window.open(url, "_blank");

  if (!openedWindow) {
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.target = "_blank";
    anchor.rel = "noopener noreferrer";
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
  }

  window.setTimeout(() => URL.revokeObjectURL(url), 60_000);
}

// Sub-tabs that show the produtos_final table and support full selection
const PRODUTOS_SUBS = new Set<SubTab>(["produtos", "id_agrupados"]);

function EstoqueSubTab({ cnpj, sub }: { cnpj: string; sub: SubTab }) {
  const [page, setPage] = useState(1);
  const [sort, setSort] = useState<TableSortState>(null);
  const [search, setSearch] = useState("");
  const [columnFilters, setColumnFilters] = useState<Record<string, string>>({});
  const [filterIdAgrupado, setFilterIdAgrupado] = useState("");
  const [filterBlocoHDataInv, setFilterBlocoHDataInv] = useState("");
  const [filterBlocoHMotivo, setFilterBlocoHMotivo] = useState("");
  const [filterBlocoHPropriedade, setFilterBlocoHPropriedade] = useState("");
  const [blocoHSubTab, setBlocoHSubTab] = useState<BlocoHSubTab>("h005_resumo");
  const [selectedRowKeys, setSelectedRowKeys] = useState<Set<string>>(
    new Set(),
  );
  const [hiddenCols, setHiddenCols] = useState<Set<string>>(new Set());

  const isProdutosSub = PRODUTOS_SUBS.has(sub);
  const params = {
    ...(sort ? { sort_by: sort.col, sort_desc: sort.desc } : {}),
    ...(search ? { search } : {}),
    ...(filterIdAgrupado ? { id_agrupado: filterIdAgrupado } : {}),
    ...(Object.keys(columnFilters).length
      ? { column_filters: JSON.stringify(columnFilters) }
      : {}),
  };

  // Para sub-tabs de produtos carrega todos de uma vez (sem paginação) para popular dropdown
  const queryFn = isProdutosSub
    ? () => estoqueApi.idAgrupados(cnpj, 1, 10000, params)
    : {
        mov_estoque: () => estoqueApi.movEstoque(cnpj, page, 500, params),
        tabela_mensal: () => estoqueApi.tabelaMensal(cnpj, page, 500, params),
        tabela_anual: () => estoqueApi.tabelaAnual(cnpj, page, 500, params),
        resumo: () => estoqueApi.tabelaAnual(cnpj, page, 500, params),
        produtos: () => estoqueApi.idAgrupados(cnpj, page, 500, params),
        id_agrupados: () => estoqueApi.idAgrupados(cnpj, page, 500, params),
        bloco_h: () => {
          const filtros = {
            dt_inv: filterBlocoHDataInv || undefined,
            cod_mot_inv: filterBlocoHMotivo || undefined,
            indicador_propriedade: filterBlocoHPropriedade || undefined,
          };
          return blocoHSubTab === "h005_resumo"
            ? estoqueApi.blocoHH005(cnpj, page, 500, filtros, params)
            : estoqueApi.blocoH(cnpj, page, 500, filtros, params);
        },
      }[sub];

  const { data: blocoHResumo } = useQuery({
    queryKey: [
      "bloco_h_resumo",
      cnpj,
      filterBlocoHDataInv,
      filterBlocoHMotivo,
      filterBlocoHPropriedade,
    ],
    queryFn: () =>
      estoqueApi.blocoHResumo(cnpj, {
        dt_inv: filterBlocoHDataInv || undefined,
        cod_mot_inv: filterBlocoHMotivo || undefined,
        indicador_propriedade: filterBlocoHPropriedade || undefined,
      }),
    enabled: sub === "bloco_h",
  });

  const { data, isLoading, refetch } = useQuery({
    queryKey: isProdutosSub
      ? [
          sub,
          cnpj,
          "all",
          sort?.col ?? null,
          sort?.desc ?? false,
          search,
          filterIdAgrupado,
          JSON.stringify(columnFilters),
        ]
      : [
          sub,
          cnpj,
          page,
          sort?.col ?? null,
          sort?.desc ?? false,
          search,
          filterIdAgrupado,
          JSON.stringify(columnFilters),
          blocoHSubTab,
          filterBlocoHDataInv,
          filterBlocoHMotivo,
          filterBlocoHPropriedade,
        ],
    queryFn,
    placeholderData: (prev) => prev,
  });

  const inputCls =
    "bg-slate-800 border border-slate-600 rounded px-2 py-1 text-xs text-slate-200 focus:outline-none focus:border-blue-500";
  const btnCls =
    "px-3 py-1.5 rounded text-xs font-medium cursor-pointer transition-colors";
  const colunasDisponiveis = useMemo(
    () => data?.columns ?? [],
    [data?.columns],
  );
  const {
    ordemColunas,
    largurasColunas,
    definirOrdemColunas,
    definirLarguraColuna,
    redefinirPreferenciasColunas,
  } = usePreferenciasColunas(`estoque_colunas_${sub}_v1`, colunasDisponiveis);

  // Unique id_agrupado values for the dropdown (only relevant for produtos sub-tabs)
  const uniqueIdAgrupados = useMemo(() => {
    if (!isProdutosSub) return [];
    const seen = new Set<string>();
    for (const r of data?.rows ?? []) {
      const v = String(r["id_agrupado"] ?? "");
      if (v) seen.add(v);
    }
    return [...seen].sort();
  }, [data?.rows, isProdutosSub]);

  const filteredRows = useMemo(() => {
    // ⚡ Bolt Optimization: Hoist string casing outside filter loop to prevent O(N) performance degradation
    const searchLower = search ? search.toLowerCase() : "";
    return (data?.rows ?? []).filter((r) => {
      if (
        filterIdAgrupado &&
        String(r["id_agrupado"] ?? "") !== filterIdAgrupado
      )
        return false;
      if (searchLower) {
        let hit = false;
        // ⚡ Bolt Optimization: Replace Object.values().some() with for...in loop and early break to prevent O(N*C) object allocations
        for (const key in r) {
          if (
            String(r[key] ?? "")
              .toLowerCase()
              .includes(searchLower)
          ) {
            hit = true;
            break;
          }
        }
        if (!hit) return false;
      }
      return true;
    });
  }, [data?.rows, filterIdAgrupado, search]);

  const hasFilters = !!(
    filterIdAgrupado ||
    search ||
    Object.values(columnFilters).some(Boolean) ||
    filterBlocoHDataInv ||
    filterBlocoHMotivo ||
    filterBlocoHPropriedade
  );

  function clearFilters() {
    setFilterIdAgrupado("");
    setSearch("");
    setColumnFilters({});
    setFilterBlocoHDataInv("");
    setFilterBlocoHMotivo("");
    setFilterBlocoHPropriedade("");
    setPage(1);
  }

  function handleRowSelect(key: string, checked: boolean) {
    setSelectedRowKeys((prev) => {
      const next = new Set(prev);
      if (checked) next.add(key);
      else next.delete(key);
      return next;
    });
  }

  function handleSelectAll(checked: boolean, visibleKeys: string[]) {
    setSelectedRowKeys((prev) => {
      const next = new Set(prev);
      if (checked) visibleKeys.forEach((k) => next.add(k));
      else visibleKeys.forEach((k) => next.delete(k));
      return next;
    });
  }

  const tableTitle = {
    mov_estoque: "Movimentacao de Estoque (H010/C170/C176)",
    tabela_mensal: "Apuracao Mensal de Estoque",
    tabela_anual: "Apuracao Anual de Estoque",
    resumo: "Resumo Fiscal Global",
    produtos: "Produtos Selecionados (Consolidacao)",
    id_agrupados: "Cadastros Agrupados (Mestre)",
    bloco_h:
      blocoHSubTab === "h005_resumo"
        ? "Inventarios H005 (Resumo)"
        : "Detalhamento H010/H020 (Bloco H)",
  }[sub];

  const visibleColumns = useMemo(
    () =>
      getVisibleOrderedColumns(colunasDisponiveis, ordemColunas, hiddenCols),
    [colunasDisponiveis, ordemColunas, hiddenCols],
  );

  const exportBaseName =
    sub === "bloco_h"
      ? blocoHSubTab === "h005_resumo"
        ? "bloco_h_h005_resumo"
        : "bloco_h_h010_h020"
      : sub;

  function handleExportExcel() {
    const fileName = `estoque_${cnpj}_${exportBaseName}_${new Date().toISOString().slice(0, 10)}`;
    exportTableAsExcel(filteredRows, visibleColumns, fileName);
  }

  function handleOpenInNewTab() {
    const title = `${tableTitle} | CNPJ ${cnpj}`;
    openTableInNewTab(
      title,
      filteredRows,
      visibleColumns,
      `estoque_${sub}_${blocoHSubTab}`,
    );
  }

  return (
    <div className="flex flex-col h-full">
      <div className="px-3 py-2 border-b border-slate-700">
        <div className="text-xs font-semibold text-slate-300 mb-2">
          {tableTitle}
        </div>

        {sub === "bloco_h" && (
          <div className="flex gap-1 mb-2">
            <button
              className={`px-3 py-1 rounded text-xs border ${
                blocoHSubTab === "h005_resumo"
                  ? "bg-slate-700 border-slate-500 text-white"
                  : "bg-slate-900 border-slate-700 text-slate-300"
              }`}
              onClick={() => {
                setBlocoHSubTab("h005_resumo");
                setPage(1);
              }}
            >
              H005 Resumo
            </button>
            <button
              className={`px-3 py-1 rounded text-xs border ${
                blocoHSubTab === "todos_dados"
                  ? "bg-slate-700 border-slate-500 text-white"
                  : "bg-slate-900 border-slate-700 text-slate-300"
              }`}
              onClick={() => {
                setBlocoHSubTab("todos_dados");
                setPage(1);
              }}
            >
              Detalhamento H010/H020
            </button>
          </div>
        )}

        <div className="flex gap-2 items-center flex-wrap">
          <button
            className={
              btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-200"
            }
            onClick={() => refetch()}
            disabled={isLoading}
          >
            Recarregar
          </button>
          {hasFilters && (
            <button
              className={
                btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-400"
              }
              onClick={clearFilters}
            >
              Limpar filtros
            </button>
          )}
          {isProdutosSub && selectedRowKeys.size > 0 && (
            <span className="text-xs text-blue-400 font-medium">
              {selectedRowKeys.size} produto(s) selecionado(s)
            </span>
          )}
          {isProdutosSub && selectedRowKeys.size > 0 && (
            <button
              className={
                btnCls +
                " bg-slate-700 hover:bg-slate-600 text-slate-400 text-xs"
              }
              onClick={() => setSelectedRowKeys(new Set())}
            >
              Limpar seleção
            </button>
          )}
          <div className="flex-1" />
          <ColumnToggle
            allColumns={colunasDisponiveis}
            orderedColumns={ordemColunas}
            hiddenColumns={hiddenCols}
            columnWidths={largurasColunas}
            onChange={(col, visible) =>
              setHiddenCols((prev) => {
                const next = new Set(prev);
                if (visible) next.delete(col);
                else next.add(col);
                return next;
              })
            }
            onOrderChange={definirOrdemColunas}
            onWidthChange={definirLarguraColuna}
            onReset={() => {
              setHiddenCols(new Set());
              redefinirPreferenciasColunas();
            }}
          />
          <button
            className={
              btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-200"
            }
            onClick={handleOpenInNewTab}
            disabled={isLoading || !filteredRows.length}
          >
            Abrir em nova aba
          </button>
          <button
            className={
              btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-200"
            }
            onClick={handleExportExcel}
            disabled={isLoading || !filteredRows.length}
          >
            Exportar Excel
          </button>
        </div>

        {/* Filters row */}
        <div className="flex gap-2 mt-2 flex-wrap">
          {isProdutosSub ? (
            <select
              className={inputCls + " w-56"}
              value={filterIdAgrupado}
              onChange={(e) => {
                setFilterIdAgrupado(e.target.value);
                setPage(1);
              }}
            >
              <option value="">— Todos os produtos —</option>
              {uniqueIdAgrupados.map((id) => (
                <option key={id} value={id}>
                  {id}
                </option>
              ))}
            </select>
          ) : sub === "bloco_h" ? (
            <>
              <input
                className={inputCls + " w-40"}
                placeholder="Data inv (ex: 2021-12)"
                value={filterBlocoHDataInv}
                onChange={(e) => {
                  setFilterBlocoHDataInv(e.target.value);
                  setPage(1);
                }}
              />
              <select
                className={inputCls + " w-56"}
                value={filterBlocoHMotivo}
                onChange={(e) => {
                  setFilterBlocoHMotivo(e.target.value);
                  setPage(1);
                }}
              >
                <option value="">Motivo: todos</option>
                {(blocoHResumo?.motivos ?? []).map((m) => (
                  <option
                    key={`${m.cod_mot_inv}-${m.mot_inv_desc ?? ""}`}
                    value={m.cod_mot_inv}
                  >
                    {m.cod_mot_inv} - {m.mot_inv_desc ?? "Sem descricao"}
                  </option>
                ))}
              </select>
              {blocoHSubTab === "todos_dados" && (
                <select
                  className={inputCls + " w-44"}
                  value={filterBlocoHPropriedade}
                  onChange={(e) => {
                    setFilterBlocoHPropriedade(e.target.value);
                    setPage(1);
                  }}
                >
                  <option value="">Propriedade: todos</option>
                  {(blocoHResumo?.propriedade ?? []).map((p) => (
                    <option
                      key={`${p.indicador_propriedade}`}
                      value={p.indicador_propriedade}
                    >
                      {p.indicador_propriedade || "(vazio)"}
                    </option>
                  ))}
                </select>
              )}
            </>
          ) : (
            <select className={inputCls + " w-48"} disabled>
              <option>Filtrar id_agrupado</option>
            </select>
          )}
          <input
            className={inputCls + " flex-1"}
            placeholder="Filtrar descricao..."
            value={search}
            onChange={(e) => {
              setSearch(e.target.value);
              setPage(1);
            }}
          />
          {sub === "tabela_mensal" && (
            <>
              <select className={inputCls + " w-24"}>
                <option>Ano: Todos</option>
              </select>
              <select className={inputCls + " w-24"}>
                <option>Mes: Todos</option>
              </select>
            </>
          )}
          {sub === "tabela_anual" && (
            <select className={inputCls + " w-24"}>
              <option>Todos</option>
            </select>
          )}
        </div>

        <div className="flex items-center gap-3 mt-1">
          <span className="text-xs text-slate-400">
            {isLoading
              ? "Carregando..."
              : `Exibindo ${intlInteger.format(filteredRows.length)} de ${intlInteger.format(data?.total_rows ?? 0)} linhas.`}
          </span>
          {hasFilters && (
            <span className="text-xs text-amber-400">
              Filtros ativos:{" "}
              {[
                filterIdAgrupado && "id_agrupado",
                filterBlocoHDataInv && "dt_inv",
                filterBlocoHMotivo && "motivo",
                filterBlocoHPropriedade && "propriedade",
                search && "descricao",
              ]
                .filter(Boolean)
                .join(", ")}
            </span>
          )}
        </div>

        {sub === "bloco_h" &&
          blocoHSubTab === "h005_resumo" &&
          blocoHResumo && (
            <div className="mt-3 grid grid-cols-1 md:grid-cols-4 gap-2">
              <div className="bg-slate-900 border border-slate-700 rounded px-2 py-2">
                <div className="text-[11px] text-slate-400">
                  Inventarios H005
                </div>
                <div className="text-sm font-semibold text-slate-200">
                  {intlInteger.format(blocoHResumo.inventarios_h005 ?? 0)}
                </div>
              </div>
              <div className="bg-slate-900 border border-slate-700 rounded px-2 py-2">
                <div className="text-[11px] text-slate-400">
                  Produtos distintos (codigo_produto)
                </div>
                <div className="text-sm font-semibold text-slate-200">
                  {intlInteger.format(
                    blocoHResumo.total_produtos_codigo_produto ?? 0,
                  )}
                </div>
              </div>
              <div className="bg-slate-900 border border-slate-700 rounded px-2 py-2">
                <div className="text-[11px] text-slate-400">
                  Detalhamentos H010
                </div>
                <div className="text-sm font-semibold text-slate-200">
                  {intlInteger.format(blocoHResumo.total_linhas_h010 ?? 0)}
                </div>
              </div>
              <div className="bg-slate-900 border border-slate-700 rounded px-2 py-2">
                <div className="text-[11px] text-slate-400">
                  Valor total dos itens (H010)
                </div>
                <div className="text-sm font-semibold text-slate-200">
                  {intlCurrency.format(blocoHResumo.valor_total_itens ?? 0)}
                </div>
              </div>
              <div className="md:col-span-2 bg-slate-900 border border-slate-700 rounded px-2 py-2">
                <div className="text-[11px] text-slate-400 mb-1">
                  Motivos de inventario
                </div>
                <div className="max-h-24 overflow-auto pr-1 space-y-1">
                  {(blocoHResumo.motivos ?? []).map((m) => (
                    <div
                      key={`${m.cod_mot_inv}-${m.mot_inv_desc ?? ""}`}
                      className="text-xs text-slate-300 flex items-center justify-between"
                    >
                      <span>
                        {m.cod_mot_inv} - {m.mot_inv_desc ?? "Sem descricao"}
                      </span>
                      <span className="text-slate-400">
                        {intlInteger.format(m.qtd_itens)}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
              <div className="md:col-span-2 bg-slate-900 border border-slate-700 rounded px-2 py-2">
                <div className="text-[11px] text-slate-400 mb-1">
                  Indicador de propriedade
                </div>
                <div className="max-h-24 overflow-auto pr-1 space-y-1">
                  {(blocoHResumo.propriedade ?? []).map((p) => (
                    <div
                      key={`${p.indicador_propriedade}`}
                      className="text-xs text-slate-300 flex items-center justify-between"
                    >
                      <span>{p.indicador_propriedade || "(vazio)"}</span>
                      <span className="text-slate-400">
                        {intlInteger.format(p.qtd_itens)}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}
      </div>

      <div className="flex-1 overflow-hidden">
        <DataTable
          appearanceKey={`estoque_${sub}_${blocoHSubTab}`}
          columns={colunasDisponiveis}
          orderedColumns={ordemColunas}
          columnWidths={largurasColunas}
          onOrderedColumnsChange={definirOrdemColunas}
          onColumnWidthChange={definirLarguraColuna}
          rows={filteredRows}
          totalRows={isProdutosSub ? filteredRows.length : data?.total_rows}
          loading={isLoading}
          page={isProdutosSub ? 1 : page}
          totalPages={isProdutosSub ? 1 : data?.total_pages}
          onPageChange={isProdutosSub ? undefined : setPage}
          sortBy={sort?.col}
          sortDesc={sort?.desc ?? false}
          onSortChange={(col, desc) => {
            setSort({ col, desc });
            setPage(1);
          }}
          columnFilters={columnFilters}
          onColumnFilterChange={(col, val) => {
            setColumnFilters((prev) => ({ ...prev, [col]: val }));
            setPage(1);
          }}
          highlightRows={sub === "mov_estoque"}
          rowKey={isProdutosSub ? "id_agrupado" : undefined}
          selectedRowKeys={isProdutosSub ? selectedRowKeys : undefined}
          onRowSelect={isProdutosSub ? handleRowSelect : undefined}
          onSelectAll={isProdutosSub ? handleSelectAll : undefined}
          hiddenColumns={hiddenCols}
        />
      </div>
    </div>
  );
}

export function EstoqueTab() {
  const { selectedCnpj } = useAppStore();
  const [subTab, setSubTab] = useState<SubTab>("mov_estoque");

  const { data: movData } = useQuery({
    queryKey: ["mov_estoque_count", selectedCnpj],
    queryFn: () => estoqueApi.movEstoque(selectedCnpj!, 1, 1),
    enabled: !!selectedCnpj,
  });
  const { data: mensalData } = useQuery({
    queryKey: ["tabela_mensal_count", selectedCnpj],
    queryFn: () => estoqueApi.tabelaMensal(selectedCnpj!, 1, 1),
    enabled: !!selectedCnpj,
  });
  const { data: anualData } = useQuery({
    queryKey: ["tabela_anual_count", selectedCnpj],
    queryFn: () => estoqueApi.tabelaAnual(selectedCnpj!, 1, 1),
    enabled: !!selectedCnpj,
  });
  const { data: idAgrData } = useQuery({
    queryKey: ["id_agrupados_count", selectedCnpj],
    queryFn: () => estoqueApi.idAgrupados(selectedCnpj!, 1, 1),
    enabled: !!selectedCnpj,
  });
  const { data: blocoHData } = useQuery({
    queryKey: ["bloco_h_count", selectedCnpj],
    queryFn: () => estoqueApi.blocoH(selectedCnpj!, 1, 1),
    enabled: !!selectedCnpj,
  });

  if (!selectedCnpj) {
    return (
      <div className="flex items-center justify-center h-full text-slate-500">
        Selecione um CNPJ.
      </div>
    );
  }

  const subtabs: { key: SubTab; label: string; count?: number }[] = [
    {
      key: "mov_estoque",
      label: "Movimentacao de Estoque",
      count: movData?.total_rows,
    },
    {
      key: "tabela_mensal",
      label: "Apuracao Mensal",
      count: mensalData?.total_rows,
    },
    {
      key: "tabela_anual",
      label: "Apuracao Anual",
      count: anualData?.total_rows,
    },
    { key: "resumo", label: "Resumo Fiscal" },
    {
      key: "produtos",
      label: "Produtos Consolidados",
      count: idAgrData?.total_rows,
    },
    {
      key: "id_agrupados",
      label: "Cadastros Agrupados",
      count: idAgrData?.total_rows,
    },
    {
      key: "bloco_h",
      label: "Bloco H (Inventario)",
      count: blocoHData?.total_rows,
    },
  ];

  return (
    <div className="flex flex-col h-full">
      {/* Subtab bar */}
      <div
        className="flex gap-1 px-3 pt-2 border-b border-slate-700"
        style={{ background: "#0a1628" }}
      >
        {subtabs.map((st) => (
          <button
            key={st.key}
            onClick={() => setSubTab(st.key)}
            className={`px-3 py-1.5 rounded-t text-xs font-medium transition-colors border-t border-l border-r ${
              subTab === st.key
                ? "border-slate-600 text-white"
                : "border-transparent text-slate-400 hover:text-slate-200"
            }`}
            style={subTab === st.key ? { background: "#0f1b33" } : {}}
          >
            {st.label}
            {st.count !== undefined ? ` (${intlInteger.format(st.count)})` : ""}
          </button>
        ))}
      </div>

      <div className="flex-1 overflow-hidden">
        <EstoqueSubTab cnpj={selectedCnpj} sub={subTab} />
      </div>
    </div>
  );
}
