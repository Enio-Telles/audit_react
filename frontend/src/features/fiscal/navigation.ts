export interface BootstrapFiscalUrl {
  modoAplicacao: "audit" | null;
  abaAtiva: string | null;
  cnpjSelecionado: string | null;
  dataset: string | null;
  record: string | null;
}

export function normalizarCnpjFiscal(cnpj: string | null | undefined): string {
  return String(cnpj ?? "").replace(/\D/g, "");
}

export function construirUrlFiscal(params: {
  tab: string;
  cnpj?: string | null;
  dataset?: string | null;
  record?: string | null;
}) {
  const url = new URL(window.location.href);
  url.searchParams.set("mode", "audit");
  url.searchParams.set("tab", params.tab);

  const cnpj = normalizarCnpjFiscal(params.cnpj);
  if (cnpj) {
    url.searchParams.set("cnpj", cnpj);
  } else {
    url.searchParams.delete("cnpj");
  }

  if (params.dataset) {
    url.searchParams.set("dataset", params.dataset);
  } else {
    url.searchParams.delete("dataset");
  }

  if (params.record) {
    url.searchParams.set("record", params.record);
  } else {
    url.searchParams.delete("record");
  }

  return url.toString();
}

export function abrirFiscalEmNovaAba(params: {
  tab: string;
  cnpj?: string | null;
  dataset?: string | null;
  record?: string | null;
}) {
  window.open(construirUrlFiscal(params), "_blank", "noopener,noreferrer");
}

export function lerBootstrapFiscalDaUrl(): BootstrapFiscalUrl | null {
  const url = new URL(window.location.href);
  const modoAplicacao = url.searchParams.get("mode");
  const abaAtiva = url.searchParams.get("tab");
  const cnpjSelecionado = normalizarCnpjFiscal(url.searchParams.get("cnpj"));
  const dataset = url.searchParams.get("dataset");
  const record = url.searchParams.get("record");

  if (!modoAplicacao && !abaAtiva && !cnpjSelecionado && !dataset && !record) {
    return null;
  }

  return {
    modoAplicacao: modoAplicacao === "audit" ? "audit" : null,
    abaAtiva,
    cnpjSelecionado: cnpjSelecionado || null,
    dataset,
    record,
  };
}
