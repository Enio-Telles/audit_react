export interface BootstrapDossieUrl {
  modoAplicacao: "audit" | null;
  abaAtiva: string | null;
  cnpjSelecionado: string | null;
}

export function normalizar_cnpj_dossie(cnpj: string | null | undefined): string {
  return String(cnpj ?? "").replace(/\D/g, "");
}

export function construir_url_dossie(cnpj: string): string {
  const url = new URL(window.location.href);
  const cnpjNormalizado = normalizar_cnpj_dossie(cnpj);

  url.searchParams.set("mode", "audit");
  url.searchParams.set("tab", "dossie");

  if (cnpjNormalizado) {
    url.searchParams.set("cnpj", cnpjNormalizado);
  } else {
    url.searchParams.delete("cnpj");
  }

  return url.toString();
}

export function abrir_dossie_em_nova_aba(cnpj: string): void {
  window.open(construir_url_dossie(cnpj), "_blank", "noopener,noreferrer");
}

export function ler_bootstrap_dossie_da_url(): BootstrapDossieUrl | null {
  const url = new URL(window.location.href);
  const modoAplicacao = url.searchParams.get("mode");
  const abaAtiva = url.searchParams.get("tab");
  const cnpjSelecionado = normalizar_cnpj_dossie(url.searchParams.get("cnpj"));

  if (!modoAplicacao && !abaAtiva && !cnpjSelecionado) {
    return null;
  }

  return {
    modoAplicacao: modoAplicacao === "audit" ? "audit" : null,
    abaAtiva,
    cnpjSelecionado: cnpjSelecionado || null,
  };
}

export function limpar_bootstrap_dossie_da_url(): void {
  const url = new URL(window.location.href);

  url.searchParams.delete("mode");
  url.searchParams.delete("tab");
  url.searchParams.delete("cnpj");

  window.history.replaceState({}, document.title, `${url.pathname}${url.search}${url.hash}`);
}
