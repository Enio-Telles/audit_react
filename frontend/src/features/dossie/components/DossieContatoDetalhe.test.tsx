import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { DossieSectionData } from "../types";
import { DossieContatoDetalhe } from "./DossieContatoDetalhe";

function criar_dados_contato_volume_alto(): DossieSectionData {
  const registros: Record<string, unknown>[] = [];

  for (let indice = 0; indice < 60; indice += 1) {
    registros.push({
      tipo_vinculo: "FILIAL_RAIZ",
      nome_referencia: `Filial ${indice + 1}`,
      cpf_cnpj_referencia: `11111111000${String(indice).padStart(3, "0")}`,
      indicador_matriz_filial: "FILIAL",
      telefone: indice % 2 === 0 ? null : `9230000${String(indice).padStart(4, "0")}`,
      telefone_nfe_nfce: null,
      email: indice % 3 === 0 ? null : `filial${indice + 1}@empresa.com`,
      endereco: `Rua Filial ${indice + 1}`,
      origem_dado: "dados_cadastrais.sql",
      tabela_origem: "SITAFE.SITAFE_HISTORICO_CONTRIBUINTE",
      situacao_cadastral: "ATIVA",
    });
  }

  for (let indice = 0; indice < 40; indice += 1) {
    registros.push({
      tipo_vinculo: "SOCIO_ATUAL",
      nome_referencia: `Socio ${indice + 1}`,
      cpf_cnpj_referencia: `22222222000${String(indice).padStart(3, "0")}`,
      telefone: `9291000${String(indice).padStart(4, "0")}`,
      email: `socio${indice + 1}@empresa.com`,
      endereco: `Endereco Socio ${indice + 1}`,
      origem_dado: "dossie_historico_socios.sql",
      tabela_origem: "SITAFE.SITAFE_HISTORICO_SOCIO",
      situacao_cadastral: "ATIVO",
    });
  }

  for (let indice = 0; indice < 20; indice += 1) {
    registros.push({
      tipo_vinculo: "EMAIL_NFE",
      nome_referencia: `Contato NFe ${indice + 1}`,
      cpf_cnpj_referencia: `33333333000${String(indice).padStart(3, "0")}`,
      telefone: null,
      telefone_nfe_nfce: indice % 2 === 0 ? `9299000${String(indice).padStart(4, "0")}` : null,
      email: `nfe${indice + 1}@empresa.com`,
      endereco: null,
      origem_dado: "NFe.sql",
      tabela_origem: "BI.FATO_NFE_DETALHE",
      situacao_cadastral: null,
    });
  }

  return {
    id: "contato",
    title: "Contato",
    columns: [],
    rows: registros,
    rowCount: registros.length,
    cacheFile: "CNPJ/12345678000190/arquivos_parquet/dossie/dossie_12345678000190_contato.parquet",
    metadata: null,
    updatedAt: "2026-04-08T10:00:00",
  };
}

describe("DossieContatoDetalhe", () => {
  it("exibe contatos do contador consolidados por fonte com FAC como referencia principal", () => {
    const dados: DossieSectionData = {
      id: "contato",
      title: "Contato",
      columns: [],
      rows: [
        {
          tipo_vinculo: "CONTADOR_EMPRESA",
          nome_referencia: "Contador Consolidado",
          cpf_cnpj_referencia: "77777777000166",
          indicador_matriz_filial: "CONTADOR",
          telefone: "6933002200",
          telefone_nfe_nfce: "6933998800",
          email: "misto@fac.com",
          emails_por_fonte: "FAC atual: misto@fac.com | SITAFE_PESSOA: sitafe@contador.com",
          telefones_por_fonte: "FAC atual: 6933002200 | SITAFE_PESSOA: 6933001100",
          fontes_contato: "FAC atual | SITAFE_PESSOA",
          endereco: "Rua FAC, 55, Porto Velho, RO",
          origem_dado: "dossie_historico_fac.sql",
          tabela_origem: "SITAFE.SITAFE_HISTORICO_CONTRIBUINTE; SITAFE.SITAFE_PESSOA; BI.DM_PESSOA",
          situacao_cadastral: "Atual",
        },
      ],
      rowCount: 1,
      cacheFile: "CNPJ/12345678000190/arquivos_parquet/dossie/dossie_12345678000190_contato.parquet",
      metadata: null,
      updatedAt: "2026-04-08T10:00:00",
    };

    render(<DossieContatoDetalhe dados={dados} />);

    expect(screen.getByText("Contador da empresa")).toBeInTheDocument();
    expect(screen.getByText("Contador Consolidado")).toBeInTheDocument();
    expect(screen.getByText("Emails por fonte:")).toBeInTheDocument();
    expect(screen.getByText("FAC atual: misto@fac.com")).toBeInTheDocument();
    expect(screen.getByText("SITAFE_PESSOA: sitafe@contador.com")).toBeInTheDocument();
    expect(screen.getByText("Telefones por fonte:")).toBeInTheDocument();
    expect(screen.getByText("FAC atual: 6933002200")).toBeInTheDocument();
    expect(screen.getByText("SITAFE_PESSOA: 6933001100")).toBeInTheDocument();
    expect(screen.getByText("Fontes consolidadas:")).toBeInTheDocument();
  });

  it("renderiza volume alto de linhas preservando agrupamento e registros extremos", () => {
    const dados = criar_dados_contato_volume_alto();

    render(<DossieContatoDetalhe dados={dados} />);

    expect(screen.getByText("Filiais da mesma raiz")).toBeInTheDocument();
    expect(screen.getByText("60 registros")).toBeInTheDocument();
    expect(screen.getByText("Socios atuais")).toBeInTheDocument();
    expect(screen.getByText("40 registros")).toBeInTheDocument();
    expect(screen.getByText("Emails observados em documentos")).toBeInTheDocument();
    expect(screen.getByText("20 registros")).toBeInTheDocument();

    expect(screen.getByText("Filial 1")).toBeInTheDocument();
    expect(screen.getByText("Filial 60")).toBeInTheDocument();
    expect(screen.getByText("Socio 1")).toBeInTheDocument();
    expect(screen.getByText("Socio 40")).toBeInTheDocument();
    expect(screen.getByText("Contato NFe 1")).toBeInTheDocument();
    expect(screen.getByText("Contato NFe 20")).toBeInTheDocument();

    expect(screen.getAllByText("Filial sem contato cadastral").length).toBeGreaterThan(0);
    expect(screen.getAllByText("BI.FATO_NFE_DETALHE").length).toBeGreaterThan(0);
  });
});
