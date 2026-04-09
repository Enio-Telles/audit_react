import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { DossieSectionData } from "../types";
import { DossieContatoDetalhe } from "./DossieContatoDetalhe";

describe("DossieContatoDetalhe", () => {
  it("agrega evidencias por entidade e exibe fontes auditaveis do contador", () => {
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
          emails_por_fonte:
            "FAC atual: misto@fac.com | SITAFE_PESSOA: sitafe@contador.com",
          telefones_por_fonte:
            "FAC atual: 6933002200 | SITAFE_PESSOA: 6933001100",
          fontes_contato: "FAC atual | SITAFE_PESSOA",
          endereco: "Rua FAC, 55, Porto Velho, RO",
          origem_dado: "dossie_historico_fac.sql",
          tabela_origem:
            "SITAFE.SITAFE_HISTORICO_CONTRIBUINTE; SITAFE.SITAFE_PESSOA; BI.DM_PESSOA",
          situacao_cadastral: "Atual | Anterior",
        },
      ],
      rowCount: 1,
      cacheFile:
        "CNPJ/12345678000190/arquivos_parquet/dossie/dossie_12345678000190_contato.parquet",
      metadata: null,
      updatedAt: "2026-04-09T10:00:00",
    };

    render(<DossieContatoDetalhe dados={dados} />);

    expect(screen.getAllByText("Agenda dos contadores").length).toBeGreaterThan(
      0,
    );
    expect(screen.getByText("Contador Consolidado")).toBeInTheDocument();
    expect(screen.getAllByText("FAC atual").length).toBeGreaterThan(0);
    expect(screen.getAllByText("SITAFE_PESSOA").length).toBeGreaterThan(0);
    expect(screen.getByText("NFe/NFCe reconciliado")).toBeInTheDocument();
    expect(screen.getByText("6933998800")).toBeInTheDocument();
    expect(
      screen.getAllByText("SITAFE.SITAFE_HISTORICO_CONTRIBUINTE").length,
    ).toBeGreaterThan(0);
  });

  it("separa empresa, socios atuais e socios antigos na agenda integrada", () => {
    const dados: DossieSectionData = {
      id: "contato",
      title: "Contato",
      columns: [],
      rows: [
        {
          tipo_vinculo: "EMPRESA_PRINCIPAL",
          nome_referencia: "Empresa Base",
          cpf_cnpj_referencia: "12345678000190",
          endereco: "Rua A, Centro",
          origem_dado: "dados_cadastrais.sql",
          tabela_origem: "BI.DM_PESSOA; BI.DM_LOCALIDADE",
          situacao_cadastral: "001 - ATIVA",
        },
        {
          tipo_vinculo: "EMPRESA_FAC_ATUAL",
          nome_referencia: "Empresa Base",
          cpf_cnpj_referencia: "12345678000190",
          telefone: "6932221100",
          email: "fac@empresa.com",
          endereco: "Rua A, 100, Rio Branco, AC",
          origem_dado: "dossie_historico_fac.sql",
          tabela_origem:
            "SITAFE.SITAFE_HISTORICO_CONTRIBUINTE; SITAFE.SITAFE_PESSOA; BI.DM_LOCALIDADE",
          situacao_cadastral: "FAC atual",
        },
        {
          tipo_vinculo: "SOCIO_ATUAL",
          nome_referencia: "Socio Atual",
          cpf_cnpj_referencia: "11111111111",
          email: "atual@socio.com",
          telefone: "69911110000",
          endereco: "Endereco Atual",
          origem_dado: "dossie_historico_socios.sql",
          tabela_origem: "SITAFE.SITAFE_HISTORICO_SOCIO; SITAFE.SITAFE_PESSOA",
          situacao_cadastral: "SOCIO ATUAL",
        },
        {
          tipo_vinculo: "SOCIO_ANTIGO",
          nome_referencia: "Socio Antigo",
          cpf_cnpj_referencia: "22222222222",
          origem_dado: "dossie_historico_socios.sql",
          tabela_origem: "SITAFE.SITAFE_HISTORICO_SOCIO; SITAFE.SITAFE_PESSOA",
          situacao_cadastral: "SOCIO ANTIGO",
        },
      ],
      rowCount: 4,
      cacheFile:
        "CNPJ/12345678000190/arquivos_parquet/dossie/dossie_12345678000190_contato.parquet",
      metadata: null,
      updatedAt: "2026-04-09T10:00:00",
    };

    render(<DossieContatoDetalhe dados={dados} />);

    expect(screen.getAllByText("Agenda da empresa").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Agenda dos socios").length).toBeGreaterThan(0);
    expect(screen.getByText("Empresa Base")).toBeInTheDocument();
    expect(screen.getByText("Socio Atual")).toBeInTheDocument();
    expect(screen.getByText("Socio Antigo")).toBeInTheDocument();
    expect(screen.getAllByText("FAC atual").length).toBeGreaterThan(0);
    expect(screen.getByText("Sem telefone")).toBeInTheDocument();
    expect(screen.getByText("Sem email")).toBeInTheDocument();
  });
});
