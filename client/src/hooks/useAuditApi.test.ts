import { describe, expect, it } from "vitest";
import { construirMensagemErroApi, serializarDetalheErroApi } from "./useAuditApi";

describe("serializarDetalheErroApi", () => {
  it("mantem detalhe em texto simples", () => {
    expect(serializarDetalheErroApi("Falha simples")).toBe("Falha simples");
  });

  it("serializa lista de erros do FastAPI em mensagem legivel", () => {
    expect(
      serializarDetalheErroApi([
        {
          loc: ["query", "por_pagina"],
          msg: "Input should be less than or equal to 1000",
        },
      ]),
    ).toBe("query.por_pagina: Input should be less than or equal to 1000");
  });

  it("serializa objeto estruturado usando mensagem prioritaria", () => {
    expect(
      serializarDetalheErroApi({
        mensagem: "Falha na conexao com o Oracle durante a extracao",
        detalhe: "getaddrinfo failed",
      }),
    ).toBe("Falha na conexao com o Oracle durante a extracao");
  });
});

describe("construirMensagemErroApi", () => {
  it("combina mensagem principal com detalhe estruturado", () => {
    expect(
      construirMensagemErroApi(503, {
        mensagem: "Falha na conexao com o Oracle durante a extracao",
        detalhe: "getaddrinfo failed",
      }),
    ).toBe("Falha na conexao com o Oracle durante a extracao: getaddrinfo failed");
  });

  it("usa texto puro como fallback para respostas nao JSON", () => {
    expect(construirMensagemErroApi(500, null, "Internal Server Error")).toBe("Internal Server Error");
  });
});
