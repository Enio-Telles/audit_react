# Cache do Dossiê

Este documento explica a estratégia de reaproveitamento de extrações do dossiê no `main`.

## Objetivo

Evitar:

- extrair duas vezes a mesma seção;
- salvar resultados duplicados;
- perder rastreabilidade entre CNPJ, seção e consulta usada.

## Chave de cache

A identidade de uma extração do dossiê deve considerar:

- `cnpj`
- `secao`
- `parametros` normalizados
- `versao_consulta`

A chave curta é gerada por hash estável para permitir:

- nome de arquivo previsível;
- reuso seguro;
- invalidação automática quando a consulta mudar.

## Utilitário atual

O `main` agora possui um utilitário dedicado em:

```text
src/interface_grafica/services/dossie_cache_keys.py
```

Funções principais:

- `normalizar_parametros_dossie`
- `serializar_parametros_dossie`
- `gerar_chave_cache_dossie`
- `gerar_nome_arquivo_cache_dossie`
- `criar_chave_cache_secao` como alias legada para compatibilidade com chamadores antigos do backend

## Regras de normalização

Os parâmetros do dossiê são normalizados para reduzir duplicação por diferença apenas de forma:

- chaves são ordenadas;
- valores vazios são removidos;
- listas equivalentes passam a ter serialização estável;
- estruturas internas são normalizadas recursivamente.

## Exemplo conceitual

Entrada:

```python
{
    "fim": "2025-12-31",
    "inicio": "2025-01-01",
    "ufs": ["GO", "TO"]
}
```

Se a mesma consulta chegar com as UFs invertidas, a chave de cache deve continuar a mesma.

## Nome de arquivo

O nome de arquivo segue a ideia:

```text
dossie_<cnpj>_<secao>_<hash>.parquet
```

Exemplo:

```text
dossie_12345678000190_nfe_saida_ab12cd34ef56.parquet
```

## Benefícios

- reduz custo de reextração;
- evita salvar duas vezes a mesma coisa;
- facilita auditoria de resultados persistidos;
- prepara o backend para catálogo de seções do dossiê.

## Compatibilidade retroativa

O nome canônico para geração da chave continua sendo `gerar_chave_cache_dossie`.

Para preservar compatibilidade com imports antigos do backend, o módulo também expõe
`criar_chave_cache_secao`, que delega internamente para a função canônica sem alterar
as regras de formação da chave.

## Próximo passo natural

Usar essas chaves no serviço real de persistência do dossiê, conectando:

1. catálogo de seções;
2. alias para SQL existente;
3. leitura de cache antes da execução;
4. gravação de cache após extração nova.
