# AGENTS_SQL.md — Contrato SQL para Extração, Consolidação e Reuso Fiscal

## Missão do SQL
O SQL deste projeto existe para:
- extrair dados com eficiência;
- reduzir custo de leitura;
- alimentar datasets canônicos em Parquet.

Ele não deve ser tratado como camada final de visualização.

## Princípios obrigatórios

### Segurança e integridade
- todo SQL deve ficar em arquivo estático versionado;
- nunca concatenar parâmetros em Python ou TypeScript;
- usar bind variables sempre.

### Reuso estrutural
- não criar consultas monolíticas independentes para cada tela;
- sempre reaproveitar blocos lógicos existentes;
- duplicação de CTE estrutural é sinal de módulo reutilizável faltando.

### SQL para extração; não para acoplamento visual
- o SQL deve entregar dados úteis para materialização;
- layout didático e agrupamento visual pertencem à camada de apresentação.

## Paradigma oficial
O paradigma oficial passa a ser SQL composável por domínio.

### SQL base
Consultas pequenas e reutilizáveis que representam uma fonte ou regra estrutural.

### SQL de composição
Consultas que combinam módulos base para formar datasets canônicos.

### SQL de inspeção
Consultas leves e legíveis para análise humana, sem virar contrato principal da UI.

## Regra final
Se uma lógica fiscal for reutilizada em mais de uma tela, ela não deve permanecer escondida em uma única query de tela. Ela deve virar base SQL reutilizável ou dataset canônico em Parquet.
