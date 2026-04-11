# Critérios de Aceite, Testes e Riscos

## Critérios de aceite
- novos domínios fiscais visíveis na navegação
- routers fiscais expostos na API
- datasets canônicos definidos por contrato
- migração das abas legadas documentada

## Testes mínimos
- smoke test dos endpoints de health do módulo fiscal
- validação de rotas renderizadas no frontend
- checagem de regressão das abas legadas

## Riscos principais
- duplicação entre consultas SQL e datasets canônicos
- quebra de navegação durante a migração
- inconsistência de nomenclatura entre backend e frontend
- acoplamento excessivo entre tela e consulta
