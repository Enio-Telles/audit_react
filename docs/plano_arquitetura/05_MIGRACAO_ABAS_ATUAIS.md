# Migração das Abas Atuais

## Regras de reposicionamento

- Estoque -> Cruzamentos
- Agregação -> Verificações
- Conversão -> Verificações

## Justificativa

### Estoque
Opera como leitura analítica derivada de múltiplas fontes e regras. Conceitualmente pertence a cruzamentos fiscais.

### Agregação
Funciona como verificação e consolidação da identidade do produto. Deve alimentar a futura classificação.

### Conversão
É uma verificação estrutural para comparabilidade de unidades, estoque e ressarcimento.

## Estratégia de migração
1. manter compatibilidade temporária
2. criar datasets canônicos equivalentes
3. redirecionar ou renomear navegação
4. remover telas antigas apenas após validação funcional
