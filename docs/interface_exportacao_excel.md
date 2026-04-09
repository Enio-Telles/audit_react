# Exportacao Excel

## Regra de formato

A exportacao `.xlsx` do projeto segue estas regras:

- numeros devem sair como celulas numericas, com mascara Excel compativel com exibicao brasileira;
- datas devem sair como celulas de data, com formato `dd/mm/yyyy`;
- data e hora devem sair como celulas de data/hora, com formato `dd/mm/yyyy hh:mm:ss`;
- identificadores fiscais e codigos correlatos devem sair como texto, para evitar notacao cientifica, perda de zeros a esquerda ou reinterpretacao pelo Excel.

## Identificadores forçados como texto

Os fluxos de exportacao tratam como texto, por nome de coluna ou preset, campos como:

- `cnpj`, `cpf`, `cpf_cnpj`, `ie`, `nsu`;
- `chave`, `chave_acesso`, `chv_nfe`;
- colunas de codigo como `cod_*`, `*_cod`, `codigo`, `cfop`, `cst`, `csosn`;
- identificadores tecnicos como `id_*`;
- codigos fiscais recorrentes como `ncm`, `cest`, `gtin`, `cep`.

## Pontos de aplicacao

- Exportacao principal com `xlsxwriter`: [exportar_excel_adaptado.py](/c:/Sistema_react/src/utilitarios/exportar_excel_adaptado.py)
- Exportacoes auxiliares com `openpyxl`: [main_window.py](/c:/Sistema_react/src/interface_grafica/ui/main_window.py)

## Observacoes

- A regra e conservadora: na duvida, colunas de identificacao fiscal sao mantidas como texto.
- A formatacao textual exibida em HTML e DOCX continua usando `display_cell`, mas no Excel a prioridade e preservar tipo de celula quando isso melhora analise e filtro no arquivo final.
