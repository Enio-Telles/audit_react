# Bloco 0 - Abertura, Identificação e Referências

## Registro 0000 - Abertura do Arquivo Digital e Identificação da Entidade

| Campo | Tipo | Descrição |
| --- | --- | --- |
| ID | VARCHAR2 (1020) | Chave Primária do Item |
| REG | VARCHAR2 (16) | 01 | REG | E310 | 02 |
| COD_VER | VARCHAR2 (12) | O | 02 COD_VER | Código da versão do leiaute conforme a tabela indicada no | Ato COTEPE. |
| COD_FIN | VARCHAR2 (4) | Página 29 de 360 | 03 COD_FIN | Código da finalidade do arquivo: | 0 - Remessa do arquivo original; |
| DT_INI | DATE (7) | O | 04 DT_INI | Data inicial das informações contidas no arquivo. | N |
| DT_FIN | DATE (7) | O | 05 DT_FIN | Data final das informações contidas no arquivo. | N |
| NOME | VARCHAR2 (400) | d) Para finalizar, clicar em “Cadastrar procuração”. | Obs.: No caso de estabelecer Procuração Eletrônica em nome de filial para terceiros: | a) https://cav.receita.fazenda.gov.br/scripts/CAV/login/login.asp | b) Login com certificado digital de pessoa jurídica; |
| CNPJ | VARCHAR2 (56) | Poderão assinar a EFD-ICMS/IPI, com certificados digitais do tipo A1 ou A3: | 1. e-PJ ou e-CNPJ que contenha a mesma base do CNPJ (8 primeiros caracteres) do estabelecimento; | Guia Prático EFD-ICMS/IPI – Versão 3.2.1 | Atualização: 28 de outubro de 2025 |
| CPF | VARCHAR2 (44) | OC | 08 CPF | Número de inscrição da entidade no CPF. | N |
| UF | VARCHAR2 (8) | Sem descrição encontrada no PDF. |
| IE | VARCHAR2 (56) | Sem descrição encontrada no PDF. |
| COD_MUN | VARCHAR2 (28) | 2) Informar no campo IE a inscrição estadual na unidade federada do tomador de serviços/consumidor de energia elétrica; | 3) Informar no campo COD_MUN o código de município correspondente à capital do estado do tomador de | serviços/consumidor de energia elétrica. | Nº Campo |
| IM | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| SUFRAMA | VARCHAR2 (36) | OC | 13 SUFRAMA | Inscrição da entidade na SUFRAMA | C |
| IND_PERFIL | VARCHAR2 (4) | OC | 14 IND_PERFIL | Perfil de apresentação do arquivo fiscal; | A – Perfil A; |
| IND_ATIV | VARCHAR2 (4) | O | 15 IND_ATIV | Indicador de tipo de atividade: | 0 – Industrial ou equiparado a industrial; |
| ARQUIVO_NOME | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| DATA_ENTREGA | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| CREATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| UPDATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| REG_1 | VARCHAR2 (40) | Sem descrição encontrada no PDF. |
| REG_C | VARCHAR2 (40) | Sem descrição encontrada no PDF. |
| REG_D | VARCHAR2 (40) | Sem descrição encontrada no PDF. |
| REG_E | VARCHAR2 (40) | Sem descrição encontrada no PDF. |
| REG_G | VARCHAR2 (40) | Sem descrição encontrada no PDF. |
| REG_H | VARCHAR2 (40) | Sem descrição encontrada no PDF. |
| REG_K | VARCHAR2 (40) | Sem descrição encontrada no PDF. |
| ARQUIVO_TAMANHO | NUMBER (22,38) | Sem descrição encontrada no PDF. |

---

## Registro 0150 - Tabela de Participantes

| Campo | Tipo | Descrição |
| --- | --- | --- |
| ID | VARCHAR2 (1020) | Chave Primária do Item |
| REG_0000_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG | VARCHAR2 (16) | 01 | REG | E310 | 02 |
| COD_PART | VARCHAR2 (240) | Preenchido se ind_prop = 1 ou 2 |
| NOME | VARCHAR2 (400) | d) Para finalizar, clicar em “Cadastrar procuração”. | Obs.: No caso de estabelecer Procuração Eletrônica em nome de filial para terceiros: | a) https://cav.receita.fazenda.gov.br/scripts/CAV/login/login.asp | b) Login com certificado digital de pessoa jurídica; |
| COD_PAIS | VARCHAR2 (20) | 04 | COD_PAIS | Código do país do participante, conforme a tabela | indicada no item 3.2.1 |
| CNPJ | VARCHAR2 (56) | Poderão assinar a EFD-ICMS/IPI, com certificados digitais do tipo A1 ou A3: | 1. e-PJ ou e-CNPJ que contenha a mesma base do CNPJ (8 primeiros caracteres) do estabelecimento; | Guia Prático EFD-ICMS/IPI – Versão 3.2.1 | Atualização: 28 de outubro de 2025 |
| CPF | VARCHAR2 (44) | OC | 08 CPF | Número de inscrição da entidade no CPF. | N |
| IE | VARCHAR2 (56) | Sem descrição encontrada no PDF. |
| COD_MUN | VARCHAR2 (28) | 2) Informar no campo IE a inscrição estadual na unidade federada do tomador de serviços/consumidor de energia elétrica; | 3) Informar no campo COD_MUN o código de município correspondente à capital do estado do tomador de | serviços/consumidor de energia elétrica. | Nº Campo |
| SUFRAMA | VARCHAR2 (36) | OC | 13 SUFRAMA | Inscrição da entidade na SUFRAMA | C |
| END | VARCHAR2 (240) | Campo derivado da consulta: classifica a operação como entrada/saída para o CNPJ filtrado |
| NUM | VARCHAR2 (40) | O | 05 NUM | Número do imóvel. | C |
| COMPL | VARCHAR2 (240) | OC | 06 COMPL | Dados complementares do endereço. | C |
| BAIRRO | VARCHAR2 (240) | OC | 07 BAIRRO | Bairro em que o imóvel está situado. | C |
| CREATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| UPDATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |

---

## Registro 0200 - Tabela de Identificação do Item (Produto e Serviços)

| Campo | Tipo | Descrição |
| --- | --- | --- |
| ID | VARCHAR2 (1020) | Chave Primária do Item |
| REG_0000_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG | VARCHAR2 (16) | 01 | REG | E310 | 02 |
| COD_ITEM | VARCHAR2 (240) | Nova chave de produto fonte |
| DESCR_ITEM | VARCHAR2 (1020) | 03 | DESCR_ITEM | Descrição do item | C |
| COD_BARRA | VARCHAR2 (1020) | 04 | COD_BARRA | C500 | 34 |
| COD_ANT_ITEM | VARCHAR2 (240) | 05 | COD_ANT_ITEM | C120 | 06 |
| UNID_INV | VARCHAR2 (24) | 06 | UNID_INV | Unidade de medida utilizada na quantificação de | estoques. |
| TIPO_ITEM | VARCHAR2 (8) | 00=Mercadoria p/ Revenda, 01=Matéria-Prima, etc. |
| COD_NCM | VARCHAR2 (32) | 08 | COD_NCM | Código da Nomenclatura Comum do Mercosul | C |
| EX_IPI | VARCHAR2 (12) | 09 | EX_IPI | Código EX, conforme a TIPI | C |
| COD_GEN | VARCHAR2 (8) | 10 | COD_GEN | Código do gênero do item, conforme a Tabela | 4.2.1 |
| COD_LST | VARCHAR2 (20) | 11 | COD_LST | Código do serviço conforme lista do Anexo I da | Lei Complementar Federal nº 116/03. |
| ALIQ_ICMS | NUMBER (22,17) | 12 | ALIQ_ICMS | Alíquota de ICMS aplicável ao item nas | operações internas |
| CEST | VARCHAR2 (28) | 13 | CEST | C113 | 10 |
| CREATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| UPDATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |

---

## Registro 0220 - Fatores de Conversão de Unidades

| Campo | Tipo | Descrição |
| --- | --- | --- |
| ID | VARCHAR2 (1020) | Chave Primária do Item |
| REG_0000_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG_0200_ID | VARCHAR2 (1020) | Sem descrição encontrada no PDF. |
| REG | VARCHAR2 (16) | 01 | REG | E310 | 02 |
| UNID_CONV | VARCHAR2 (24) | 02 | UNID_CONV Unidade comercial a ser convertida na unidade de estoque, | referida no registro 0200. Ou unidade do 0200 utilizada na EFD | anterior. |
| FAT_CONV | NUMBER (22,38) | 03 | FAT_CONV | Fator de conversão: fator utilizado para converter (multiplicar) a | unidade a ser convertida na unidade adotada no inventário. |
| CREATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |
| UPDATED_AT | TIMESTAMP(6) (11) | Sem descrição encontrada no PDF. |

---
