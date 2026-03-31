import os
import json
from pathlib import Path

# Diretórios
BASE_DIR = Path("/home/ubuntu/audit_react_mock/storage")
CONFIG_DIR = BASE_DIR / "_config"
CNPJ_DIR = BASE_DIR / "CNPJ"

# Criar diretórios
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
CNPJ_DIR.mkdir(parents=True, exist_ok=True)

# Dados do Auditor
auditor_data = {
    "nome": "ENIO CARSTENS TELLES",
    "cargo": "Auditor Fiscal de Tributos Estaduais",
    "matricula": "300201625",
    "orgao": "SEFIN/CRE/GEFIS - Gerência de Fiscalização",
    "endereco": "Avenida Farquar, nº 2986 - Palácio Rio Madeira - Bairro Pedrinhas - CEP 76.801-470 - Porto Velho/RO",
    "local_data": "Porto Velho, 29 de março de 2026"
}

with open(CONFIG_DIR / "auditor.json", "w", encoding="utf-8") as f:
    json.dump(auditor_data, f, ensure_ascii=False, indent=4)

# Dados dos Contribuintes
empresas = [
    {
        "cnpj": "23722199000167",
        "contribuinte": "CARLOS GOMES CONSULTORIA E SERVICOS GEOLOGICOS E AMBIENTAIS LTDA",
        "ie": "00000006953956",
        "dsf": "20263710400226",
        "notificacao_det": "15397588",
        "manifestacao": "Não apresentou manifestação",
        "contatos_realizados": "Após envio da Notificação DET, envidou-se contato por e-mail, Whatsapp e por telefone. Contatou-se a empresa de contabilidade Alliance, em 10/03/2026 (69 98405-0500): foi confirmada a ciência da notificação relativa ao não atendimento de pendências do Fisconforme. Recomendou-se o saneamento das pendências relacionadas à notificação 15397588. Fora dada ciência no DET 05/03/2026, porém até o momento não se identificou saneamento da inconsistência.",
        "decisao_fiscal": "Encaminhamento para ação fiscal, considerando-se a inação do contribuinte.",
        "desfecho": "Não houve resposta à notificação n. 15397588 e as notificações do Fisconforme continuam com status pendente.",
        "pdf_det": "/home/ubuntu/upload/2_det_carlos_gomes.pdf"
    },
    {
        "cnpj": "46675389000176",
        "contribuinte": "COOFFER COMERCIO DE ALIMENTOS LTDA",
        "ie": "00000006366007",
        "dsf": "20263710400226",
        "notificacao_det": "15397592",
        "manifestacao": "Não apresentou manifestação",
        "contatos_realizados": "Após envio da Notificação DET, envidou-se contato por e-mail, Whatsapp e por telefone. No endereço da empresa, não foi encontrado estabelecimento em atividade. Foi reconhecida ciência tácita no DET 20/03/2026, porém até o momento não se identificou saneamento da inconsistência.",
        "decisao_fiscal": "Encaminhamento para ação fiscal, considerando-se a inação do contribuinte.",
        "desfecho": "Não houve resposta à notificação n. 15397592 e as notificações do Fisconforme continuam com status pendente.",
        "pdf_det": "/home/ubuntu/upload/02_det_cooffer.pdf"
    },
    {
        "cnpj": "19288989000290",
        "contribuinte": "M C INDUSTRIA E COMERCIO DE PAPEIS LTDA",
        "ie": "00000005597099",
        "dsf": "20263710400226",
        "notificacao_det": "15397586",
        "manifestacao": "Não apresentou manifestação",
        "contatos_realizados": "Após envio da Notificação DET, envidou-se contato por e-mail, Whatsapp e por telefone. Contatou-se o contador do Contribuinte, o Sr. Oziel, 10/03/2026, 15:15. Recomendou-se o saneamento das pendências relacionadas ao Fisconforme. Fora dada ciência no DET 06/03/2026, porém até o momento não se identificou saneamento da inconsistência.",
        "decisao_fiscal": "Encaminhamento para ação fiscal, considerando-se a inação do contribuinte.",
        "desfecho": "Houve resposta à notificação n. 15397586, com pedido de prorrogação de prazo (denegado); as notificações do Fisconforme continuam com status pendente e indeferido.",
        "pdf_det": "/home/ubuntu/upload/02_det_mc.pdf"
    },
    {
        "cnpj": "32846441000103",
        "contribuinte": "MEGAFORTT COMERCIO DE ALIMENTOS LTDA",
        "ie": "00000005289254",
        "dsf": "20263710400226",
        "notificacao_det": "15397591",
        "manifestacao": "Não apresentou manifestação",
        "contatos_realizados": "Após envio da Notificação DET n. 15397591, envidou-se contato por e-mail, Whatsapp e por telefone. Contatou-se o contador do Contribuinte, o Sr. Geovany, 10/03/2026, por meio do telefone 69 98169-2945. Recomendou-se que acessasse o Portal do Contribuinte e realizasse o saneamento das pendências relacionadas ao Fisconforme. Foi dada ciência no DET 05/03/2026. O contribuinte apôs contestação à notificação de não cumprimento das inconsistências. Argumentou que havia solicitado cancelamento extemporâneo da nota fiscal relacionada à pendência. A contestação foi indeferida, porém pedido de autorização para cancelamento foi deferido, com ciência do contribuinte. Resta, atualmente, o contribuinte apor o evento de cancelamento para que a inconsistência seja saneada. Até o momento, não houve o registro do evento.",
        "decisao_fiscal": "Encaminhamento para ação fiscal, caso o contribuinte não efetue o registro de cancelamento da nota fiscal.",
        "desfecho": "Mantém-se o status pendente, haja vista que o contribuinte não registrou o evento de cancelamento da nota fiscal, muito embora já tenha obtido autorização para realizar o lançamento.",
        "pdf_det": "/home/ubuntu/upload/02_det_megafortt.pdf"
    },
    {
        "cnpj": "22397093000172",
        "contribuinte": "PORTOGASES COMERCIO E DISTRIBUICAO DE GASES LTDA",
        "ie": "00000004316886",
        "dsf": "20263710400226",
        "notificacao_det": "15397587",
        "manifestacao": "Não apresentou manifestação",
        "contatos_realizados": "Após envio da Notificação DET, envidou-se contato por e-mail, Whatsapp e por telefone. Contatou-se o contador do Contribuinte, o Sra. Elenice, 10/03/2026, 15:15, por meio do telefone 69 99279-8052. Recomendou-se que acessasse o Portal do Contribuinte e realizasse o saneamento das pendências relacionadas ao Fisconforme. Foi dada ciência no DET 14/03/2026, porém até o momento não se identificou saneamento da inconsistência.",
        "decisao_fiscal": "Encaminhamento para ação fiscal, considerando-se a inação do contribuinte.",
        "desfecho": "Não houve resposta à notificação n. 15397587 e as notificações do Fisconforme continuam com status pendente.",
        "pdf_det": "/home/ubuntu/upload/02_det_portogases.pdf"
    },
    {
        "cnpj": "21418376000190",
        "contribuinte": "PRIM INDUSTRIA E COMERCIO DE ARTEFATOS DE CIMENTO LTDA",
        "ie": "00000004208927",
        "dsf": "20263710400226",
        "notificacao_det": "15397585",
        "manifestacao": "Não apresentou manifestação",
        "contatos_realizados": "Após envio da Notificação DET, envidou-se contato por e-mail, Whatsapp e por telefone. Contatou-se a sócia Cirlene Prim 10/03/2026, 15:36. Recomendou-se que acessasse o Portal do Contribuinte para verificar e sanar pendências relacionadas ao Fisconforme. Foi dada ciência no DET em 19/03/2026, porém até o momento não se identificou saneamento da inconsistência.",
        "decisao_fiscal": "Encaminhamento para ação fiscal, considerando-se a inação do contribuinte.",
        "desfecho": "Não houve resposta à notificação n. 15397585 e a notificação do Fisconforme continua com status pendente.",
        "pdf_det": "/home/ubuntu/upload/02_det_prim.pdf"
    }
]

for emp in empresas:
    cnpj = emp["cnpj"]
    emp_dir = CNPJ_DIR / cnpj / "relatorio"
    emp_dir.mkdir(parents=True, exist_ok=True)
    
    with open(emp_dir / "dados.json", "w", encoding="utf-8") as f:
        json.dump(emp, f, ensure_ascii=False, indent=4)

print("Estrutura de dados criada com sucesso!")
