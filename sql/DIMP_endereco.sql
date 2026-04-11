SELECT
    dimp.reg0100s.cpf,
    dimp.reg0100s.cnpj,
    dimp.reg0100s.cod_cliente,
    dimp.reg0100s.n_fant,
    dimp.reg0100s."END",
    dimp.reg0100s.cep,
    dimp.reg0100s.cod_mun,
    dimp.reg0100s.nome_resp,
    dimp.reg0100s.uf,
    dimp.reg0100s.fone_cont,
    dimp.reg0100s.email_cont
FROM
    dimp.reg0100s
WHERE
    dimp.reg0100s.cpf = :cpf