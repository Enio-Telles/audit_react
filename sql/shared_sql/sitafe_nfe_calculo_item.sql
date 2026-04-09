-- Extração de itens de cálculo do SITAFE vinculados a um contribuinte
SELECT 
    it_nu_chave_acesso,
    it_nu_item,
    it_co_sefin,
    it_co_rotina_calculo,
    it_vl_icms
FROM sitafe.sitafe_nfe_calculo_item
WHERE it_nu_chave_acesso IN (
    SELECT chave_acesso 
    FROM bi.fato_nfe_detalhe 
    WHERE (co_emitente = :CNPJ OR co_destinatario = :CNPJ)
    AND dhemi >= DATE '2020-01-01'
    AND dhemi <= SYSDATE
)
