SELECT
    co_regime_pagto,
    desc_reg_pagto,
    MIN(da_referencia)  inicio,
    CASE WHEN MAX(da_referencia) = TRUNC(SYSDATE,'mm') THEN 'Atual' ELSE TO_CHAR(MAX(da_referencia)) END fim
FROM BI.dm_regime_pagto_contribuinte
WHERE co_cnpj_cpf = :CNPJ
GROUP BY co_cnpj_cpf, co_regime_pagto, desc_reg_pagto
ORDER BY 3 DESC
