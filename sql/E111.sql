WITH parametros AS (
    SELECT
        -- CNPJ que será usado como filtro principal da consulta
        :CNPJ AS cnpj_filtro,

        -- Data inicial do período consultado.
        -- Se o parâmetro não for informado, assume 01/01/1900
        -- para permitir busca desde o início da base.
        NVL(
            TO_DATE(:data_inicial, 'DD/MM/YYYY'),
            DATE '1900-01-01'
        ) AS dt_ini_filtro,

        -- Data final do período consultado.
        -- Se o parâmetro não for informado, assume a data atual.
        NVL(
            TO_DATE(:data_final, 'DD/MM/YYYY'),
            TRUNC(SYSDATE)
        ) AS dt_fim_filtro,

        -- Data de corte para "viagem no tempo".
        -- Define até qual data de entrega um arquivo será considerado válido.
        -- Se não for informada, assume a data atual.
        NVL(
            TO_DATE(:data_limite_processamento, 'DD/MM/YYYY'),
            TRUNC(SYSDATE)
        ) AS dt_corte
    FROM dual
),

arquivos_ranking AS (
    SELECT
        -- Identificador do registro principal da EFD
        r0.id AS reg_0000_id,

        -- CNPJ do contribuinte
        r0.cnpj,

        -- Código da finalidade do arquivo EFD
        r0.cod_fin AS cod_fin_efd,

        -- Data inicial do período da escrituração
        r0.dt_ini,

        -- Data final do período da escrituração
        r0.dt_fin,

        -- Data em que essa versão do arquivo foi entregue
        r0.data_entrega,

        -- Ranking para identificar a última versão válida do arquivo
        -- dentro do mesmo período (dt_ini/dt_fin) para o mesmo CNPJ.
        -- A versão mais recente recebe rn = 1.
        ROW_NUMBER() OVER (
            PARTITION BY r0.cnpj, r0.dt_ini, r0.dt_fin
            ORDER BY r0.data_entrega DESC
        ) AS rn
    FROM sped.reg_0000 r0
    JOIN parametros p
        ON r0.cnpj = p.cnpj_filtro
    WHERE
        -- Considera apenas arquivos entregues até a data de corte
        r0.data_entrega <= p.dt_corte

        -- Filtra períodos que tenham interseção com o intervalo informado
        -- pelo usuário
        AND r0.dt_fin >= p.dt_ini_filtro
        AND r0.dt_ini <= p.dt_fim_filtro
)

SELECT
    -- Período da EFD no formato AAAA/MM
    TO_CHAR(arq.dt_ini, 'YYYY/MM') AS periodo_efd,

    -- Código do ajuste de apuração
    e111.cod_aj_apur AS codigo_ajuste,

    -- Descrição do código de ajuste, quando encontrada na dimensão
    aj.no_cod_aj AS descricao_codigo_ajuste,

    -- Descrição complementar informada no registro E111
    e111.descr_compl_aj AS descr_compl,

    -- Valor do ajuste de apuração
    e111.vl_aj_apur AS valor_ajuste,

    -- Data de entrega da versão da EFD considerada válida para o período
    arq.data_entrega AS data_entrega_efd_periodo,

    -- Código de finalidade da EFD
    arq.cod_fin_efd

FROM arquivos_ranking arq

    -- Junta com os registros E111 da versão selecionada do arquivo EFD
    INNER JOIN sped.reg_e111 e111
        ON e111.reg_0000_id = arq.reg_0000_id

    -- Junta com a dimensão de códigos de ajuste para obter a descrição
    -- RTRIM é usado para compatibilizar possíveis espaços à direita
    LEFT JOIN bi.dm_efd_ajustes aj
        ON e111.cod_aj_apur = RTRIM(aj.co_cod_aj)

WHERE
    -- Mantém somente a última versão válida do arquivo por período
    arq.rn = 1

ORDER BY
    -- Ordena cronologicamente por período
    arq.dt_ini,

    -- Em seguida, pelo código do ajuste
    e111.cod_aj_apur;