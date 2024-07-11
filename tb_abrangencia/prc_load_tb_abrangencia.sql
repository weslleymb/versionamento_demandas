CREATE OR REPLACE PROCEDURE `sp.prc_load_tb_abrangencia`(VAR_PRJ_RAW STRING, VAR_PRJ_TRUSTED STRING)
BEGIN

    DECLARE VAR_PROCEDURE DEFAULT 'prc_load_tb_abrangencia';
    DECLARE VAR_DELTA_INI INT64;
    DECLARE VAR_DELTA_FIM INT64;
    DECLARE VAR_TABELA STRING;
    DECLARE VAR_INICIO DATE;
    DECLARE VAR_DTH_INICIO TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    DECLARE VAR_ID_CARD STRING DEFAULT '';

    -- Inicio do bloco de TRY/CATCH (tratamento de erros)
    BEGIN

        -- Recupera parametros 
        SELECT 'Execucao procedure' AS get_paramns;

        ------------------------------------------
        --: Variaveis desenvolvimento
        --SET VAR_DELTA_INI = ;
        --SET VAR_DELTA_FIM = ;
        --SET VAR_ID_CARD = '';
        ------------------------------------------


        EXECUTE IMMEDIATE """
        CREATE TEMP TABLE tmp_origem_material_promo AS 
        SELECT *
        FROM (
        SELECT 
            'ABC-123' AS chave
            , '[{"cod":"51315", "cat": true},{"cod":"51315 ", "cat": true}]' AS material_trg
            , '[{"cod":"51315", "cat": true,"perc":20.0}]' AS material_bnf
        UNION ALL
        SELECT 
            'DEF-456' AS codigo
            , '[{"cod":"46784", "cat": false},{"cod":"57893", "cat": true}]' AS material_trg
            , '[{"cod":"65342", "cat": false,"perc":20.0},{"cod":"57893", "cat": true,"perc":5.0}]' AS material_bnf
        ) AS dataset_HML_tb_origem;
        """;

        EXECUTE IMMEDIATE """
        CREATE TEMP TABLE tmp_percentual_ideal AS 
        SELECT *
        FROM (
            SELECT '51315' AS codigo, CAST(15.0 AS NUMERIC) AS percentual_ideal UNION ALL
            SELECT '46784' AS codigo, CAST(15.0 AS NUMERIC) AS percentual_ideal UNION ALL
            --SELECT '57893' AS codigo, CAST(10.0 AS NUMERIC) AS percentual_ideal UNION ALL
            SELECT '65342' AS codigo, CAST(21.0 AS NUMERIC) AS percentual_ideal
        ) AS origem_percentual_ideal_arq""";

        EXECUTE IMMEDIATE """
        CREATE TEMP TABLE tmp_material_promo_trg AS 
        SELECT DISTINCT
            chave
            , JSON_EXTRACT_SCALAR(material, '$.cod') AS codigo
            , CAST(JSON_EXTRACT_SCALAR(material, '$.cat') AS BOOLEAN) AS catalogo
        FROM tmp_origem_material_promo
            LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(REPLACE(material_trg, 'None', '"None"'))) AS material
        ;
        """;

        EXECUTE IMMEDIATE """
        CREATE TEMP TABLE tmp_material_promo_bnf AS 
        SELECT DISTINCT
            chave
            , JSON_EXTRACT_SCALAR(material, '$.cod') AS codigo
            , CAST(JSON_EXTRACT_SCALAR(material, '$.cat') AS BOOLEAN) AS catalogo
            , JSON_EXTRACT_SCALAR(material, '$.perc') AS percentual
        FROM tmp_origem_material_promo
            LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(REPLACE(material_bnf, 'None', '"None"'))) AS material
        ;
        """;

        EXECUTE IMMEDIATE """
        CREATE TEMP TABLE tmp_material_promo_base AS 
        SELECT DISTINCT chave, TRIM(codigo) AS codigo, catalogo FROM tmp_material_promo_trg
        UNION DISTINCT
        SELECT DISTINCT chave, TRIM(codigo), catalogo FROM tmp_material_promo_bnf
        ;
        """;

        EXECUTE IMMEDIATE """
        CREATE TEMP TABLE tmp_abrangencia AS 
        SELECT DISTINCT 
            base.chave
            , TRIM(base.codigo) AS codigo
            , CASE WHEN trg.codigo IS NULL THEN FALSE ELSE TRUE END AS flg_trg
            , CASE WHEN bnf.codigo IS NULL THEN FALSE ELSE TRUE END AS flg_bnf
            , IFNULL(prc.percentual_ideal, CAST(bnf.percentual AS NUMERIC)) AS percentual_ideal
            , base.catalogo
        FROM tmp_material_promo_base AS base

            LEFT JOIN tmp_material_promo_trg AS trg
            ON trg.codigo = base.codigo

            LEFT JOIN tmp_material_promo_bnf AS bnf
            ON bnf.codigo = base.codigo

            LEFT JOIN tmp_percentual_ideal AS prc
            ON prc.codigo = base.codigo

        ;
        """;

        EXECUTE IMMEDIATE """
        TRUNCATE TABLE """ || VAR_PRJ_TRUSTED || """.teste.tb_abrangencia""" || VAR_ID_CARD || """;
        """;

        EXECUTE IMMEDIATE """
        INSERT INTO `""" || VAR_PRJ_TRUSTED || """.teste.tb_abrangencia""" || VAR_ID_CARD || """` 
        SELECT DISTINCT chave, codigo, flg_trg, flg_bnf, percentual_ideal, catalogo
        FROM tmp_abrangencia AS origem
        """;

        SELECT 'SUCESSO' AS status_execucao;

    EXCEPTION WHEN ERROR THEN
    
        SELECT 'FALHA' AS status_execucao;

    END;
END;