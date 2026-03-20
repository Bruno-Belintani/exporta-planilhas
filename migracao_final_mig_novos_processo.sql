-- Script Final de Migração Dinâmico
-- Fonte de dados: mig_novos_processo

-- Inserindo dados na tabela 'p_processos' a partir da tabela temporária
INSERT INTO p_processos (
    pro_cod_ext, pro_par_cnt, pro_dta_ent, pro_nro_lmp, pro_nro
)
SELECT 
    gcpj, 
    reclamante, 
    data_cadastro, 
    processo, 
    processo_formatado
FROM mig_novos_processo;

-- Inserindo dados na tabela 'p_foro' a partir da tabela temporária
INSERT INTO p_foro (
    for_dsc
)
SELECT 
    orgao_julgador
FROM mig_novos_processo;

-- Inserindo dados na tabela 'p_vara' a partir da tabela temporária
INSERT INTO p_vara (
    var_dsc
)
SELECT 
    vara
FROM mig_novos_processo;

-- Inserindo dados na tabela 'p_comarca' a partir da tabela temporária
INSERT INTO p_comarca (
    com_dsc
)
SELECT 
    comarca
FROM mig_novos_processo;

