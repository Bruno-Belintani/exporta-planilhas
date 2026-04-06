import pandas as pd
import numpy as np
import re
import math
import sys
import os
import json
from datetime import datetime

MEMORIA_FILE = 'memoria_mapeamento.json'

def fix_mojibake(val):
    if isinstance(val, str):
        # Tenta reverter quando UTF-8 é lido incorretamente como Windows-1252 (cp1252)
        try:
            return val.encode('cp1252').decode('utf-8')
        except Exception:
            pass
        # Fallback para latin-1
        try:
            return val.encode('latin-1').decode('utf-8')
        except Exception:
            pass
    return val

def fix_dataframe_mojibake(df):
    df.columns = [fix_mojibake(col) for col in df.columns]
    for col in df.columns:
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].apply(fix_mojibake)
    return df

def load_memory():
    if os.path.exists(MEMORIA_FILE):
        try:
            with open(MEMORIA_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_memory(memoria):
    with open(MEMORIA_FILE, 'w', encoding='utf-8') as f:
        json.dump(memoria, f, indent=4, ensure_ascii=False)

def clean_col_name(name):
    if pd.isna(name):
        return "coluna_desconhecida"
    name = str(name).strip().lower()
    replacements = {
        'á':'a', 'à':'a', 'ã':'a', 'â':'a',
        'é':'e', 'ê':'e',
        'í':'i',
        'ó':'o', 'õ':'o', 'ô':'o',
        'ú':'u',
        'ç':'c',
        ' ':'_', '-':'_', '/':'_', '\\':'_', '.':'', '(':'', ')':'', 'º':'o', 'ª':'a'
    }
    for k, v in replacements.items():
        name = name.replace(k, v)
    name = re.sub(r'__+', '_', name)
    name = re.sub(r'[^a-z0-9_]', '', name)
    name = name.strip('_')
    if re.match(r'^[0-9]', name):
        name = 'c_' + name
    return name

def sanitize_string_latin1(val):
    if pd.isna(val):
        return 'NULL'
    val = str(val)
    replacements = {
        '“': '"', '”': '"', '„': '"',
        '‘': "'", '’': "'", '‚': "'",
        '–': '-', '—': '-',
        '…': '...',
        '•': '-',
        '€': 'E',
        '™': 'TM',
        '®': '(R)',
        '©': '(C)',
        '˜': '~',
        'ˆ': '^'
    }
    for old, new in replacements.items():
        val = val.replace(old, new)
    
    val_bytes = val.encode('latin-1', 'replace')
    val = val_bytes.decode('latin-1')
    val = val.replace("'", "''")
    return f"'{val}'"

def format_value(val):
    if pd.isna(val):
        return 'NULL'
    if isinstance(val, (int, float, np.integer, np.floating)):
        if math.isnan(val) or math.isinf(val):
            return 'NULL'
        return str(val)
    if isinstance(val, pd.Timestamp) or isinstance(val, datetime):
        return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
    return sanitize_string_latin1(val)

def heuristic_guess(col_name):
    """
    Inteligência heurística base para sugerir o campo no LegalManager
    mesmo que a memória json esteja vazia na primeira vez.
    """
    col = str(col_name).strip().upper()
    
    if "RECLAMANTE" in col or "AUTOR" in col: return "p_processos.pro_par_pri"
    if "RECLAMADA" in col or "RÉU" in col or "REU" in col: return "p_processos.pro_par_cnt"
    if "DATA CADASTRO" in col or "DATA_CADASTRO" in col: return "p_processos.pro_dta_ent"
    if col == "PROCESSO": return "p_processos.pro_nro"
    if "VARA" in col: return "p_vara.var_dsc"
    if "COMARCA" in col: return "p_comarca.com_dsc"
    if "TIPO" in col or "AÇÃO" in col or "ACAO" in col: return "p_acoes.aca_nom"
    if "STATUS" in col: return "p_processos.pro_flg_sta"
    if "GCPJ" in col or "EMPRESA" in col: return "c_empresas.emp_nom"
    if "ORGAO" in col or "ÓRGÃO" in col or "TRT" in col or "FORO" in col: return "p_foro.for_dsc"
    if "VALOR" in col: return "p_processos.pro_vlr_est"
    if "AUDIENCIA" in col or "AUDIÊNCIA" in col: return "p_audiencias.aud_dta"
    if "VENCTO" in col or "VENCIMENTO" in col: return "c_titulos_pagar.tit_dta_vnc"
    if "CPF" in col or "CNPJ" in col: return "c_pessoas.pes_cpf_cgc"
        
    return "PULAR"

def get_table_prefix(table_name):
    parts = table_name.split('_')
    if len(parts) > 1 and len(parts[0]) == 1:
        return parts[1][:3]
    return table_name[:3]

def process_dataframe_columns(df):
    original_cols = df.columns.tolist()
    clean_cols = [clean_col_name(c) for c in original_cols]
    seen = {}
    final_cols = []
    for c in clean_cols:
        if not c:
            c = "coluna"
        if c in seen:
            seen[c] += 1
            final_cols.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            final_cols.append(c)
    df.columns = final_cols
    return original_cols, final_cols

def generate_staging_sql(df, table_name, final_cols):
    sql_lines = []
    sql_lines.append(f"-- Script gerado automaticamente (STAGING)")
    create_table_sql = f"CREATE TABLE {table_name} (\n"
    col_defs = [f"    {col} TEXT" for col in final_cols]
    create_table_sql += ",\n".join(col_defs) + "\n);"
    sql_lines.append(create_table_sql)
    
    cols_joined = ", ".join(final_cols)
    for idx, row in df.iterrows():
        values = [format_value(val) for val in row]
        vals_joined = ", ".join(values)
        insert_sql = f"INSERT INTO {table_name} ({cols_joined}) VALUES ({vals_joined});"
        sql_lines.append(insert_sql)
    
    return "\n".join(sql_lines) + "\n"

def generate_mapping_suggestions(original_cols, memoria):
    suggestions = []
    for orig_col in original_cols:
        if str(orig_col) in memoria:
            sugestao = memoria[str(orig_col)]
        else:
            sugestao = heuristic_guess(orig_col)
        suggestions.append({
            "Coluna Original": orig_col,
            "Destino (tabela.coluna)": sugestao
        })
    return suggestions

def generate_final_sql(mapeamento_validado, table_name):
    # mapeamento_validado is a list of dicts: {'orig_col': ..., 'safe_col': ..., 'tab_dest': ..., 'col_dest': ...}
    sql_lines = []
    sql_lines.append("-- Script Final de Migração Dinâmico (ANSI SQL Massivo)")
    sql_lines.append(f"-- Fonte de staging: {table_name}\n")
    
    tabelas_agrupadas = {}
    for map_item in mapeamento_validado:
        tab = map_item['tab_dest']
        if tab not in tabelas_agrupadas:
            tabelas_agrupadas[tab] = []
        tabelas_agrupadas[tab].append(map_item)
        
    main_tab = max(tabelas_agrupadas.keys(), key=lambda t: len(tabelas_agrupadas[t]))
    main_prefix = get_table_prefix(main_tab)
    lookup_tabs = [t for t in tabelas_agrupadas.keys() if t != main_tab]
    
    for lt in lookup_tabs:
        l_pref = get_table_prefix(lt)
        cols_lt = tabelas_agrupadas[lt]
        chave_dest = cols_lt[0]['col_dest']
        chave_safe = cols_lt[0]['safe_col']
        mig_col = f"mig_{l_pref}_ide"
        
        sql_lines.append(f"-- {'='*60}")
        sql_lines.append(f"-- TABELA DE DOMÍNIO: {lt}")
        sql_lines.append(f"-- {'='*60}\n")
        
        sql_lines.append(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {mig_col} numeric;\n")
        
        sql_lines.append(f"-- 1. Faz de-para com os dados já existentes")
        sql_lines.append(f"UPDATE {table_name} SET {mig_col} = {l_pref}_ide")
        sql_lines.append(f"FROM {lt}")
        sql_lines.append(f"WHERE upper(to_ascii(trim(cast({chave_dest} as text)))) = upper(to_ascii(trim(cast({chave_safe} as text))));\n")
        
        sql_lines.append(f"-- 2. Insere dados na tabela de domínio que ainda não existem")
        lt_cols = [c['col_dest'] for c in cols_lt]
        lt_selects = [c['safe_col'] for c in cols_lt]
        
        sql_lines.append(f"INSERT INTO {lt} ({', '.join(lt_cols)})")
        sql_lines.append(f"SELECT DISTINCT {', '.join(lt_selects)}")
        sql_lines.append(f"FROM {table_name}")
        sql_lines.append(f"WHERE {mig_col} IS NULL")
        sql_lines.append(f"  AND {chave_safe} IS NOT NULL")
        sql_lines.append(f"  AND {chave_safe} <> 'NULL'")
        sql_lines.append(f"  AND NOT EXISTS (")
        sql_lines.append(f"      SELECT 1 FROM {lt}")
        sql_lines.append(f"      WHERE upper(to_ascii(trim(cast({lt}.{chave_dest} as text)))) = upper(to_ascii(trim(cast({table_name}.{chave_safe} as text))))")
        sql_lines.append(f"  );\n")
        
        sql_lines.append(f"-- 3. Atualiza as chaves novamente captando os recém-inseridos")
        sql_lines.append(f"UPDATE {table_name} SET {mig_col} = {l_pref}_ide")
        sql_lines.append(f"FROM {lt}")
        sql_lines.append(f"WHERE upper(to_ascii(trim(cast({chave_dest} as text)))) = upper(to_ascii(trim(cast({chave_safe} as text))))")
        sql_lines.append(f"  AND {mig_col} IS NULL;\n")
        
    # Tabela Principal
    sql_lines.append(f"-- {'='*60}")
    sql_lines.append(f"-- TABELA PRINCIPAL: {main_tab}")
    sql_lines.append(f"-- {'='*60}\n")
    
    main_cols = tabelas_agrupadas[main_tab]
    first_col_dest = main_cols[0]['col_dest']
    first_safe_col = main_cols[0]['safe_col']
    
    insert_cols = [c['col_dest'] for c in main_cols]
    insert_vals = []
    
    for c in main_cols:
        dest = c['col_dest']
        safe = c['safe_col']
        if 'dta' in dest or 'data' in dest:
            insert_vals.append(f"NULLIF({safe}, 'NULL')::date")
        elif 'vlr' in dest or 'val' in dest:
            insert_vals.append(f"REPLACE(REPLACE({safe}, '.', ''), ',', '.')::numeric")
        else:
            insert_vals.append(f"NULLIF({safe}, 'NULL')")
    
    for lt in lookup_tabs:
        l_pref = get_table_prefix(lt)
        fk_name = f"{main_prefix}_fky_{l_pref}_ide"
        insert_cols.append(fk_name)
        insert_vals.append(f"mig_{l_pref}_ide")
        
    sql_lines.append(f"INSERT INTO {main_tab} (")
    sql_lines.append(f"    {', '.join(insert_cols)}")
    sql_lines.append(f")\nSELECT ")
    sql_lines.append(f"    {', '.join(insert_vals)}")
    sql_lines.append(f"FROM {table_name}")
    sql_lines.append(f"WHERE NOT EXISTS (")
    sql_lines.append(f"    SELECT 1 FROM {main_tab} m")
    sql_lines.append(f"    WHERE upper(to_ascii(trim(cast(m.{first_col_dest} as text)))) = upper(to_ascii(trim(cast({table_name}.{first_safe_col} as text))))")
    sql_lines.append(f");\n")
    
    sql_lines.append(f"-- Gravando o ID principal gerado de volta na staging (útil caso vá importar andamentos depois)")
    mig_main_col = f"mig_{main_prefix}_ide"
    sql_lines.append(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {mig_main_col} numeric;")
    sql_lines.append(f"UPDATE {table_name} SET {mig_main_col} = {main_prefix}_ide")
    sql_lines.append(f"FROM {main_tab}")
    sql_lines.append(f"WHERE upper(to_ascii(trim(cast({first_col_dest} as text)))) = upper(to_ascii(trim(cast({first_safe_col} as text))));\n")
    
    return "\n".join(sql_lines) + "\n"

def parse_mapping_dict(mapping_suggestions, original_cols, final_cols, memoria):
    mapeamento_validado = []
    
    for item in mapping_suggestions:
        orig_col = item["Coluna Original"]
        destino = item["Destino (tabela.coluna)"].strip()
        
        if not destino or destino.upper() == "PULAR":
            memoria[orig_col] = "PULAR"
            continue
            
        if "." not in destino:
            print(f"Aviso: O destino '{destino}' da coluna '{orig_col}' não está no formato tabela.coluna. Será ignorada.")
            memoria[orig_col] = "PULAR"
            continue
            
        memoria[orig_col] = destino
        tabela_destino, coluna_destino = destino.split(".", 1)
        
        safe_col = ""
        for orig, safe in zip(original_cols, final_cols):
            if orig == orig_col:
                safe_col = safe
                break
                
        if safe_col:
            mapeamento_validado.append({
                'orig_col': orig_col,
                'safe_col': safe_col,
                'tab_dest': tabela_destino.strip(),
                'col_dest': coluna_destino.strip()
            })
            
    return mapeamento_validado

def main():
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
    else:
        print("Uso: python gera_script.py <nome_da_planilha.xlsm>")
        return
        
    print("\n---------------------------------------------------------")
    print("MENU DE OPÇÕES")
    print("---------------------------------------------------------")
    print("1. Gerar apenas o arquivo de criação da tabela (Script de Staging)")
    print("2. Realizar o procedimento completo (Staging + Mapeamento Interativo + Final)")
    print("---------------------------------------------------------")
    
    opcao = input("Escolha a opção (1 ou 2): ").strip()
    if opcao not in ['1', '2']:
        print("Opção inválida. Encerrando.")
        return
        
    base_name = os.path.splitext(os.path.basename(file_name))[0]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    staging_dir = "scripts_staging"
    migracao_dir = "scripts_migracao"
    mapeamento_dir = "mapeamentos"
    
    os.makedirs(staging_dir, exist_ok=True)
    os.makedirs(migracao_dir, exist_ok=True)
    os.makedirs(mapeamento_dir, exist_ok=True)
    
    output_file = os.path.join(staging_dir, f'script_{base_name}_{timestamp}.sql')
    output_final = os.path.join(migracao_dir, f'migracao_final_{base_name}_{timestamp}.sql')
    txt_mapeamento = os.path.join(mapeamento_dir, f'mapeamento_{base_name}.txt')
    
    table_name = clean_col_name(base_name)
    if not table_name:
        table_name = 'tabela_importada'
    
    print(f"Lendo arquivo {file_name}...")
    try:
        df = pd.read_excel(file_name)
    except Exception as e:
        print(f"Erro ao ler excel: {e}")
        return
        
    print(f"Planilha lida com {len(df)} linhas e {len(df.columns)} colunas.")
    
    # Tratamento automático de codificação falha utf-8 para latin-1
    df = fix_dataframe_mojibake(df)
    
    original_cols, final_cols = process_dataframe_columns(df)
    memoria = load_memory()
    
    print(f"\nGerando script de staging (tabela temporária '{table_name}')...")
    
    staging_sql_content = generate_staging_sql(df, table_name, final_cols)
    
    with open(output_file, 'w', encoding='latin-1', errors='replace') as f:
        f.write(staging_sql_content)
        
    print(f"[OK] Script de Staging gerado com sucesso em '{output_file}'!")
    
    if opcao == '1':
        print("\nOpção 1 concluída: Apenas o script da tabela bruta foi gerado.")
        return

    if not os.path.exists(txt_mapeamento):
        suggestions = generate_mapping_suggestions(original_cols, memoria)
        with open(txt_mapeamento, 'w', encoding='utf-8') as f:
            f.write(f"# Mapeamento de colunas para '{file_name}'\n")
            f.write("# Edite o lado direito do sinal de igual (=) indicando a tabela.coluna de destino no seu sistema.\n")
            f.write("# Exemplo: RECLAMANTE = p_processos.pro_par_pri\n")
            f.write("# Se não quiser migrar a coluna, deixe em branco ou escreva PULAR\n\n")
            for sug in suggestions:
                f.write(f"{sug['Coluna Original']} = {sug['Destino (tabela.coluna)']}\n")
        
        print("\n" + "="*70)
        print("ATENÇÃO: Mapeamento interativo via arquivo")
        print("="*70)
        print(f"O arquivo editável '{txt_mapeamento}' foi criado.")
        print("Por favor, abra este arquivo, edite-o apontando o destino correto (tabela.coluna) e rode este script novamente.")
        print(f"Comando para gerar o final: python gera_script.py {file_name}")
        return

    print(f"\nLendo mapeamentos do arquivo '{txt_mapeamento}'...")
    with open(txt_mapeamento, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    read_suggestions = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' in line:
            left, right = line.split('=', 1)
            read_suggestions.append({
                "Coluna Original": left.strip(),
                "Destino (tabela.coluna)": right.strip()
            })
            
    mapeamento_validado = parse_mapping_dict(read_suggestions, original_cols, final_cols, memoria)
    save_memory(memoria)
    print("[OK] Mapeamento lido e memória atualizada!")
    
    if mapeamento_validado:
        print("\nGerando script final dinâmico em PL/pgSQL (Lookup + Insert)...")
        final_sql_content = generate_final_sql(mapeamento_validado, table_name)
        with open(output_final, 'w', encoding='latin-1', errors='replace') as f:
            f.write(final_sql_content)
        print(f"[OK] Script dinâmico em lote gerado com sucesso em '{output_final}'!")
    else:
        print("\nNenhuma coluna válida foi mapeada no arquivo .txt. O script final dinâmico não foi gerado.")

if __name__ == '__main__':
    main()

