import pandas as pd
import numpy as np
import re
import math
import sys
import os
import json
from datetime import datetime

MEMORIA_FILE = 'memoria_mapeamento.json'

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
    
    # Clean columns
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
    memoria = load_memory()
    
    # ---------------------------------------------------------
    # STAGING GENERATION
    # ---------------------------------------------------------
    print(f"\nGerando script de staging (tabela temporária '{table_name}')...")
    create_table_sql = f"CREATE TABLE {table_name} (\n"
    col_defs = []
    for col in final_cols:
        col_defs.append(f"    {col} TEXT")  
    create_table_sql += ",\n".join(col_defs)
    create_table_sql += "\n);\n\n"
    
    cols_joined = ", ".join(final_cols)
    with open(output_file, 'w', encoding='latin-1', errors='replace') as f:
        f.write("-- Script gerado automaticamente (STAGING)\n")
        f.write(create_table_sql)
        f.write("\n")
        
        for idx, row in df.iterrows():
            values = [format_value(val) for val in row]
            vals_joined = ", ".join(values)
            insert_sql = f"INSERT INTO {table_name} ({cols_joined}) VALUES ({vals_joined});\n"
            f.write(insert_sql)
            
            if (idx + 1) % 1000 == 0:
                print(f"Geradas {idx + 1} linhas de staging...")
    print(f"[OK] Script de Staging gerado com sucesso em '{output_file}'!")
    
    if opcao == '1':
        print("\nOpção 1 concluída: Apenas o script da tabela bruta foi gerado.")
        return

    # ---------------------------------------------------------
    # FILE MAPPING LOGIC
    # ---------------------------------------------------------
    if not os.path.exists(txt_mapeamento):
        with open(txt_mapeamento, 'w', encoding='utf-8') as f:
            f.write(f"# Mapeamento de colunas para '{file_name}'\n")
            f.write("# Edite o lado direito do sinal de igual (=) indicando a tabela.coluna de destino no seu sistema.\n")
            f.write("# Exemplo: RECLAMANTE = p_processos.pro_par_pri\n")
            f.write("# Se não quiser migrar a coluna, deixe em branco ou escreva PULAR\n\n")
            
            for orig_col in original_cols:
                if str(orig_col) in memoria:
                    sugestao = memoria[str(orig_col)]
                else:
                    sugestao = heuristic_guess(orig_col)
                f.write(f"{orig_col} = {sugestao}\n")
        
        print("\n" + "="*70)
        print("ATENÇÃO: Mapeamento interativo via arquivo")
        print("="*70)
        print(f"O arquivo editável '{txt_mapeamento}' foi criado.")
        print("Por favor, abra este arquivo, edite-o apontando o destino correto (tabela.coluna) e rode este script novamente.")
        print(f"Comando para gerar o final: python gera_script.py {file_name}")
        return

    # If the file exists, parse it and update memory / generate final sql
    mapeamento_validado = []
    print(f"\nLendo mapeamentos do arquivo '{txt_mapeamento}'...")
    
    with open(txt_mapeamento, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
            
        if '=' in line:
            left, right = line.split('=', 1)
            orig_col = left.strip()
            destino = right.strip()
            
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

    save_memory(memoria)
    print("[OK] Mapeamento lido e memória atualizada!")
    
    if mapeamento_validado:
        print("\nGerando script final dinâmico em PL/pgSQL (Lookup + Insert)...")
        tabelas_agrupadas = {}
        for map_item in mapeamento_validado:
            tab = map_item['tab_dest']
            if tab not in tabelas_agrupadas:
                tabelas_agrupadas[tab] = []
            tabelas_agrupadas[tab].append(map_item)
            
        with open(output_final, 'w', encoding='latin-1', errors='replace') as f:
            f.write("-- Script Final de Migração Dinâmico (ANSI SQL Massivo)\n")
            f.write(f"-- Fonte de staging: {table_name}\n\n")
            
            # Determinar a tabela principal (a que tiver mais colunas mapeadas)
            main_tab = max(tabelas_agrupadas.keys(), key=lambda t: len(tabelas_agrupadas[t]))
            main_prefix = get_table_prefix(main_tab)
            lookup_tabs = [t for t in tabelas_agrupadas.keys() if t != main_tab]
            
            # Subtabelas / Lookups (Domínios)
            for lt in lookup_tabs:
                l_pref = get_table_prefix(lt)
                cols_lt = tabelas_agrupadas[lt]
                
                # Assume a primeira coluna mapeada como a chave de busca (ex: var_dsc)
                chave_dest = cols_lt[0]['col_dest']
                chave_safe = cols_lt[0]['safe_col']
                mig_col = f"mig_{l_pref}_ide"
                
                f.write(f"-- {'='*60}\n")
                f.write(f"-- TABELA DE DOMÍNIO: {lt}\n")
                f.write(f"-- {'='*60}\n\n")
                
                f.write(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {mig_col} numeric;\n\n")
                
                f.write(f"-- 1. Faz de-para com os dados já existentes\n")
                f.write(f"UPDATE {table_name} SET {mig_col} = {l_pref}_ide\n")
                f.write(f"FROM {lt}\n")
                f.write(f"WHERE upper(to_ascii(trim(cast({chave_dest} as text)))) = upper(to_ascii(trim(cast({chave_safe} as text))));\n\n")
                
                f.write(f"-- 2. Insere dados na tabela de domínio que ainda não existem\n")
                lt_cols = [c['col_dest'] for c in cols_lt]
                lt_selects = [c['safe_col'] for c in cols_lt]
                
                f.write(f"INSERT INTO {lt} ({', '.join(lt_cols)})\n")
                f.write(f"SELECT DISTINCT {', '.join(lt_selects)}\n")
                f.write(f"FROM {table_name}\n")
                f.write(f"WHERE {mig_col} IS NULL\n")
                f.write(f"  AND {chave_safe} IS NOT NULL\n")
                f.write(f"  AND {chave_safe} <> 'NULL'\n")
                f.write(f"  AND NOT EXISTS (\n")
                f.write(f"      SELECT 1 FROM {lt}\n")
                f.write(f"      WHERE upper(to_ascii(trim(cast({lt}.{chave_dest} as text)))) = upper(to_ascii(trim(cast({table_name}.{chave_safe} as text))))\n")
                f.write(f"  );\n\n")
                
                f.write(f"-- 3. Atualiza as chaves novamente captando os recém-inseridos\n")
                f.write(f"UPDATE {table_name} SET {mig_col} = {l_pref}_ide\n")
                f.write(f"FROM {lt}\n")
                f.write(f"WHERE upper(to_ascii(trim(cast({chave_dest} as text)))) = upper(to_ascii(trim(cast({chave_safe} as text))))\n")
                f.write(f"  AND {mig_col} IS NULL;\n\n")
                
            # Tabela Principal
            f.write(f"-- {'='*60}\n")
            f.write(f"-- TABELA PRINCIPAL: {main_tab}\n")
            f.write(f"-- {'='*60}\n\n")
            
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
                
            f.write(f"INSERT INTO {main_tab} (\n")
            f.write(f"    {', '.join(insert_cols)}\n")
            f.write(f")\nSELECT \n")
            f.write(f"    {', '.join(insert_vals)}\n")
            f.write(f"FROM {table_name}\n")
            f.write(f"WHERE NOT EXISTS (\n")
            f.write(f"    SELECT 1 FROM {main_tab} m\n")
            f.write(f"    WHERE upper(to_ascii(trim(cast(m.{first_col_dest} as text)))) = upper(to_ascii(trim(cast({table_name}.{first_safe_col} as text))))\n")
            f.write(f");\n\n")
            
            f.write(f"-- Gravando o ID principal gerado de volta na staging (útil caso vá importar andamentos depois)\n")
            mig_main_col = f"mig_{main_prefix}_ide"
            f.write(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {mig_main_col} numeric;\n")
            f.write(f"UPDATE {table_name} SET {mig_main_col} = {main_prefix}_ide\n")
            f.write(f"FROM {main_tab}\n")
            f.write(f"WHERE upper(to_ascii(trim(cast({first_col_dest} as text)))) = upper(to_ascii(trim(cast({first_safe_col} as text))));\n\n")
            
        print(f"[OK] Script dinâmico em lote gerado com sucesso em '{output_final}'!")
    else:
        print("\nNenhuma coluna válida foi mapeada no arquivo .txt. O script final dinâmico não foi gerado.")

if __name__ == '__main__':
    main()
