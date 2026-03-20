import pandas as pd
import numpy as np
import re
import math
import sys
import os
from datetime import datetime

def clean_col_name(name):
    if pd.isna(name):
        return "coluna_desconhecida"
    # Basic transliteration for common accents to make column names safe
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
    # prevent leading numbers
    if re.match(r'^[0-9]', name):
        name = 'c_' + name
    return name

def sanitize_string_latin1(val):
    if pd.isna(val):
        return 'NULL'
    val = str(val)
    # Common replacements for UTF-8 chars not in Latin1
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
    
    # encode to latin1 ignoring the rest or replacing
    val_bytes = val.encode('latin-1', 'replace')
    val = val_bytes.decode('latin-1')
    
    # escape single quotes for SQL
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

def main():
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
    else:
        print("Uso: python gera_script.py <nome_da_planilha.xlsm>")
        return
        
    base_name = os.path.splitext(os.path.basename(file_name))[0]
    output_file = f'script_{base_name}.sql'
    
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
    
    # Ensure unique column names
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
    
    # Build CREATE TABLE
    create_table_sql = f"CREATE TABLE {table_name} (\n"
    col_defs = []
    for col in final_cols:
        col_defs.append(f"    {col} TEXT")  # Using TEXT for all columns to avoid type errors
    create_table_sql += ",\n".join(col_defs)
    create_table_sql += "\n);\n\n"
    
    # Build INSERT queries
    print("Gerando scripts de INSERT...")
    inserts = []
    cols_joined = ", ".join(final_cols)
    
    with open(output_file, 'w', encoding='latin-1', errors='replace') as f:
        f.write("-- Script gerado automaticamente\n")
        f.write(create_table_sql)
        f.write("\n")
        
        for idx, row in df.iterrows():
            values = [format_value(val) for val in row]
            vals_joined = ", ".join(values)
            insert_sql = f"INSERT INTO {table_name} ({cols_joined}) VALUES ({vals_joined});\n"
            f.write(insert_sql)
            
            if (idx + 1) % 1000 == 0:
                print(f"Geradas {idx + 1} linhas...")
                
    print(f"Script gerado com sucesso em '{output_file}'!")

if __name__ == '__main__':
    main()
