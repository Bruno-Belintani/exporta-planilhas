import streamlit as st
import pandas as pd
import os
import tkinter as tk
from tkinter import filedialog
from datetime import datetime
import streamlit.components.v1 as components
from gera_script import (
    process_dataframe_columns,
    generate_staging_sql,
    generate_mapping_suggestions,
    generate_final_sql,
    parse_mapping_dict,
    clean_col_name,
    load_memory,
    save_memory
)

# Configuração da Página
st.set_page_config(
    page_title="LM Exportador", 
    page_icon="logo.png", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- UTILS ---
def format_size(bytes):
    for unit in ['B', 'KB', 'MB']:
        if bytes < 1024:
            return f"{bytes:.1f} {unit}"
        bytes /= 1024
    return f"{bytes:.1f} GB"

# --- INITIAL STATE ---
if "step" not in st.session_state:
    st.session_state.step = 1
if "preview_rows" not in st.session_state:
    st.session_state.preview_rows = 20
if "reset_counter" not in st.session_state:
    st.session_state.reset_counter = 0

# --- CUSTOM CSS (JurisData Obsidian v2 - Modern Template Adaptation) ---
def inject_custom_css():
    st.markdown("""
        <script src="https://cdn.tailwindcss.com?plugins=forms,container-queries"></script>
        <link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=Inter:wght@400;500;600&display=swap" rel="stylesheet"/>
        <link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:wght,FILL@100..700,0..1&display=swap" rel="stylesheet"/>
        
        <style>
        /* Base Reset & Fonts */
        html, body, [data-testid="stAppViewContainer"] {
            font-family: 'Inter', sans-serif !important;
            background-color: #f8f9fa !important;
            color: #191c1d;
        }
        
        h1, h2, h3, h4, .font-headline {
            font-family: 'Manrope', sans-serif !important;
        }

        /* Hide Streamlit Elements */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        
        /* Modern Rail Design */
        [data-testid="stSidebar"] {
            background-color: #f8fafc !important;
            border-right: 1px solid #e2e8f0;
            width: 260px !important;
        }
        
        [data-testid="stSidebarNav"] {display: none;}
        
        .nav-item {
            display: flex;
            align-items: center;
            gap: 12px;
            padding: 12px 16px;
            border-radius: 8px;
            color: #64748b;
            text-decoration: none !important;
            font-weight: 500;
            margin-bottom: 4px;
        }
        
        .nav-item.active {
            color: #185FA5;
            background-color: white;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            font-weight: 700;
            border-right: 3px solid #185FA5;
        }

        /* Top Bar Wrapper Modernizado */
        .top-bar {
            height: 64px;
            background-color: #001944;
            border-bottom: 1px solid rgba(255,255,255,0.1);
            padding: 0 2rem;
            display: flex;
            align-items: center;
            justify-content: space-between;
            position: sticky;
            top: 0;
            z-index: 999;
            margin: -6rem -4rem 2rem -4rem;
        }

        /* Bento Cards & Design Elements */
        [data-testid="stVerticalBlockBorderWrapper"] {
            border: none !important;
            background-color: #ffffff !important;
            border-radius: 12px !important;
            padding: 2rem !important;
            box-shadow: 0 1px 3px 0 rgba(0,0,0,0.08) !important;
        }
        
        .material-symbols-outlined {
            font-variation-settings: 'FILL' 0, 'wght' 500, 'GRAD' 0, 'opsz' 24;
            vertical-align: middle;
        }

        /* Modelo Moderno Components */
        .drop-zone {
            border: 2px dashed #cbd5e1;
            border-radius: 12px;
            padding: 3rem;
            text-align: center;
            background-color: #ffffff;
            transition: all 0.2s;
        }
        
        .drop-zone:hover {
            border-color: #475569;
            background-color: #f8fafc;
        }

        .success-card {
            background-color: #ecfdf5;
            border: 1px solid #d1fae5;
            border-radius: 12px;
            padding: 1.5rem;
            display: flex;
            align-items: center;
            gap: 1.5rem;
        }

        /* Forçar Cor Primária dos Botões Streamlit (Incluindo Download) */
        div.stButton > button[kind="primary"], div.stDownloadButton > button {
            background-color: #185FA5 !important;
            border-color: #185FA5 !important;
            color: white !important;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
        }

        /* Ícone Profissional no Botão de Download */
        div.stDownloadButton > button::before {
            content: 'download';
            font-family: 'Material Symbols Outlined';
            font-size: 20px;
            color: white;
        }
        
        div.stButton > button[kind="primary"]:hover, div.stDownloadButton > button:hover {
            background-color: #134d85 !important;
            border-color: #134d85 !important;
        }

        .btn-primary-modern {
            background-color: #185FA5;
            color: white !important;
            padding: 10px 24px;
            border-radius: 8px;
            font-weight: 700;
            transition: all 0.2s;
        }
        
        /* Expander Customization */
        .stExpander {
            border: 1px solid #e1e3e4 !important;
            border-radius: 8px !important;
            background-color: #f8fafc !important;
            margin-bottom: 1.5rem !important;
        }
        </style>
    """, unsafe_allow_html=True)

def draw_sidebar():
    with st.sidebar:
        # Logo e Título
        st.markdown(f"""
        <div style="padding: 1rem 0;">
            <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 2.5rem; padding: 0 0.5rem;">
                <div style="background-color: #185FA5; width: 32px; height: 32px; border-radius: 8px; display: flex; align-items: center; justify-content: center;">
                    <span class="material-symbols-outlined" style="color: white; font-size: 20px;">database</span>
                </div>
                <div>
                    <div style="font-weight: 800; color: #1e293b; font-size: 1rem; line-height: 1;">LM Exportador</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Itens de Navegação Controlados pelo Estado
        steps = [
            ("cloud_upload", "Upload", 1),
            ("visibility", "Preview", 3),
            ("database", "Gerar SQL", 4)
        ]
        
        for icon, label, step_num in steps:
            is_active = st.session_state.step == step_num
            active_class = "active" if is_active else ""
            
            # Usando uma única string de markdown para evitar que o Streamlit quebre a renderização HTML
            st.markdown(f"""
                <div class="nav-item {active_class}">
                    <span class="material-symbols-outlined">{icon}</span>
                    <span style="font-size: 14px; font-weight: {('700' if is_active else '500')};">{label}</span>
                </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<br><br>", unsafe_allow_html=True)

def draw_top_bar():
    st.markdown("""
        <div class="top-bar">
            <div style="display: flex; align-items: center; gap: 1.5rem;">
                <span style="font-size: 1.125rem; font-weight: 900; color: white;">LM Exportador</span>
            </div>
            <div style="display: flex; gap: 1rem; color: rgba(255,255,255,0.8);">
                <span class="material-symbols-outlined" style="cursor: pointer;">notifications</span>
                <span class="material-symbols-outlined" style="cursor: pointer;">help_outline</span>
            </div>
        </div>
    """, unsafe_allow_html=True)

def render_info_panel():
    # Iniciando a montagem do HTML sem indentação extra para evitar que o Streamlit interprete como código
    html_content = """<div style="background-color: #f3f4f5; padding: 2rem; border-radius: 12px; margin-bottom: 2rem;">
<h3 style="font-size: 0.75rem; font-weight: 900; color: #001944; letter-spacing: 0.1em; text-transform: uppercase; border-bottom: 1px solid #e1e3e4; padding-bottom: 0.75rem; margin-bottom: 1.5rem;">STATUS DO PROCESSAMENTO</h3>"""
    
    if "df" in st.session_state:
        html_content += f"""<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1.25rem;">
<div style="display: flex; align-items: center; gap: 12px; font-size: 14px; color: #454652;">
<span class="material-symbols-outlined" style="color: #64748b; font-size: 20px;">format_list_bulleted</span>
<span>Total Linhas</span>
</div>
<span style="font-size: 1.125rem; font-weight: 800; color: #191c1d;">{len(st.session_state.df):,}</span>
</div>
<div style="display: flex; justify-content: space-between; align-items: center;">
<div style="display: flex; align-items: center; gap: 12px; font-size: 14px; color: #454652;">
<span class="material-symbols-outlined" style="color: #64748b; font-size: 20px;">view_column</span>
<span>Colunas</span>
</div>
<span style="font-size: 1.125rem; font-weight: 800; color: #191c1d;">{len(st.session_state.df.columns)}</span>
</div>"""
    else:
        html_content += '<p style="font-size: 13px; color: #64748b; font-style: italic;">Aguardando carga de arquivo...</p>'
    
    html_content += '</div>'
    st.markdown(html_content, unsafe_allow_html=True)

def save_file_native(content, default_filename):
    """Abre um diálogo nativo do Windows para salvar o arquivo, 
    contornando restrições de download do webview."""
    try:
        # Inicializa o Tkinter de forma oculta
        root = tk.Tk()
        root.withdraw()
        # Garante que a janela de salvamento apareça na frente do aplicativo
        root.attributes("-topmost", True)
        
        # Abre o diálogo de "Salvar Como"
        file_path = filedialog.asksaveasfilename(
            defaultextension=".sql",
            initialfile=default_filename,
            filetypes=[("Arquivos SQL", "*.sql"), ("Todos os arquivos", "*.*")],
            title="Escolha onde salvar o seu Script SQL"
        )
        root.destroy()
        
        if file_path:
            # Gravamos o arquivo diretamente no disco
            with open(file_path, "w", encoding="latin-1") as f:
                f.write(content)
            return file_path
    except Exception as e:
        st.error(f"Erro ao abrir diálogo de salvamento: {e}")
    return None

# --- CALLBACKS DE SINCRONIZAÇÃO ROBUSTA ---

def sync_header_edits():
    """Processa mudanças nos nomes das colunas via mapeador."""
    key = f"col_editor_{st.session_state.reset_counter}"
    if key in st.session_state:
        changes = st.session_state[key]
        # Pegamos o DataFrame original de colunas que passamos para o editor
        current_cols = list(st.session_state.df.columns)
        
        # Aplicamos apenas as edições feitas no 'edited_rows'
        edited = changes.get("edited_rows", {})
        for idx, val in edited.items():
            if "Original" in val:
                current_cols[idx] = val["Original"]
                
        # Atualizamos o DataFrame principal com os novos cabeçalhos
        st.session_state.df.columns = current_cols
        # Recalcula colunas finais e SQL
        _, nf_cols = process_dataframe_columns(st.session_state.df)
        st.session_state.final_cols = nf_cols
        st.session_state.staging_sql = generate_staging_sql(
            st.session_state.df, 
            st.session_state.table_name, 
            nf_cols
        )

def sync_df_edits():
    """Processa mudanças de dados na planilha principal via deltas."""
    key = f"editor_{st.session_state.reset_counter}"
    if key in st.session_state:
        changes = st.session_state[key]
        edited = changes.get("edited_rows", {})
        
        # Aplicamos cada mudança capturada pelo widget ao Session State de forma direta
        for row_idx, cols in edited.items():
            for col_name, val in cols.items():
                st.session_state.df.iloc[row_idx, st.session_state.df.columns.get_loc(col_name)] = val
        
        # Recalcula o SQL com os novos dados editados
        st.session_state.staging_sql = generate_staging_sql(
            st.session_state.df, 
            st.session_state.table_name, 
            st.session_state.final_cols
        )

def render_breadcrumb(step):
    # Passos: Upload (1), Preview (3), SQL (4)
    steps_list = [1, 3, 4]
    current_idx = steps_list.index(step) + 1
    progress_pct = (current_idx / len(steps_list)) * 100
    
    # Barra de Progresso Minimalista (Stepper Visual)
    st.markdown(f"""
        <div style="width: 100%; height: 4px; background-color: #e2e8f0; border-radius: 2px; margin-bottom: 1.5rem; overflow: hidden;">
            <div style="width: {progress_pct}%; height: 100%; background-color: #185FA5; transition: width 0.4s ease-in-out;"></div>
        </div>
    """, unsafe_allow_html=True)

    steps = [("Upload", 1), ("Preview", 3), ("SQL", 4)]
    breadcrumb_html = '<div style="display: flex; gap: 12px; align-items: center; margin-bottom: 2rem; font-family: Inter, sans-serif; font-size: 13px; font-weight: 500; color: #64748b;">'
    
    for i, (label, s_num) in enumerate(steps):
        is_active = step == s_num
        color = "#185FA5" if is_active else "#94a3b8"
        weight = "700" if is_active else "500"
        
        breadcrumb_html += f'<span style="color: {color}; font-weight: {weight};">{label}</span>'
        if i < len(steps) - 1:
            breadcrumb_html += '<span style="color: #cbd5e1;">&rarr;</span>'
            
    breadcrumb_html += '</div>'
    st.markdown(breadcrumb_html, unsafe_allow_html=True)

# Injeção de CSS e Sidebar
inject_custom_css()
draw_sidebar()
draw_top_bar()

# --- APP FLOW ---

# STEP 1: UPLOAD
if st.session_state.step == 1:
    render_breadcrumb(1)
    
    col_main, col_side = st.columns([2, 1], gap="large")
    with col_main:
        with st.container(border=True):
            if "uploaded_filename" not in st.session_state:
                st.markdown("""
                    <div class="drop-zone">
                        <div style="width: 4rem; height: 4rem; margin-left: auto; margin-right: auto; margin-bottom: 1rem;">
                            <span class="material-symbols-outlined" style="font-size: 3rem; color: #185FA5;">upload_file</span>
                        </div>
                        <p style="font-weight: 600; font-size: 1.1rem;">Arraste seu arquivo aqui ou clique</p>
                        <p style="font-size: 0.8rem; color: #64748b; margin-bottom: 1.5rem;">Máx: 200MB (.xlsx, .csv)</p>
                    </div>
                """, unsafe_allow_html=True)
                uploaded_file = st.file_uploader("Upload", type=["xlsx", "xlsm"], label_visibility="collapsed")
                if uploaded_file:
                    with st.spinner("Analisando estrutura..."):
                        df = pd.read_excel(uploaded_file)
                        base = os.path.splitext(uploaded_file.name)[0]
                        table = clean_col_name(base)
                        _, f_cols = process_dataframe_columns(df)
                        
                        st.session_state.df = df
                        st.session_state.df_original = df.copy() # Backup de Origem
                        st.session_state.table_name = table
                        st.session_state.uploaded_filename = uploaded_file.name
                        st.session_state.final_cols = f_cols
                        st.session_state.staging_sql = generate_staging_sql(df, table, f_cols)
                        st.session_state.reset_counter = 0 # Inicia contador de reset
                        st.rerun()
            else:
                st.markdown(f"""
                    <div class="success-card">
                        <div style="width: 3.5rem; height: 3.5rem; background-color: #d1fae5; border-radius: 8px; display: flex; align-items: center; justify-content: center;">
                            <span class="material-symbols-outlined" style="color: #059669; font-size: 2rem;">description</span>
                        </div>
                        <div style="flex: 1;">
                            <h4 style="font-size: 14px; font-weight: 700; color: #064e3b; margin: 0;">{st.session_state.uploaded_filename}</h4>
                            <p style="font-size: 12px; color: #059669; margin: 0;">Carregado com sucesso</p>
                        </div>
                    </div>
                """, unsafe_allow_html=True)
                st.markdown("<br>", unsafe_allow_html=True)
                btn_col1, btn_col2 = st.columns([1, 1.5])
                with btn_col1:
                    if st.button("Trocar Arquivo", use_container_width=True):
                        del st.session_state.uploaded_filename
                        st.rerun()
                with btn_col2:
                    if st.button("Próxima Etapa →", type="primary", use_container_width=True):
                        st.session_state.step = 3
                        st.rerun()

    with col_side:
        render_info_panel()

# STEP 3: PREVIEW
elif st.session_state.step == 3:
    render_breadcrumb(3)
    
    col_main, col_side = st.columns([2.5, 1], gap="large")
    with col_main:
        with st.container(border=True):
            # NOVO: Ferramenta de Mapeamento de Cabeçalhos
            with st.expander("Mapear Cabeçalhos (Renomear Colunas)", expanded=False):
                st.markdown("<p style='font-size: 13px; color: #64748b;'>Edite os nomes para renomear os cabeçalhos da planilha.</p>", unsafe_allow_html=True)
                cols_mapping = pd.DataFrame({
                    "Original": st.session_state.df.columns
                })
                # Usar callback para evitar conflito de estado
                st.data_editor(
                    cols_mapping, 
                    use_container_width=True, 
                    hide_index=True,
                    key=f"col_editor_{st.session_state.reset_counter}",
                    on_change=sync_header_edits
                )

            st.markdown("<br>", unsafe_allow_html=True)

            # Planilha Principal com Dynamic Key e Callback Sync
            st.data_editor(
                st.session_state.df, 
                use_container_width=True, 
                height=350,
                key=f"editor_{st.session_state.reset_counter}",
                on_change=sync_df_edits
            )
            
            nav_col1, nav_col2, nav_col3 = st.columns([1, 1, 1.5])
            with nav_col1:
                if st.button("⬅ Voltar", use_container_width=True): 
                    st.session_state.step = 1
                    st.rerun()
            with nav_col2:
                # Reset Robusto: Volta ao original e muda a key
                if st.button("Resetar Edições", use_container_width=True):
                    st.session_state.df = st.session_state.df_original.copy()
                    st.session_state.reset_counter += 1
                    # Recalcula SQL
                    _, r_cols = process_dataframe_columns(st.session_state.df)
                    st.session_state.final_cols = r_cols
                    st.session_state.staging_sql = generate_staging_sql(
                        st.session_state.df, 
                        st.session_state.table_name, 
                        r_cols
                    )
                    st.rerun()
            with nav_col3:
                if st.button("Gerar SQL ➔", type="primary", use_container_width=True): 
                    st.session_state.step = 4
                    st.rerun()
    with col_side:
        render_info_panel()

# STEP 4: GERAÇÃO
elif st.session_state.step == 4:
    render_breadcrumb(4)
    
    col_main, col_side = st.columns([2.5, 1], gap="large")
    with col_main:
        with st.container(border=True):
            new_table = st.text_input("Renomear Tabela de Staging", value=st.session_state.table_name)
            if new_table != st.session_state.table_name:
                st.session_state.table_name = new_table
                st.session_state.staging_sql = generate_staging_sql(st.session_state.df, new_table, st.session_state.final_cols)
                st.rerun()
            
            st.markdown('<h3 style="font-size: 1rem; font-weight: 700; margin-bottom: 1rem;">Preview do SQL</h3>', unsafe_allow_html=True)
            
            # Editor com Cópia Via Iframe Seguro (st.components.v1.html)
            code_html = f"""
            <html>
                <head>
                    <link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:wght,FILL@100..700,0..1&display=swap" rel="stylesheet"/>
                    <style>
                        body {{ margin: 0; background-color: transparent; font-family: sans-serif; }}
                        .container {{ position: relative; background-color: #000; border-radius: 12px; overflow: hidden; }}
                        .editor {{ 
                            width: 100%; height: 320px; padding: 1.5rem; 
                            font-family: 'Courier New', monospace; font-size: 14px; 
                            background-color: #000; color: #4ade80; border: none; 
                            resize: none; outline: none; line-height: 1.5;
                        }}
                        .copy-btn {{
                            position: absolute; top: 12px; right: 12px;
                            background: rgba(255,255,255,0.1); border: 1px solid rgba(255,255,255,0.2);
                            color: #e2e8f0; border-radius: 6px; padding: 8px; cursor: pointer;
                            display: flex; align-items: center; justify-content: center;
                            transition: all 0.2s; z-index: 100;
                        }}
                        .copy-btn:hover {{ background: rgba(255,255,255,0.2); color: #fff; }}
                        .material-symbols-outlined {{ font-size: 20px; }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <button class="copy-btn" onclick="copyText()">
                            <span class="material-symbols-outlined" id="icon">content_copy</span>
                        </button>
                        <textarea id="sql" class="editor" readonly>{st.session_state.staging_sql}</textarea>
                    </div>
                    <script>
                        function copyText() {{
                            const text = document.getElementById('sql').value;
                            navigator.clipboard.writeText(text).then(() => {{
                                const icon = document.getElementById('icon');
                                icon.innerText = 'check';
                                icon.style.color = '#4ade80';
                                setTimeout(() => {{
                                    icon.innerText = 'content_copy';
                                    icon.style.color = '#e2e8f0';
                                }}, 2000);
                            }});
                        }}
                    </script>
                </body>
            </html>
            """
            components.html(code_html, height=330)
            
            st.markdown("<br>", unsafe_allow_html=True)
            col_d1, col_d2 = st.columns([1, 3])
            with col_d1:
                if st.button("⬅ Voltar", use_container_width=True):
                    st.session_state.step = 3
                    st.rerun()
            with col_d2:
                if st.button("Salvar Script SQL", use_container_width=True, type="primary", icon=":material/save:"):
                    default_name = f"script_{st.session_state.table_name}.sql"
                    caminho = save_file_native(st.session_state.staging_sql, default_name)
                    if caminho:
                        st.success(f"✅ Arquivo salvo com sucesso em:\n{caminho}")

    with col_side:
        render_info_panel()
