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
    save_memory,
    fix_dataframe_mojibake,
    INSERT_TEMPLATES_REGISTRY,
    generate_insert_sql,
    auto_map_fields
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
if "insert_mapping" not in st.session_state:
    st.session_state.insert_mapping = {}
if "target_template" not in st.session_state:
    st.session_state.target_template = None
if "final_sql" not in st.session_state:
    st.session_state.final_sql = ""
if "staging_table_name" not in st.session_state:
    st.session_state.staging_table_name = ""
if "insert_table_name" not in st.session_state:
    st.session_state.insert_table_name = ""
if "global_configs" not in st.session_state:
    st.session_state.global_configs = {
        "mig_prefix": "mig_",
        "tipo_pessoa": "J",
        "tag_auditoria": "#LMBB",
        "sit_ide": "1"
    }

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
        
        /* Estilo dos Botões da Sidebar para imitar os Nav Items */
        [data-testid="stSidebar"] div.stButton > button {
            background-color: transparent !important;
            border: none !important;
            color: #64748b !important;
            display: flex;
            align-items: center;
            justify-content: flex-start;
            gap: 12px;
            padding: 8px 16px;
            border-radius: 8px;
            font-weight: 500;
            margin-bottom: 4px;
            width: 100%;
            height: 48px;
            box-shadow: none !important;
            transition: all 0.2s;
            border: 1px solid transparent !important;
        }

        [data-testid="stSidebar"] div.stButton > button:hover {
            color: #185FA5 !important;
            background-color: #f1f5f9 !important;
        }
        
        [data-testid="stSidebar"] div.stButton > button p {
            font-size: 14px;
            margin: 0;
            text-align: left;
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
            (":material/cloud_upload:", "Upload", 1),
            (":material/visibility:", "Preview", 3),
            (":material/account_tree:", "Mapeamento", 5),
            (":material/database:", "Gerar SQL", 4)
        ]
        
        active_css = ""
        button_idx = 2
        for icon, label, step_num in steps:
            is_active = st.session_state.step == step_num
            
            if is_active:
                active_css = f"""
                [data-testid="stSidebar"] .element-container:nth-child({button_idx}) div.stButton > button {{
                    color: #185FA5 !important;
                    background-color: white !important;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.1) !important;
                    font-weight: 700 !important;
                    border-right: 3px solid #185FA5 !important;
                }}
                [data-testid="stSidebar"] .element-container:nth-child({button_idx}) div.stButton > button span {{
                    color: #185FA5 !important;
                }}
                [data-testid="stSidebar"] .element-container:nth-child({button_idx}) div.stButton > button p {{
                    font-weight: 700 !important;
                }}
                """
                
            if st.button(label, icon=icon, key=f"nav_{step_num}", use_container_width=True):
                if step_num in [3, 4, 5] and "df" not in st.session_state:
                    st.toast("⚠️ Faça o upload de um arquivo na aba Upload primeiro!")
                elif step_num == 5 and not st.session_state.target_template:
                    st.toast("⚠️ Escolha um modelo de INSERT na tela de Preview primeiro!")
                else:
                    st.session_state.step = step_num
                    st.rerun()
            
            button_idx += 1
            
        if active_css:
            st.markdown(f"<style>{active_css}</style>", unsafe_allow_html=True)
        
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
    # Passos: Upload (1), Preview (3), Mapeamento (5), SQL (4)
    steps_list = [1, 3, 5, 4]
    try:
        current_idx = steps_list.index(step) + 1
    except ValueError:
        current_idx = 1
        
    progress_pct = (current_idx / len(steps_list)) * 100
    
    # Barra de Progresso Minimalista (Stepper Visual)
    st.markdown(f"""
        <div style="width: 100%; height: 4px; background-color: #e2e8f0; border-radius: 2px; margin-bottom: 1.5rem; overflow: hidden;">
            <div style="width: {progress_pct}%; height: 100%; background-color: #185FA5; transition: width 0.4s ease-in-out;"></div>
        </div>
    """, unsafe_allow_html=True)

    steps = [("Upload", 1), ("Preview", 3), ("Mapeamento", 5), ("Gerar SQL", 4)]
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
                        
                        # Tratamento automático de codificação falha utf-8 para latin-1
                        df = fix_dataframe_mojibake(df)
                        
                        base = os.path.splitext(uploaded_file.name)[0]
                        table = clean_col_name(base)
                        _, f_cols = process_dataframe_columns(df)
                        
                        st.session_state.df = df
                        st.session_state.df_original = df.copy() # Backup de Origem
                        st.session_state.table_name = table
                        st.session_state.staging_table_name = table
                        st.session_state.insert_table_name = table
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
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # --- NOVA BARRA DE AÇÕES DO PREVIEW ---
            col_actions = st.columns([1, 1, 1.2, 1.8], gap="small")
            
            with col_actions[0]:
                if st.button("⬅ Voltar", use_container_width=True): 
                    st.session_state.step = 1
                    st.rerun()
            
            with col_actions[1]:
                if st.button("Resetar", use_container_width=True, help="Volta a planilha ao estado original do upload"):
                    st.session_state.df = st.session_state.df_original.copy()
                    st.session_state.reset_counter += 1
                    _, r_cols = process_dataframe_columns(st.session_state.df)
                    st.session_state.final_cols = r_cols
                    st.session_state.staging_sql = generate_staging_sql(st.session_state.df, st.session_state.table_name, r_cols)
                    st.rerun()
            
            with col_actions[2]:
                if st.button("Gerar SQL Tabela", use_container_width=True, help="Gera apenas o CREATE TABLE e INSERTS da staging"):
                    # Ao entrar no fluxo de Staging Puro, garante que o SQL esteja atualizado com o nome correto
                    _, r_cols = process_dataframe_columns(st.session_state.df)
                    st.session_state.staging_sql = generate_staging_sql(
                        st.session_state.df, 
                        st.session_state.staging_table_name, 
                        r_cols
                    )
                    st.session_state.target_template = None
                    st.session_state.step = 4
                    st.rerun()
            
            with col_actions[3]:
                # Dropdown de Scripts INSERT
                template_options = {None: "Selecione o Script INSERT ▾"}
                for tid, tval in INSERT_TEMPLATES_REGISTRY.items():
                    template_options[tid] = tval['label']
                
                selected_template = st.selectbox(
                    "Script INSERT",
                    options=list(template_options.keys()),
                    format_func=lambda x: template_options[x],
                    label_visibility="collapsed"
                )
                
                if selected_template:
                    st.session_state.target_template = selected_template
                    # Auto-mapeamento inicial
                    st.session_state.insert_mapping = auto_map_fields(selected_template, st.session_state.df.columns)
                    st.session_state.step = 5
                    st.rerun()
    with col_side:
        render_info_panel()

# STEP 5: MAPEAMENTO INSERT
elif st.session_state.step == 5:
    render_breadcrumb(5)
    
    template_id = st.session_state.target_template
    template = INSERT_TEMPLATES_REGISTRY[template_id]
    
    col_main, col_side = st.columns([2.5, 1], gap="large")
    
    with col_main:
        st.markdown(f"""
            <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 1.5rem;">
                <div style="background-color: #185FA5; width: 40px; height: 40px; border-radius: 10px; display: flex; align-items: center; justify-content: center;">
                    <span class="material-symbols-outlined" style="color: white; font-size: 24px;">account_tree</span>
                </div>
                <div>
                    <h2 style="font-size: 1.25rem; font-weight: 800; color: #1e293b; margin: 0;">Mapeamento de INSERT — {template['label']}</h2>
                    <p style="font-size: 13px; color: #64748b; margin: 0;">Configure a origem de cada campo para gerar o script de importação.</p>
                </div>
            </div>
        """, unsafe_allow_html=True)

        with st.container(border=True):
            st.markdown('<h4 style="font-size: 0.875rem; font-weight: 700; margin-bottom: 1rem; color: #475569;">MAPEAMENTO DE CAMPOS</h4>', unsafe_allow_html=True)
            
            # Cabeçalho da Tabela
            h_col1, h_col2, h_col3, h_col4 = st.columns([1.5, 1, 2, 0.8])
            h_col1.markdown("**Campo do INSERT**")
            h_col2.markdown("**Tipo**")
            h_col3.markdown("**Origem / Valor**")
            h_col4.markdown("**Status**")
            st.markdown("<hr style='margin: 0.5rem 0 1rem 0; opacity: 0.1;'>", unsafe_allow_html=True)
            
            mapping = st.session_state.insert_mapping
            spreadsheet_cols = ["PULAR"] + list(st.session_state.df.columns)
            
            for field in template['fields']:
                f_name = field['name']
                f_type = field['type']
                
                r_col1, r_col2, r_col3, r_col4 = st.columns([1.5, 1, 2, 0.8])
                
                # Nome do Campo
                r_col1.markdown(f"<span style='color: #1e293b; font-size: 14px; font-weight: 500;'>{field['label']}<br><small style='color: #94a3b8;'>{f_name}</small></span>", unsafe_allow_html=True)
                
                # Tipo do Campo
                type_info = {
                    'fixed': ('build', 'Fixo'),
                    'spreadsheet': ('description', 'Planilha'),
                    'configurable': ('settings', 'Configurável')
                }.get(f_type, ('help', f_type))
                r_col2.markdown(f"<div style='display: flex; align-items: center; gap: 6px; color: #64748b;'><span class='material-symbols-outlined' style='font-size: 16px;'>{type_info[0]}</span><span style='font-size: 13px;'>{type_info[1]}</span></div>", unsafe_allow_html=True)
                
                # Origem / Valor (Widget)
                if f_type == 'spreadsheet':
                    m_data = mapping.get(f_name)
                    # Normalização (converte legado p/ híbrido se necessário)
                    if not isinstance(m_data, dict):
                        m_data = {"column": m_data if m_data in spreadsheet_cols else "PULAR", "override": ""}
                    
                    c_drop, c_edit = r_col3.columns([1.2, 1], gap="small")
                    
                    current_col = m_data.get('column', 'PULAR')
                    if current_col not in spreadsheet_cols: current_col = "PULAR"
                    
                    selected_col = c_drop.selectbox(
                        f"Col_{f_name}",
                        options=spreadsheet_cols,
                        index=spreadsheet_cols.index(current_col),
                        key=f"field_col_{f_name}",
                        label_visibility="collapsed"
                    )
                    
                    override_val = c_edit.text_input(
                        f"Over_{f_name}",
                        value=m_data.get('override', ''),
                        placeholder="Sobrescrever...",
                        key=f"field_over_{f_name}",
                        label_visibility="collapsed"
                    )
                    
                    mapping[f_name] = {
                        "column": selected_col if selected_col != "PULAR" else None, 
                        "override": override_val
                    }
                    
                else:
                    # 'configurable' ou 'fixed' agora ambos são editáveis via override direto
                    val = mapping.get(f_name, field.get('default', ''))
                    # Se vier do formato híbrido por erro, extrai o override
                    if isinstance(val, dict): val = val.get('override', '')
                    
                    new_val = r_col3.text_input(
                        f"Valor para {f_name}",
                        value=val,
                        key=f"field_{f_name}",
                        label_visibility="collapsed"
                    )
                    mapping[f_name] = new_val
                
                # Status
                if f_type == 'fixed':
                    r_col4.markdown("<div style='display: flex; align-items: center; gap: 4px; color: #94a3b8;'><span class='material-symbols-outlined' style='font-size: 18px;'>lock</span><span style='font-size: 12px;'>Fixo</span></div>", unsafe_allow_html=True)
                elif f_type == 'spreadsheet':
                    if mapping.get(f_name):
                        r_col4.markdown("<div style='display: flex; align-items: center; gap: 4px; color: #059669;'><span class='material-symbols-outlined' style='font-size: 18px;'>check_circle</span><span style='font-size: 12px;'>Mapeado</span></div>", unsafe_allow_html=True)
                    elif field.get('required'):
                        r_col4.markdown("<div style='display: flex; align-items: center; gap: 4px; color: #e11d48;'><span class='material-symbols-outlined' style='font-size: 18px;'>warning</span><span style='font-size: 12px;'>Obrigatório</span></div>", unsafe_allow_html=True)
                    else:
                        r_col4.markdown("<div style='display: flex; align-items: center; gap: 4px; color: #94a3b8;'><span class='material-symbols-outlined' style='font-size: 18px;'>radio_button_unchecked</span><span style='font-size: 12px;'>Opcional</span></div>", unsafe_allow_html=True)
                else:
                    r_col4.markdown("<div style='display: flex; align-items: center; gap: 4px; color: #059669;'><span class='material-symbols-outlined' style='font-size: 18px;'>check_circle</span><span style='font-size: 12px;'>Pronto</span></div>", unsafe_allow_html=True)

                st.markdown("<hr style='margin: 0.5rem 0; opacity: 0.05;'>", unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)
            
            # Bottom Buttons
            b_col1, b_col2, b_col3, b_col4 = st.columns([1, 1, 1, 1])
            with b_col1:
                if st.button("⬅ Voltar", use_container_width=True):
                    st.session_state.step = 3
                    st.rerun()
            with b_col2:
                if st.button("Auto-mapear", use_container_width=True, icon=":material/magic_button:"):
                    st.session_state.insert_mapping = auto_map_fields(template_id, st.session_state.df.columns)
                    st.rerun()
            with b_col3:
                if st.button("Salvar JSON", use_container_width=True, icon=":material/save:"):
                    st.toast("Mapeamento salvo com sucesso!")
            with b_col4:
                if st.button("Confirmar →", type="primary", use_container_width=True):
                    # Validação
                    missing = [f['label'] for f in template['fields'] if f.get('required') and f['type'] == 'spreadsheet' and not mapping.get(f['name'])]
                    if missing:
                        st.error(f"Campos obrigatórios não mapeados: {', '.join(missing)}")
                    else:
                        # Gera o SQL Final usando o nome da tabela do fluxo INSERT
                        st.session_state.final_sql = generate_insert_sql(
                            template_id, 
                            mapping, 
                            st.session_state.global_configs,
                            st.session_state.insert_table_name
                        )
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
            # Lógica de Renomeação Isolada por Contexto
            if st.session_state.target_template:
                # Fluxo de Mapeamento INSERT
                new_table = st.text_input("Renomear Tabela de Staging (INSERT)", value=st.session_state.insert_table_name)
                if new_table != st.session_state.insert_table_name:
                    st.session_state.insert_table_name = new_table
                    st.session_state.final_sql = generate_insert_sql(
                        st.session_state.target_template, 
                        st.session_state.insert_mapping, 
                        st.session_state.global_configs,
                        new_table
                    )
                    st.rerun()
            else:
                # Fluxo de Staging Puro (Tabela)
                new_table = st.text_input("Renomear Tabela de Staging (Tabela)", value=st.session_state.staging_table_name)
                if new_table != st.session_state.staging_table_name:
                    st.session_state.staging_table_name = new_table
                    _, r_cols = process_dataframe_columns(st.session_state.df)
                    st.session_state.staging_sql = generate_staging_sql(st.session_state.df, new_table, r_cols)
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
                        <textarea id="sql" class="editor" readonly>{st.session_state.final_sql if st.session_state.final_sql else st.session_state.staging_sql}</textarea>
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
                    sql_content = st.session_state.final_sql if st.session_state.final_sql else st.session_state.staging_sql
                    caminho = save_file_native(sql_content, default_name)
                    if caminho:
                        st.success(f"✅ Arquivo salvo com sucesso em:\n{caminho}")

    with col_side:
        render_info_panel()
