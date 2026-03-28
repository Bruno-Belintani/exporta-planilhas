import streamlit as st
import pandas as pd
import os
import base64
from datetime import datetime
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

st.set_page_config(page_title="Exportador de Planilhas", page_icon="logo.png", layout="wide")

def set_header_logo():
    logo_path = "logo_header.png" if os.path.exists("logo_header.png") else "logo_header.jpg" if os.path.exists("logo_header.jpg") else None
    if logo_path:
        # O Streamlit possui uma função nativa perfeita para colocar a imagem no topo esquerdo do cabeçalho
        st.logo(logo_path)

set_header_logo()
st.title("Exportador de Planilhas para SQL")
st.write("Faça o upload da sua planilha Excel (XLSX, XLSM) para gerar os scripts de banco de dados e mapeamentos ajustáveis na tela.")

uploaded_file = st.file_uploader("Selecione a planilha", type=["xlsx", "xlsm"])

if uploaded_file is not None:
    st.success("Planilha carregada com sucesso!")
    
    # Executar processamento inicial apenas uma vez por arquivo enviado
    if "data_processed" not in st.session_state or st.session_state.uploaded_filename != uploaded_file.name:
        df = pd.read_excel(uploaded_file)
        
        base_name = os.path.splitext(uploaded_file.name)[0]
        table_name = clean_col_name(base_name)
        if not table_name:
            table_name = 'tabela_importada'
            
        original_cols, final_cols = process_dataframe_columns(df)
        memoria = load_memory()
        
        st.session_state.df = df
        st.session_state.table_name = table_name
        st.session_state.original_cols = original_cols
        st.session_state.final_cols = final_cols
        st.session_state.memoria = memoria
        st.session_state.base_name = base_name
        st.session_state.uploaded_filename = uploaded_file.name
        st.session_state.data_processed = True
        
        # Gerar sugestões iniciais
        suggestions = generate_mapping_suggestions(original_cols, memoria)
        st.session_state.mapping_df = pd.DataFrame(suggestions)
        
        # Gerar string com script staging
        st.session_state.staging_sql = generate_staging_sql(df, table_name, final_cols)
        st.session_state.staging_file_name = f'script_{base_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.sql'
        
        # Limpar o sql final caso estivesse preenchido com planilha antiga
        if "final_sql" in st.session_state:
            del st.session_state["final_sql"]

    st.divider()

    # col1, col2 = st.columns(2)
    # 
    # with col1:
    st.subheader("1. Script de Staging (Tabela Bruta)")
    # st.write("Este script cria a tabela temporária e insere os dados da planilha de forma bruta.")
    
    st.download_button(
        label="Baixar Script de Staging (.sql)",
        data=st.session_state.staging_sql,
        file_name=st.session_state.staging_file_name,
        mime="text/plain"
    )
    
    with st.expander("Visualizar as 10 primeiras linhas dos dados"):
        st.dataframe(st.session_state.df.head(10))

    # with col2:
    #     st.subheader("2. Mapeamento de Colunas")
    #     st.caption("Edite o campo 'Destino'. Pular a coluna informando 'PULAR' ou deixando em branco. Formato exigido: tabela.coluna")
    #     
    #     # Tabela editável
    #     edited_mapping_df = st.data_editor(
    #         st.session_state.mapping_df,
    #         num_rows="fixed",
    #         use_container_width=True,
    #         hide_index=True,
    #         key="mapping_editor"
    #     )
    #     
    #     if st.button("Validar Mapeamento e Gerar Script Final", type="primary"):
    #         suggestions_list = edited_mapping_df.to_dict('records')
    #         
    #         # Formats back and updates memory dict
    #         mapeamento_validado = parse_mapping_dict(
    #             suggestions_list, 
    #             st.session_state.original_cols, 
    #             st.session_state.final_cols, 
    #             st.session_state.memoria
    #         )
    #         
    #         save_memory(st.session_state.memoria)
    #         
    #         if mapeamento_validado:
    #             final_sql = generate_final_sql(mapeamento_validado, st.session_state.table_name)
    #             st.session_state.final_sql = final_sql
    #             st.session_state.final_file_name = f'migracao_final_{st.session_state.base_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.sql'
    #             st.success("Mapeamento lido, memória salva e Script Final gerado!")
    #         else:
    #             st.error("Nenhuma coluna válida mapeada. Script dinâmico cancelado.")
    #             if "final_sql" in st.session_state:
    #                 del st.session_state["final_sql"]
    #                 
    #     if "final_sql" in st.session_state:
    #         st.download_button(
    #             label="Baixar Script de Migração Final (.sql)",
    #             data=st.session_state.final_sql,
    #             file_name=st.session_state.final_file_name,
    #             mime="text/plain"
    #         )
