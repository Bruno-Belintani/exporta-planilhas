# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_all
from PyInstaller.utils.hooks import copy_metadata

datas = [('D:\\LegalManager\\MIGRACOES\\BP\\exporta_planilhas\\app.py', '.'), ('gera_script.py', '.'), ('logo.png', '.')]
binaries = []
hiddenimports = ['datetime.datetime', 'gera_script.clean_col_name', 'gera_script.generate_final_sql', 'gera_script.generate_mapping_suggestions', 'gera_script.generate_staging_sql', 'gera_script.load_memory', 'gera_script.parse_mapping_dict', 'gera_script.process_dataframe_columns', 'gera_script.save_memory', 'os', 'pandas', 'streamlit', 'streamlit.components.v1', 'tkinter', 'tkinter.filedialog']
datas += copy_metadata('streamlit')
tmp_ret = collect_all('streamlit')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]


a = Analysis(
    ['C:\\Users\\BRUNO_~1\\AppData\\Local\\Temp\\tmpcc1jwavv.py'],
    pathex=['.'],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='LM Exportador',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['D:\\LegalManager\\MIGRACOES\\BP\\exporta_planilhas\\logo.png'],
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='LM Exportador',
)
