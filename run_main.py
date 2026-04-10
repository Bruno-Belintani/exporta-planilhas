import os
import sys
import streamlit.web.cli as stcli

import threading
import time
import subprocess

def resolve_path(path):
    if getattr(sys, 'frozen', False):
        resolved_path = os.path.abspath(os.path.join(sys._MEIPASS, path))
    else:
        resolved_path = os.path.abspath(os.path.join(os.getcwd(), path))
    return resolved_path

def start_window():
    time.sleep(1.5)
    # Abre o PWA no formato de aplicativo Windows sem abas do navegador
    try:
        subprocess.run(['start', 'msedge', '--app=http://localhost:8501'], shell=True)
    except:
        subprocess.run(['start', 'chrome', '--app=http://localhost:8501'], shell=True)

if __name__ == "__main__":
    app_path = resolve_path("app.py")
    
    # Inicia a thread que chamará a janela local
    threading.Thread(target=start_window, daemon=True).start()
    
    # Bloqueia a abertura padrão da aba de internet, forçamos o modo headless + janela acima
    sys.argv = ["streamlit", "run", app_path, "--server.headless=true", "--global.developmentMode=false"]
    sys.exit(stcli.main())
