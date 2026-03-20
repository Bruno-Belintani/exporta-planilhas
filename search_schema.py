import re
import sys

def parse_schema(output_file):
    with open(output_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    keywords = ["processo", "vara", "comarca", "estado", "reclamante", "reclamada", "gcpj", "cadastro", "audiencia", "status"]
    
    for line in content.split('\n'):
        if any(kw in line.lower() for kw in keywords):
            print(line.strip())

if __name__ == "__main__":
    parse_schema(r"C:\Users\Bruno_Belintani\.gemini\antigravity\brain\8947280c-28c4-4cfd-b938-8c846e0f71dc\.system_generated\steps\60\output.txt")
