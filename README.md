# Gerador de Script SQL a partir de Planilhas (gera_script.py)

Este script lê planilhas no formato Excel (`.xlsx`, `.xlsm`, `.xls`) e converte os dados automaticamente num arquivo de script SQL compatível com banco de dados codificados em **Latin1** (ex: bancos PostgreSQL mais antigos).

Ele é responsável por três grandes partes:
1. **Criação da Tabela**: Lê o cabeçalho da planilha, normaliza os nomes das colunas (removendo acentuações, limpezas, trocando espaços por _ e tratando letras), e gera uma instrução de `CREATE TABLE`.
2. **Substituição de Caracteres**: Detecta e subsitutui caracteres de codificação **UTF-8** que normalmente quebram importações pro formato **Latin1** (por exemplo, aspas inglesas “”‘’, hifens longos –, entre outros). As eventuais sobras são convertidas com segurança para não gerar erro no banco.
3. **Instruções de INSERT**: Gera todas as instruções `INSERT INTO` processando todas as linhas da planilha de uma vez.

## Requisitos
Para que o seu programa funcione, certifique-se de que tenha a biblioteca Pandas instalada no Node/Python:
```bash
pip install pandas openpyxl
```

## Como Usar o Programa

Você não precisa mais editar o código toda as vezes que mudar de planilha. Agora você pode executar passando o arquivo diretamente no terminal:

```bash
python gera_script.py <nome_do_seu_arquivo.xlsm>
```

**Exemplo prático de uso:**
```bash
python gera_script.py planilha-global-atual.xlsm
```

### O que acontece quando você roda o comando?

1. O script procurará pelo arquivo especificado (por exemplo, `planilha-global-atual.xlsm`).
2. Ele fará todo o levantamento de colunas e dados.
3. Quando finalizar, ele gera um arquivo `.sql` na mesma pasta baseado no nome do seu arquivo original (nesse exemplo, sairia `script_planilha_global_atual.sql`).
4. Também de forma automática, a tabela referenciada interna neste SQL já estará nomeada pelo mesmo prefixo (no exemplo, o `CREATE TABLE` gerará uma tabela `planilha_global_atual`).

Pronto! Ao final de tudo basta rodar o `.sql` gerado dentro do seu Banco de Dados (como com o pgAdmin, DBeaver, Dteq, etc.).
