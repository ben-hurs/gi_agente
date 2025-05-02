from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import pandas as pd
import os

app = FastAPI()

# Caminho para o arquivo CSV
CSV_PATH = os.path.join("dados", "ibama_multas.csv")

# Lê o CSV e padroniza colunas
try:
    df = pd.read_csv(CSV_PATH, sep=",", encoding="utf-8", dtype=str, on_bad_lines='skip')
    df.columns = df.columns.str.strip()  # Limpa espaços extras
    df.fillna("", inplace=True)
except Exception as e:
    print(f"Erro ao carregar CSV: {e}")
    df = pd.DataFrame()


@app.get("/")
def raiz():
    return {"status": "ok", "mensagem": "API do agente IA ativa"}



@app.get("/multas-ibama")
def consultar_multas(cpf_cnpj: str = Query(..., alias="cpf_cnpj")):
    if df.empty:
        return JSONResponse(status_code=500, content={"erro": "Base de dados não carregada."})

    if 'CPF ou CNPJ' not in df.columns:
        return JSONResponse(status_code=500, content={"erro": "Coluna 'CPF ou CNPJ' não encontrada no CSV."})

    resultados = df[df['CPF ou CNPJ'] == cpf_cnpj]

    if resultados.empty:
        return JSONResponse(status_code=404, content={"mensagem": "Nenhuma multa encontrada para o CPF/CNPJ informado."})

    return resultados.to_dict(orient="records")
