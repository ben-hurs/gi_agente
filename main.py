from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import pandas as pd
import os
import glob

app = FastAPI()

# === Carregar os arquivos Parquet divididos ===
try:
    parquet_dir = "dados/parquet_dividido"
    arquivos_parquet = glob.glob(os.path.join(parquet_dir, "*.parquet"))

    # Carrega e concatena os arquivos
    df_pgfn = pd.concat([
        pd.read_parquet(arquivo, columns=[
            "CPF_CNPJ", "NOME_DEVEDOR", "RECEITA_PRINCIPAL",
            "SITUACAO_INSCRICAO", "VALOR_CONSOLIDADO"
        ], engine="pyarrow")
        for arquivo in arquivos_parquet
    ], ignore_index=True)

    df_pgfn.fillna("", inplace=True)
except Exception as e:
    print(f"Erro ao carregar Parquet PGFN particionado: {e}")
    df_pgfn = pd.DataFrame()

# === Carregar o CSV do IBAMA ===
CSV_PATH = os.path.join("dados", "ibama_multas.csv")
try:
    df_ibama = pd.read_csv(CSV_PATH, sep=",", encoding="utf-8", dtype=str, on_bad_lines='skip')
    df_ibama.columns = df_ibama.columns.str.strip()
    df_ibama.fillna("", inplace=True)
except Exception as e:
    print(f"Erro ao carregar CSV IBAMA: {e}")
    df_ibama = pd.DataFrame()

# === Endpoints ===

@app.get("/")
def raiz():
    return {"status": "ok", "mensagem": "API do agente IA ativa"}

@app.get("/multas-ibama")
def consultar_multas(cpf_cnpj: str = Query(..., alias="cpf_cnpj")):
    if df_ibama.empty:
        return JSONResponse(status_code=500, content={"erro": "Base de dados do IBAMA não carregada."})

    if 'CPF ou CNPJ' not in df_ibama.columns:
        return JSONResponse(status_code=500, content={"erro": "Coluna 'CPF ou CNPJ' não encontrada no CSV."})

    resultados = df_ibama[df_ibama['CPF ou CNPJ'] == cpf_cnpj]

    if resultados.empty:
        return JSONResponse(status_code=404, content={"mensagem": "Nenhuma multa encontrada para o CPF/CNPJ informado."})

    return resultados.to_dict(orient="records")

@app.get("/dividas-pgfn")
def consultar_dividas_pgfn(cpf_cnpj: str = Query(..., alias="cpf_cnpj")):
    if df_pgfn.empty:
        return JSONResponse(status_code=500, content={"erro": "Base de dados PGFN não carregada."})

    resultados = df_pgfn[df_pgfn['CPF_CNPJ'] == cpf_cnpj]

    if resultados.empty:
        return JSONResponse(status_code=404, content={"mensagem": "Nenhuma dívida encontrada para o CPF/CNPJ informado."})

    return resultados.to_dict(orient="records")
