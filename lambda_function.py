import json
import requests
import pandas as pd
from datetime import datetime
import s3fs
data_pregao = datetime.today().strftime("%Y%m%d")

def salvar_dataframe_parquet_s3(df, bucket, s3_key, compression='snappy',  **args):
    s3 = s3fs.S3FileSystem()
    full_s3_path = f's3://{bucket}/{s3_key}'

    df.to_parquet(full_s3_path, engine='pyarrow', compression=compression, storage_options={'s3_additional_kwargs': {'ACL': 'bucket-owner-full-control'}}, **args)


def process_registro(registro):
    # Corrigir campo 'segment'
    raw_segment = registro.get('segment', '')
    raw_segment = raw_segment.strip()
    setores = [s.strip() for s in raw_segment.split('/') if s.strip()] if raw_segment else []

    # Corrigir campo 'type'
    raw_type = registro.get('type', '')
    raw_type = raw_type.strip()
    tipos = [t.strip() for t in raw_type.split() if t.strip()] if raw_type else []

    # Adicionar listas e data
    registro['segment_list'] = setores
    registro['type_list'] = tipos
    registro['data_pregao'] = data_pregao
    return registro



def lambda_handler(event, context):

    payload = "eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjEyMCwiaW5kZXgiOiJJQk9WIiwic2VnbWVudCI6IjIifQ=="
    url = f"https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/{payload}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    registros = data.get("results") or data
    if isinstance(registros, dict):
        # tentar pegar lista dentro do dict
        registros = next((v for v in registros.values() if isinstance(v, list)), [data])

    print(f"Recebidos {len(registros)} registros.")
    print(json.dumps(registros[:2], ensure_ascii=False, indent=2)) 

    # Processar
    registros_processados = [process_registro(r) for r in registros]

    # Criar DataFrame
    df = pd.DataFrame(registros_processados)

    salvar_dataframe_parquet_s3(df, 'bovespa-204590505567/raw', f'dados_ibov_{data_pregao}.parquet')

    return {
        'statusCode': 200,
        'body': json.dumps(f'Arquivo com dados de {datetime.today().strftime("%Y/%m/%d")} salvo com sucesso! ')
    }
