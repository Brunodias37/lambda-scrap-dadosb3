import json
import requests
import pandas as pd
from datetime import datetime
import s3fs
import boto3
import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)
data_pregao = datetime.today().strftime("%Y%m%d")

def salvar_dataframe_parquet_s3(df, bucket, s3_key, compression='snappy',  **args):
    s3 = s3fs.S3FileSystem()
    df.to_parquet(
        f's3://{bucket}/{s3_key}/',
        engine='pyarrow',
        filesystem=s3,
        partition_cols=['anomesdia']
    )

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


def criar_tabela_raw(bucket, s3_key):
    database_name = "workspace_db"
    table_name = "tb_dados_b3_raw"
    table_location = f's3://{bucket}/{s3_key}/'
    glue_client = boto3.client("glue")

    try:
        glue_client.get_database(Name=database_name)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_database(DatabaseInput={'Name': database_name})

    table_input = {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': [
                {"Name": "segment", "Type": "string"},
                {"Name": "cod", "Type": "string"},
                {"Name": "asset", "Type": "string"},
                {"Name": "type", "Type": "string"},
                {"Name": "part", "Type": "string"},
                {"Name": "partAcum", "Type": "string"},
                {"Name": "theoricalQty", "Type": "string"},
                # {"Name": "segment_list", "Type": "string"},
                # {"Name": "type_list", "Type": "string"}
            ],
            'Location': table_location,  # sem anomesdia
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {'serialization.format': '1'}
            }
        },
        'PartitionKeys': [{"Name": "anomesdia", "Type": "string"}],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {'classification': 'parquet'}
    }

    try:
        glue_client.get_table(DatabaseName=database_name, Name=table_name)
        glue_client.update_table(DatabaseName=database_name, TableInput=table_input)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_table(DatabaseName=database_name, TableInput=table_input)


def reparar_tabela_athena(database_name, table_name):
    athena = boto3.client('athena')
    output_location = 's3://bovespa-204590505567/athena-query-results/'  # crie este prefix
    response = athena.start_query_execution(
        QueryString=f"MSCK REPAIR TABLE {database_name}.{table_name}",
        QueryExecutionContext={'Database': database_name},
        ResultConfiguration={'OutputLocation': output_location}
    )
    logger.info(f"MSCK REPAIR TABLE iniciado. QueryExecutionId: {response['QueryExecutionId']}")


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
    df = df.rename(columns={'data_pregao': 'anomesdia'})
    df = df.drop(columns=['type_list', 'segment_list'])
    salvar_dataframe_parquet_s3(df, 'bovespa-204590505567', 'raw/tb_dados_b3_raw')
    criar_tabela_raw('bovespa-204590505567', 'raw/tb_dados_b3_raw')
    reparar_tabela_athena('workspace_db', 'tb_dados_b3_raw')
    return {
        'statusCode': 200,
        'body': json.dumps(f'Arquivo com dados de {datetime.today().strftime("%Y/%m/%d")} salvo com sucesso! ')
    }
