import json
import pandas as pd
import numpy as np
import datetime
import boto3
from io import StringIO, BytesIO
import math

s3 = boto3.client('s3')

def read_parquet_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(response['Body'].read()))

def write_parquet_to_s3(bucket, key, df):
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=parquet_buffer.getvalue())

def get_30min_interval(unix_timestamp):
    dt = datetime.datetime.utcfromtimestamp(unix_timestamp)
    floored_dt = dt.replace(minute=(dt.minute // 30) * 30, second=0, microsecond=0)
    interval = f"{floored_dt.strftime('%H:%M')}-{(floored_dt + datetime.timedelta(minutes=30)).strftime('%H:%M')}"
    date_interval = floored_dt.date().strftime("%Y-%m-%d")
    return interval, date_interval

# Função para calcular a distância em metros usando a fórmula de Haversine
def haversine(lat1, lon1, lat2, lon2):

    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None

    # Radius of the Earth in meters
    R = 6371000.0

    # Convert degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Differences in coordinates
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    # Haversine formula
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Distance in meters
    distance = R * c
    return distance

def lambda_handler(event, context):
    # Parâmetros S3
    input_bucket = 'olho-vivo-etl'
    input_key = 'pos-2025-03-06.parquet'
    output_bucket = 'olho-vivo-etl'
    output_key = 'velocidades/teste.parquet'

    # Lê o arquivo Parquet do S3
    df = read_parquet_from_s3(input_bucket, input_key)

    # Ordena por veículo e timestamp
    df = df.sort_values(by=['prefixo_veiculo', 'timestamp'])

    # Criação de intervalos de 30 minutos
    df[['intervalo', 'date_interval']] = pd.DataFrame(df['timestamp'].map(get_30min_interval).tolist(), index=df.index)

    # Ordernar
    df = df.sort_values(by=['prefixo_veiculo', 'timestamp'])

    df['px_anterior'] = df.groupby('prefixo_veiculo')['px'].shift(1)
    df['py_anterior'] = df.groupby('prefixo_veiculo')['py'].shift(1)
    df['timestamp_anterior'] = df.groupby('prefixo_veiculo')['timestamp'].shift(1)

    df['distancia'] = df.apply(
        lambda row: haversine(row['py_anterior'], row['px_anterior'], row['py'], row['px']),
        axis=1
    )

    df['tempo'] = df['timestamp'] - df['timestamp_anterior']

    df['velocidade_media'] = df['distancia'] / df['tempo']

    # Escreve o resultado no S3 como Parquet
    write_parquet_to_s3(output_bucket, output_key, df)

    print('Velocidade média calculada e salva no S3. Iniciando agregação')

    df = df[df['velocidade_media'].notna()]

    aggregated_df = df.groupby(['date_interval', 'intervalo', 'letreiro', 'codigo_linha', 'sentido_linha', 'destino_linha', 'origem_linha', 'prefixo_veiculo'], as_index=False).agg({
        'px': 'mean',
        'py': 'mean',
        'velocidade_media': 'mean',
        'distancia': 'sum',
        'tempo': 'sum'
    })

    # Escreve o resultado no S3 como Parquet
    write_parquet_to_s3(output_bucket, 'velocidades/teste-agregado.parquet', aggregated_df)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
