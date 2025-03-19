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

def write_csv_to_s3(bucket, key, df):
    parquet_buffer = BytesIO()
    df.to_csv(parquet_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=parquet_buffer.getvalue())
    print(f"File {key} saved successfully to S3 bucket {bucket}.")

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

    # Processando sempre o dia de ontem por padrão
    day_to_process = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
    #day_to_process = '2025-03-06' # Remover o comentário para processamento manual

    # S3 bucket parameters
    input_bucket = 'olho-vivo-etl'
    input_key = 'raw/' + day_to_process[:7] + '/pos-' + day_to_process + '.parquet'

    # Output de velocidades médias, sem agregação
    output_bucket = 'olho-vivo-etl'
    output_key_vel = 'velocidades/' + day_to_process[:7] + '/vel-' + day_to_process + '.csv'

    # Output de velocidades médias, agregadas
    output_key_vel_agg = 'velocidades-agg/' + day_to_process[:7] + '/vel-agg-' + day_to_process + '.csv'

    # Output de pontos de lentidão
    output_key_slow = 'lentidao/' + day_to_process[:7] + '/slow-' + day_to_process + '.csv'

    # Output de acessibilidade
    output_acessiveis = 'acessiveis/' + day_to_process[:7] + '/acessiveis-' + day_to_process + '.csv'

    # Lê o arquivo Parquet do S3
    print(f"Reading file {input_key} from S3 bucket {input_bucket}.")
    df = read_parquet_from_s3(input_bucket, input_key)

    print(df.head())

    # Criação de intervalos de 30 minutos
    df[['intervalo', 'data']] = pd.DataFrame(df['timestamp'].map(get_30min_interval).tolist(), index=df.index)

    # INÍCIO DO CÁLCULO DE VELOCIDADE MÉDIA

    # Ordena por veículo e timestamp
    df = df.sort_values(by=['prefixo_veiculo', 'timestamp'])

    # TEMPORARIO: TRUNCAR 1000 linhas
    df = df[:1000]

    # Criação de colunas de posição e tempo anterior
    df['px_anterior'] = df.groupby('prefixo_veiculo')['px'].shift(1)
    df['py_anterior'] = df.groupby('prefixo_veiculo')['py'].shift(1)
    df['timestamp_anterior'] = df.groupby('prefixo_veiculo')['timestamp'].shift(1)

    # LIMPEZA: remoção de linhas em que a posição anterior é nula (primeira aquisição de veículo x linha)
    df = df.drop(df[df.px_anterior.isna()].index)

    # Cálculo do tempo para percorrer a distância
    df['tempo'] = df['timestamp'] - df['timestamp_anterior']

    # LIMPEZA: remoção de linhas em que o intervalo de aquisição é maior do que 10 minutos
    df = df.drop(df[df.tempo > 600].index)

    # Cálculo da distância percorrida
    df['distancia'] = df.apply(
        lambda row: haversine(row['py_anterior'], row['px_anterior'], row['py'], row['px']),
        axis=1
    )

    # LIMPEZA: arredondamento de distância para 2 casas decimais
    df['distancia'] = df['distancia'].round(2)

    # delta_v = delta_x / delta_t
    df['velocidade_media'] = df['distancia'] / df['tempo']

    # LIMPEZA: remoção de linhas em que a velocidade é maior do que 120 km/h (33 m/s)
    df = df.drop(df[df.velocidade_media > 33].index)

    # Escreve o resultado no S3
    #write_csv_to_s3(output_bucket, output_key_vel, df)
    # Dados muito granulares, melhor manter apenas a versão agregada


    # CÁLCULO DE PONTOS DE LENTIDÃO -----------------------------

    # Filtra os pontos de lentidão (velocidade média < 5 km/h)
    slow_points = df[df['velocidade_media'] < 1.4]

    # Filtragem de colunas relevantes
    slow_points = slow_points[['data', 'intervalo', 'letreiro', 'codigo_linha', 'sentido_linha', 'origem_linha', 'destino_linha', 'prefixo_veiculo', 'px', 'py', 'velocidade_media', 'tempo', 'distancia']]

    write_csv_to_s3(output_bucket, output_key_slow, slow_points)

    # CÁLCULO DE VELOCIDADE MÉDIA AGREGADA -----------------------------

    aggregated_df = df.groupby(['data', 'intervalo', 'letreiro', 'codigo_linha', 'sentido_linha', 'destino_linha', 'origem_linha', 'prefixo_veiculo', 'acessibilidade'], as_index=False).agg({
        'px': 'mean',
        'py': 'mean',
        'velocidade_media': 'mean',
        'distancia': 'sum',
        'tempo': 'sum'
    })

    # Novo cálculo de velocidades médias, agora com os tempos e distâncias totais (não funciona algebricamente a média das velocidades médias)
    aggregated_df['velocidade_media'] = aggregated_df['distancia'] / aggregated_df['tempo']

    # Filtragem de colunas relevantes
    velocidades_agregadas = aggregated_df[['data', 'intervalo', 'letreiro', 'codigo_linha', 'sentido_linha', 'origem_linha', 'destino_linha', 'prefixo_veiculo', 'px', 'py', 'velocidade_media', 'tempo', 'distancia']]

    # Escreve o resultado no S3
    write_csv_to_s3(output_bucket, output_key_vel_agg, velocidades_agregadas)


    # LOCALIZAÇÃO DE VÍCULOS ACESSIVEIS
    #acessiveis_df = aggregated_df      [['data', 'intervalo', 'letreiro', 'codigo_linha', 'sentido_linha', 'origem_linha', 'destino_linha', 'prefixo_veiculo', 'acessibilidade']]
    acessiveis_df = aggregated_df[['data', 'intervalo', 'letreiro', 'codigo_linha', 'sentido_linha', 'origem_linha', 'destino_linha', 'prefixo_veiculo', 'px', 'py']]
    write_csv_to_s3(output_bucket, output_acessiveis, acessiveis_df)


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
