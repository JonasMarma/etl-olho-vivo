import json
import os
import pandas as pd
import boto3
from io import StringIO, BytesIO
import datetime

s3 = boto3.client('s3')

def read_json_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    return json.loads(content)

def write_parquet_to_s3(bucket, key, df):
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=parquet_buffer.getvalue())

def lambda_handler(event, context):

    day_to_process = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
    #day_to_process = '2025-03-06' # Remover o comentário para processamento manual

    source_bucket = 'olho-vivo-raw'
    source_prefix = 'posicoes/year=' + day_to_process[:4] + '/month=' + day_to_process[5:7] + '/day=' + day_to_process[-2:] + '/'

    print('Source prefix:' + source_prefix)
    
    target_bucket = 'olho-vivo-etl'
    target_key = 'raw/' + day_to_process[:7] + '/pos-' + day_to_process + '.parquet'

    # List all files in the day directory
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
    files = response.get('Contents', [])

    all_records = []

    for file in files:

        try:
            file_key = file['Key']

            # Get each JSON file
            data = read_json_from_s3(source_bucket, file_key)

            records = []
            for line in data.get('l', []):
                for vehicle in line.get('vs', []):
                    record = {
                        'letreiro': line.get('c'),
                        'codigo_linha': line.get('cl'),
                        'sentido_linha': line.get('sl'),
                        'destino_linha': line.get('lt0'),
                        'origem_linha': line.get('lt1'),
                        'prefixo_veiculo': vehicle.get('p'),
                        'acessibilidade': vehicle.get('a'),
                        'timestamp': int(datetime.datetime.strptime(vehicle.get('ta'), "%Y-%m-%dT%H:%M:%SZ").timestamp()),
                        'py': vehicle.get('py'),
                        'px': vehicle.get('px')
                    }
                    all_records.append(record)
        
        except Exception as e:
            print(f"Error processing file {file_key}: {e}")

    # Create DataFrame
    df = pd.DataFrame(all_records)

    write_parquet_to_s3(target_bucket, target_key, df)

    return {
        'statusCode': 200,
        'body': f'Successfully converted jsons and uploaded to {target_bucket}/{target_key}'
    }
