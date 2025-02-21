import json
import requests
import boto3
import datetime

def get_api_key():
    ssm = boto3.client("ssm", region_name="us-east-1")
    response = ssm.get_parameter(Name="/chave-api-olho-vivo", WithDecryption=True)
    return response["Parameter"]["Value"]

def get_session_cookie():

    # Fazer a autenticação
    auth_url = "http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token=" + get_api_key()
    session = requests.Session()
    response = session.post(auth_url)

    # Verificar se a autenticação funcionou
    if response.status_code == 200:
        print("Autenticação bem sucedida!")
        return session
    else:
        print("Falha de autenticação:", response.status_code, response.text)
        return None

def save_json(json_data, bucket_name, key):

    # Convert JSON to a string
    json_string = json.dumps(json_data)

    # Upload to S3
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json_string,
            ContentType="application/json"
        )
        return {
            "statusCode": 200,
            "body": f"JSON saved successfully to s3 as"
        }
    except Exception as e:
        print(f"Error saving JSON to S3: {e}")
        return {
            "statusCode": 500,
            "body": f"Failed to save JSON: {str(e)}"
        }

def lambda_handler(event, context):

    # Fazer autenticação na API - é usado um cookie
    s = get_session_cookie()

    data_url = 'http://api.olhovivo.sptrans.com.br/v2.1/Posicao'

    # Obter as posições
    data_response = s.get(data_url)

    destiantion_bucket = 'olho-vivo-raw'
    

    now = datetime.datetime.utcnow()
    s3_key = f"year={now.year}/month={now.month:02}/day={now.day:02}/hour={now.hour:02}/data_{now.strftime('%Y-%m-%dT%H-%M-%S-UTC-0')}.json"


    if data_response.status_code == 200:
        print("Sucesso na obtenção dos dados! Salvando em:")
        print(destiantion_bucket + '/')
        save_json(data_response.json(), destiantion_bucket, s3_key)

    else:
        print("Falha na obtenção dos dados de posição", data_response.status_code, data_response.text)


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
