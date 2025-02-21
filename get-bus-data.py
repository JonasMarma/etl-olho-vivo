import json
import os
import requests
import boto3
import datetime

def get_api_key():
    ssm = boto3.client("ssm", region_name="us-east-1")
    response = ssm.get_parameter(Name="/chave-api-olho-vivo", WithDecryption=True)
    return response["Parameter"]["Value"]

def get_session_cookie():
    # Create a session object
    session = requests.Session()

    # Step 1: Authenticate and store the cookie
    #precisa colocar o token aqui!
    #eventualmente de forma segura
    auth_url = "http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token=" + get_api_key()

    # Send the authentication request
    response = session.post(auth_url)

    # Check if authentication was successful
    if response.status_code == 200:
        print("Authentication successful!")
        #print("Cookies stored in session:", session.cookies.get_dict())
        return session
    else:
        print("Authentication failed:", response.status_code, response.text)
        return None

def save_json(json_data, bucket_name, key):

    print("Saving JSON to S3...")

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

    s = get_session_cookie()

    data_url = 'http://api.olhovivo.sptrans.com.br/v2.1/Posicao'

    # Send a GET request using the same session
    data_response = s.get(data_url)

    # Print the response
    if data_response.status_code == 200:
        print("Data retrieved successfully!")
        #print(data_response.json())  # Assuming the response is JSON
        save_json(data_response.json(), 'olho-vivo-raw', 'teste2.json')

    else:
        print("Failed to retrieve data:", data_response.status_code, data_response.text)


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
