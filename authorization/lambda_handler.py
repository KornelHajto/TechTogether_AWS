import json
import boto3

def load_token():
    s3 = boto3.client('s3')
    bucket_name = 'candatabucketpeengineers'
    filename = "token.txt"
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=filename)
        data = obj['Body'].read().decode('utf-8').strip()
        token_data = json.loads(data)  # Parse JSON properly
        return token_data.get("token", "").strip()  # Extract token field
    except Exception as e:
        print(f"Error loading Token file from S3: {e}")
        return None

def generate_policy(principal_id, effect, resource):
    return {
        'principalId': principal_id,
        'policyDocument': {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Action': 'execute-api:Invoke',
                    'Effect': effect,
                    'Resource': resource
                }
            ]
        }
    }

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}")  # Log full event

    # Extract token from either headers or identitySource
    token = event.get('headers', {}).get('authorization') or event.get('identitySource', [None])[0]
    
    print(f"Extracted token: {token}")  # Debugging

    if not token:
        print("Error: No authorization token found.")
        return generate_policy('user', 'Deny', event.get('routeArn', '*'))

    # Remove "Bearer " prefix if present
    token = token.replace("Bearer ", "").strip()

    valid_token = load_token()
    print(f"Valid token from S3: {valid_token}")  # Debugging

    if not valid_token:
        print("Error: Unable to retrieve valid token from S3.")
        return generate_policy('user', 'Deny', event.get('routeArn', '*'))

    # Compare tokens
    if token == valid_token:
        print("Authorization successful!")
        return generate_policy('user', 'Allow', event['routeArn'])
    else:
        print("Authorization failed! Token mismatch.")
        return generate_policy('user', 'Deny', event['routeArn'])
