import json
import boto3

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("can_data")  # Ensure your table exists with correct schema


def parse_can_data(line):
    """
    Parses a CAN data line into its components: timestamp, interface, CAN ID, and data.
    """
    try:
        if not isinstance(line, str) or ')' not in line or '#' not in line:
            raise ValueError("Invalid CAN data format")

        timestamp_part, rest = line.split(") ", 1)
        timestamp = timestamp_part[1:]  # Remove the opening parenthesis

        if ' ' not in rest or '#' not in rest:
            raise ValueError("Malformed CAN data")

        interface, can_id_data = rest.split(" ", 1)
        can_id, data = can_id_data.split("#", 1)

        return {
            "timestamp": timestamp.strip(),
            "can_id": can_id.strip(),
            "interface": interface.strip(),
            "data": data.strip()
        }

    except ValueError as e:
        return {"error": f"Failed to parse CAN data: {str(e)}"}


def upload_to_dynamodb(parsed_data):
    """
    Uploads parsed CAN data to DynamoDB.
    Supports both single entry and batch uploads.
    """
    try:
        if isinstance(parsed_data, list):  # Multiple entries
            with table.batch_writer() as batch:
                for item in parsed_data:
                    if "error" not in item:  # Skip invalid data
                        batch.put_item(Item=item)
        elif isinstance(parsed_data, dict):  # Single entry
            if "error" not in parsed_data:
                table.put_item(Item=parsed_data)

        return True

    except Exception as e:
        return str(e)


def lambda_handler(event, context):
    """
    AWS Lambda handler to process incoming CAN data and store it in DynamoDB.
    """
    try:
        # Ensure the event body exists and is a valid JSON string
        if "body" not in event or not event["body"]:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Bad Request, missing body in event"})
            }

        try:
            body = json.loads(event["body"])  # Convert JSON string to dictionary
        except json.JSONDecodeError:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Invalid JSON format in request body"})
            }

        # Validate `canData` field
        can_data = body.get("canData")

        if isinstance(can_data, str):  
            can_list = [can_data]  # Convert single string to a list
        elif isinstance(can_data, list):
            if not all(isinstance(item, str) for item in can_data):
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": "Invalid data: all canData items must be strings"})
                }
            can_list = can_data
        else:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Bad Request, canData must be a string or a list of strings"})
            }

        # Parse CAN data
        parsed_data = [parse_can_data(line) for line in can_list]

        # Upload to DynamoDB
        upload_result = upload_to_dynamodb(parsed_data)

        if upload_result is not True:
            return {
                "statusCode": 500,
                "body": json.dumps({"error": "Failed to upload data to DynamoDB", "details": upload_result})
            }

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Data uploaded successfully", "uploaded_data_count": len(parsed_data)})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal Server Error", "details": str(e)})
        }
