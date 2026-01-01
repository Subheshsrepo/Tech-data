import boto3
import csv
import io

# Initialize DynamoDB and S3 clients
dynamo_client = boto3.client('dynamodb', region_name='us-east-1')
s3_client = boto3.client('s3', region_name='us-east-1')

bucket_name = "Your S3 Bucket Name"  # replace with your actual bucket name
file_path = "myfile1.csv"

def lambda_handler(event, context):
    try:
        # Set up parameters for the DynamoDB scan operation
        scan_params = {
            'TableName': 'mydynamoDB',
            'Limit': 10
        }

        # Perform the DynamoDB scan operation
        data = dynamo_client.scan(**scan_params)

        if 'Items' not in data or len(data['Items']) == 0:
            print("No data found in DynamoDB.")
            return {
                'statusCode': 200,
                'body': "No data found."
            }

        print("DynamoDB Data Retrieved:", data['Items'])

        # Convert DynamoDB data to CSV format
        csv_data = json_to_csv(data['Items'])
        print("\nCSV File Content\n", csv_data)

        # Upload CSV data to S3
        put_object_to_s3(bucket_name, file_path, csv_data)

        return {
            'statusCode': 200,
            'body': "Success"
        }
    except Exception as e:
        print("Error:", e)
        return {
            'statusCode': 500,
            'body': str(e)
        }

# Function to convert DynamoDB JSON array to CSV format
def json_to_csv(items):
    fields = ["SID", "Name", "Course", "Duration"]

    # Use StringIO to build CSV in memory
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()

    for element in items:
        # DynamoDB returns AttributeValues, so unwrap them
        row = {
            "SID": element["SID"]["S"],
            "Name": element["Name"]["S"],
            "Course": element["Course"]["S"],
            "Duration": element["Duration"]["N"]
        }
        writer.writerow(row)

    return output.getvalue()

# Function to upload data to S3
def put_object_to_s3(bucket, key, data):
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data
        )
        print(f"\n\nFile {key} Created in S3\n")
    except Exception as e:
        print("S3 Upload Error:", e)
        raise
