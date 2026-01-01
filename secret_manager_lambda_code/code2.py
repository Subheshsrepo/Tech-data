import json
import boto3

def lambda_handler(event, context):
	# Input values
	Table_name = 'Your Table Name'
	AWS_Access_Key = 'YourAWSAccessKey'
	AWS_Secret_Access_Key = 'YourAWSSecretAccessKey'

	# Create a DynamoDB table
	print('DynamoDB Table creation started.')
 
	dynamodb = boto3.resource(
		'dynamodb',
		aws_access_key_id = AWS_Access_Key,
    	          aws_secret_access_key = AWS_Secret_Access_Key,
		region_name = 'us-east-1'
	)

	# Connect to table & Scan the entire table
	table = dynamodb.Table(Table_name)
	response = table.scan()

	print('---------------------------------------')
	print('------------STUDENT DETAILS------------')
	print('---------------------------------------')
	for item in response['Items']:
		print('Student Id : ', item['StudId'])
		print('Student Name : ', item['FirstName'], ' ', item['LastName'])
		print('Student Department : ', item['Dept'])
		print('Student Age : ', item['Age'])
		print('_______________________________')
	print('---------------------------------------')