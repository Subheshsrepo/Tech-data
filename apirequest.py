import requests
response = requests.get('https://api.github.com')
data = response.json()
length = len(data)
print("Response : ", "length of Data : ",+ length)