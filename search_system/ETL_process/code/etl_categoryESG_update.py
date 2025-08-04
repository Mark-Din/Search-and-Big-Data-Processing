# Hit the API endpoint to update the categoryESG table in MySQL database

import requests

# API endpoint
url = "http://localhost:8081/nodebb/queryCidTid"

data = {
        "theme": 'public',
        "updateSQL": True
    }

    # Make the POST request
response = requests.post(url, data=data)

# Check the response
if response.status_code == 200:
    print("Request was successful")
    print("Response data:", response.json())
else:
    print(f"Request failed with status code {response.status_code}")
    print("Response content:", response.content)

