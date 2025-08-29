import requests

API_URL = "http://localhost:8000/city"

city_name = "Austin"
country_code = "AUS"  

payload = {
    "name": city_name.strip(),
    "country_code": country_code.strip()
}

response = requests.post(API_URL, json=payload, timeout=10)
if response.status_code == 200:
    print('request is OK')
