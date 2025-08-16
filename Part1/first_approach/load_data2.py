import os
import requests
import csv

API_URL = "http://localhost:8000/city"

for file in os.listdir('./Attachments/Cities'):
    
    if file.endswith(".csv"):
        with open(os.path.join("./Attachments/Cities", file), newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            num=0
            for row in reader:
                payload = {
                    "name": row["city"],
                    "country_code": row["countyCode"]
                    
                }
                num+=1
                print(num)
                requests.post(API_URL, json=payload)
