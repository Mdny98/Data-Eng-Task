import os
import requests
import csv
import time

# Use the containerized FastAPI service
API_URL = "http://localhost:8000/city"

def wait_for_api():
    
    """Wait for the API to be available"""
    max_retries = 30
    for i in range(max_retries):
        try:
            response = requests.get("http://localhost:8000/health", timeout=5)
            if response.status_code == 200:
                print("API is ready!")
                return True
        except requests.exceptions.RequestException:
            pass
        print(f"Waiting for API... (attempt {i+1}/{max_retries})")
        time.sleep(2)
    return False

def load_cities_data():
    if not wait_for_api():
        print("API is not available. Exiting...")
        return
    
    total_cities = 0
    successful_inserts = 0
    
    # Process all CSV files in the Attachments/Cities directory
    cities_dir = "../Attachments/Cities"
    if not os.path.exists(cities_dir):
        print(f"Directory {cities_dir} does not exist!")
        return
    
    for file in os.listdir(cities_dir):
        if file.endswith(".csv"):
            print(f"Processing file: {file}")
            
            with open(os.path.join(cities_dir, file), newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row in reader:
                    total_cities += 1
                    
                    # Handle different possible column names
                    city_name = row.get("city") or row.get("City") or row.get("name") or row.get("Name")
                    country_code = row.get("countyCode") or row.get("country_code") or row.get("CountryCode")
                    
                    if not city_name or not country_code:
                        print(f"Skipping row {total_cities}: missing city name or country code")
                        continue
                    
                    payload = {
                        "name": city_name.strip(),
                        "country_code": country_code.strip()
                    }
                    
                    try:
                        response = requests.post(API_URL, json=payload, timeout=10)
                        if response.status_code == 200:
                            successful_inserts += 1
                            if successful_inserts % 100 == 0:  # Progress indicator
                                print(f"Inserted {successful_inserts} cities...")
                        else:
                            print(f"Failed to insert {city_name}: {response.status_code} - {response.text}")
                    except requests.exceptions.RequestException as e:
                        print(f"Error inserting {city_name}: {e}")
                    
    print(f"\nData loading complete!")
    print(f"Total cities processed: {total_cities}")
    print(f"Successfully inserted: {successful_inserts}")
    print(f"Failed: {total_cities - successful_inserts}")

if __name__ == "__main__":
    load_cities_data()
