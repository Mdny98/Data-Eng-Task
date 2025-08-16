

# data_loader.py - Script to load data from Cities folder
import requests
import json
import os
from pathlib import Path
import csv

def load_cities_from_csv(file_path: str) -> list:
    """
    Load cities from CSV file
    Expected format: city,country_code
    """
    cities = []
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if 'city' in row and 'country_code' in row:
                    cities.append({
                        "city": row['city'].strip(),
                        "country_code": row['country_code'].strip().upper()
                    })
    except Exception as e:
        print(f"Error reading CSV file: {e}")
    
    return cities

def load_cities_from_json(file_path: str) -> list:
    """
    Load cities from JSON file
    Expected format: [{"city": "CityName", "country_code": "CC"}, ...]
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data if isinstance(data, list) else []
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return []

def load_cities_to_api(cities: list, api_url: str = "http://localhost:8000"):
    """
    Load cities to API using bulk load endpoint
    """
    if not cities:
        print("No cities to load")
        return
    
    # Prepare bulk load request
    bulk_data = {"cities": cities}
    
    try:
        response = requests.post(
            f"{api_url}/cities/bulk-load",
            json=bulk_data,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"Bulk load successful:")
            print(f"  Created: {result.get('created', 0)}")
            print(f"  Updated: {result.get('updated', 0)}")
            print(f"  Total processed: {result.get('total_processed', 0)}")
            
            if result.get('errors'):
                print(f"  Errors: {len(result['errors'])}")
                for error in result['errors'][:5]:  # Show first 5 errors
                    print(f"    {error}")
        else:
            print(f"Bulk load failed: {response.status_code} - {response.text}")
    
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to API: {e}")

def main():
    """
    Main function to load cities from Cities folder
    """
    # Look for Cities folder in current directory or parent directory
    cities_folder = None
    for path in [Path("Cities"), Path("../Cities"), Path("../../Cities")]:
        if path.exists():
            cities_folder = path
            break
    
    if not cities_folder:
        print("Cities folder not found. Creating sample data...")
        # Create sample data if Cities folder doesn't exist
        sample_cities = [
            {"city": "New York", "country_code": "US"},
            {"city": "London", "country_code": "GB"},
            {"city": "Paris", "country_code": "FR"},
            {"city": "Tokyo", "country_code": "JP"},
            {"city": "Sydney", "country_code": "AU"},
            {"city": "Berlin", "country_code": "DE"},
            {"city": "Toronto", "country_code": "CA"},
            {"city": "Mumbai", "country_code": "IN"},
            {"city": "São Paulo", "country_code": "BR"},
            {"city": "Cairo", "country_code": "EG"}
        ]
        load_cities_to_api(sample_cities)
        return
    
    # Load cities from all files in Cities folder
    all_cities = []
    
    for file_path in cities_folder.glob("*"):
        if file_path.is_file():
            file_ext = file_path.suffix.lower()
            
            if file_ext == '.csv':
                cities = load_cities_from_csv(str(file_path))
                all_cities.extend(cities)
                print(f"Loaded {len(cities)} cities from {file_path.name}")
            
            elif file_ext == '.json':
                cities = load_cities_from_json(str(file_path))
                all_cities.extend(cities)
                print(f"Loaded {len(cities)} cities from {file_path.name}")
    
    if all_cities:
        print(f"\nTotal cities to load: {len(all_cities)}")
        load_cities_to_api(all_cities)
    else:
        print("No valid city data found in Cities folder")

if __name__ == "__main__":
    main()

---