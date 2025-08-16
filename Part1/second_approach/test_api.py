# test_api.py - Test script for the API
import requests
import json
import time
import random

def test_api():
    """
    Test the API endpoints
    """
    base_url = "http://localhost:8000"
    
    # Test health endpoint
    print("1. Testing health endpoint...")
    response = requests.get(f"{base_url}/health")
    print(f"Health check: {response.status_code} - {response.json()}")
    
    # Test creating cities
    print("\n2. Testing city creation...")
    test_cities = [
        {"city": "Amsterdam", "country_code": "NL"},
        {"city": "Barcelona", "country_code": "ES"},
        {"city": "Prague", "country_code": "CZ"}
    ]
    
    for city_data in test_cities:
        response = requests.post(f"{base_url}/cities", json=city_data)
        print(f"Create {city_data['city']}: {response.status_code} - {response.json().get('message')}")
    
    # Test retrieving cities (this will generate cache misses and hits)
    print("\n3. Testing city retrieval...")
    test_queries = ["Amsterdam", "Barcelona", "Prague", "Amsterdam", "NonExistentCity"]
    
    for city in test_queries:
        try:
            response = requests.get(f"{base_url}/cities/{city}")
            if response.status_code == 200:
                result = response.json()
                print(f"Get {city}: {result['country_code']} (from {result['source']})")
            else:
                print(f"Get {city}: {response.status_code} - {response.json().get('detail')}")
        except Exception as e:
            print(f"Error querying {city}: {e}")
        
        time.sleep(0.1)  # Small delay between requests
    
    # Test stats endpoint
    print("\n4. Testing stats endpoint...")
    response = requests.get(f"{base_url}/stats")
    print(f"Stats: {response.json()}")
    
    # Test listing cities
    print("\n5. Testing city listing...")
    response = requests.get(f"{base_url}/cities?limit=5")
    print(f"List cities: {len(response.json().get('cities', []))} cities returned")

if __name__ == "__main__":
    print("Testing City Country Code API...")
    print("Make sure the API is running on http://localhost:8000")
    time.sleep(2)
    test_api()