import requests
import os 
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

url = "https://api.vitrineretail.app/api/indicatorsStores/exportXlsxNew"

def extract_data():
    token = os.getenv("API_TOKEN")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    hoje = datetime.today().strftime("%Y-%m-%d")

    payload = {
        "filters": {
            "format": "json",
            "filterType": "store",
            "manuals": [],
            "groups": [],
            "stores": [],
            "spaces": [],
            "categories": [],
            "responsibles": [],
            "comercials": [],
            "viewers": [],
            "startDate": "2026-01-01",
            "endDate": hoje
        }
    }

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()

    data = response.json()

    #print("Quantidade de registros:", len(data))
    #print("Primeiros registros:")
    #print(data[:2])

    return data