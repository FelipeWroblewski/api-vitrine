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

    all_data = []
    page = 1

    while True:
        payload = {
            "filters": {
                "format": "json",
                "page": page,
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
                "endDate": datetime.today().strftime("%Y-%m-%d")
            }
        }

        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()

            data = response.json()

            if not data:
                print(f"Página {page} vazia - fim do período")
                break

            print(f"Página {page} - {len(data)} registros")
            all_data.extend(data)
            page += 1
        
        except Exception as e:
            print(f"Erro na página {page}: {e}")
            break

    print(f"\nTotal coletado: {len(all_data)} registros")

    return all_data