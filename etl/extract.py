import requests
import os 
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

url = "https://api.vitrineretail.app/api/indicatorsStores/exportXlsxNew"

def generate_monthly_periods(start_date, end_date):
    current = start_date

    while current < end_date:
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)

        yield current, min(next_month, end_date)
        current = next_month

def extract_data():
    token = os.getenv("API_TOKEN")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    start_date = datetime(2026, 1, 1)
    end_date = datetime.today()

    all_data = []

    for inicio, fim in generate_monthly_periods(start_date, end_date):
        print(f"Buscando: {inicio.date()} até {fim.date()}")

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
                "startDate": inicio.strftime("%Y-%m-%d"),
                "endDate": fim.strftime("%Y-%m-%d")
            }
        }

        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()

            data = response.json()
            print(f"Registros retornados: {len(data)}")

            all_data.extend(data)
        
        except Exception as e:
            print(f"Erro no período {inicio.date()} - {fim.date()}: {e}")

    print(f"\nTotal coletado: {len(all_data)} registros")

    return all_data