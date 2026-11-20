import requests
import csv
from datetime import date
import time

BASE_URL="https://api.worldbank.org/v2"

def fetch_indicator(country_code, indicator_code):
    url = f"{BASE_URL}/country/{country_code}/indicator/{indicator_code}"
    params = {"format": "json", "per_page": 100}

    payload = None
    for attempt in range(3):
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            payload = response.json()
            break
        except requests.exceptions.RequestException as error:
            print(f"Essai {attempt + 1}/3 échoué pour {country_code}/{indicator_code} : {error}")
            time.sleep(5)
    if payload is None:
        raise RuntimeError(f"L'API ne répond pas pour {country_code}/{indicator_code}")

    if not isinstance(payload, list) or len(payload) < 2 or payload[1] is None:
        raise ValueError(f"Erreur API pour {country_code}/{indicator_code}: {payload}")

    rows = []
    for item in payload[1]:
        rows.append({
            "country_code": item["countryiso3code"],
            "country_name": item["country"]["value"],
            "indicator_code": item["indicator"]["id"],
            "indicator_label": item["indicator"]["value"],
            "year": int(item["date"]),
            "value": item["value"],
        })
    return rows

COUNTRIES = ["MAR", "TUN", "DZA", "EGY", "FRA"]
INDICATORS = [
    "NY.GDP.MKTP.CD",
    "FP.CPI.TOTL.ZG",
    "SP.POP.TOTL",
    "SL.UEM.TOTL.ZS",
    "SP.DYN.LE00.IN",
]
COLUMNS = [
    "country_code", "country_name", "indicator_code",
    "indicator_label", "year", "value", "load_date",
]


def extract_all(load_date):
    all_rows = []
    for country in COUNTRIES:
        for indicator in INDICATORS:
            rows = fetch_indicator(country, indicator)
            for row in rows:
                if row["value"] is None:
                    continue
                row["load_date"] = load_date
                all_rows.append(row)
    return all_rows


def write_csv(rows, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    rows = extract_all(date.today().isoformat())
    write_csv(rows, "indicators.csv")
    print(f"{len(rows)} lignes ecrites dans indicators.csv")

