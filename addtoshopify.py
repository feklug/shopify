import requests
import os
import json
from datetime import datetime
import concurrent.futures
import time
import math
from typing import List, Dict, Optional, Union
from ratelimit import limits, sleep_and_retry

# Konfiguration
CONFIG = {
    "CACHE_TTL": 300,  # 5 Minuten Cache Gültigkeit
    "MAX_WORKERS": 2,  # Reduzierte Worker-Anzahl für Rate Limits
    "DEFAULT_STOCK": 1000,
    "PRICE_MARKUP": 1.075,  # 7.5% Aufschlag
    "PRICE_ROUNDING": 0.99,
    "API_VERSION": "2024-01",
    "REQUEST_DELAY": 0.5  # Pause zwischen Anfragen in Sekunden
}

# Shopify-Zugangsdaten
access_token = os.getenv("SHOPIFY_TOKEN")
shop_url = os.getenv("SHOPIFY_URL")
api_version = "2024-01"
LOCATION_ID = "108058247432"

# API-Endpunkte
base_url = f"https://{shop_url}/admin/api/{CONFIG['API_VERSION']}"
api_url = f"{base_url}/products.json"
product_url = f"{base_url}/products/"
inventory_url = f"{base_url}/inventory_levels/set.json"

headers = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": access_token
}

# Cache für vorhandene Produkte
existing_products_cache = None
last_cache_update = 0

# Rate Limited API Request Funktion
@sleep_and_retry
@limits(calls=2, period=1)  # 2 Anfragen pro Sekunde
def make_shopify_request(url: str, method: str = "GET", json_data: Optional[Dict] = None, max_retries: int = 3) -> Optional[requests.Response]:
    retries = 0
    while retries < max_retries:
        try:
            if method == "GET":
                response = requests.get(url, headers=headers)
            elif method == "POST":
                response = requests.post(url, headers=headers, json=json_data)
            elif method == "PUT":
                response = requests.put(url, headers=headers, json=json_data)

            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.headers.get('Retry-After', 10))
                time.sleep(retry_after)
                continue
            retries += 1
            if retries == max_retries:
                print(f"❌ Fehler bei API-Anfrage: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    print(f"Fehlerdetails: {e.response.text}")
                return None
            time.sleep(2 ** retries)  # Exponentielles Backoff
        except requests.exceptions.RequestException as e:
            retries += 1
            if retries == max_retries:
                print(f"❌ Verbindungsfehler: {e}")
                return None
            time.sleep(2 ** retries)

def calculate_adjusted_price(original_price: Union[str, float]) -> float:
    """
    Berechnet den angepassten Preis mit Aufschlag und Rundung.
    
    Args:
        original_price: Originalpreis als String (mit €) oder Zahl
        
    Returns:
        Angepasster Preis mit 7.5% Aufschlag, gerundet auf .99
        
    Raises:
        ValueError: Wenn Preis nicht konvertiert werden kann
    """
    try:
        if isinstance(original_price, str):
            original_price = float(original_price.replace("€", "").strip())
        
        increased_price = original_price * CONFIG["PRICE_MARKUP"]
        
        if increased_price % 1 < CONFIG["PRICE_ROUNDING"]:
            adjusted_price = math.floor(increased_price) + CONFIG["PRICE_ROUNDING"]
        else:
            adjusted_price = math.ceil(increased_price) + CONFIG["PRICE_ROUNDING"]
        
        return round(adjusted_price, 2)
    except (ValueError, TypeError) as e:
        print(f"⚠️ Preisberechnungsfehler für {original_price}: {e}")
        return original_price  # Fallback zum Originalpreis

def get_existing_products(force_refresh: bool = False) -> List[Dict]:
    global existing_products_cache, last_cache_update

    current_time = time.time()
    if (force_refresh or 
        existing_products_cache is None or 
        (current_time - last_cache_update) > CONFIG["CACHE_TTL"]):
        
        print("🔄 Aktualisiere Produkt-Cache...")
        time.sleep(CONFIG["REQUEST_DELAY"])  # Rate Limit beachten
        
        all_products = []
        url = f"{api_url}?limit=250"

        while url:
            response = make_shopify_request(url)
            if not response:
                break

            products = response.json().get("products", [])
            all_products.extend(products)

            # Paginierung
            if 'Link' in response.headers:
                links = response.headers['Link']
                next_page_url = None
                for link in links.split(','):
                    if 'rel="next"' in link:
                        next_page_url = link[link.find('<') + 1:link.find('>')]
                        break
                url = next_page_url
                time.sleep(CONFIG["REQUEST_DELAY"])  # Pause zwischen Seiten
            else:
                url = None

        existing_products_cache = all_products
        last_cache_update = current_time

    return existing_products_cache

def update_inventory(inventory_item_id: str, available: bool) -> bool:
    """Aktualisiert den Lagerbestand für ein Inventaritem"""
    time.sleep(CONFIG["REQUEST_DELAY"])  # Rate Limit beachten
    
    payload = {
        "location_id": LOCATION_ID,
        "inventory_item_id": inventory_item_id,
        "available": CONFIG["DEFAULT_STOCK"] if available else 0
    }
    
    response = make_shopify_request(inventory_url, method="POST", json_data=payload)
    return response is not None

def build_product_payload(product_data: Dict, is_update: bool = False) -> Dict:
    """Erstellt den Payload für Produktcreate/update"""
    payload = {
        "product": {
            "title": product_data["title"],
            "body_html": product_data.get("body_html", ""),
            "variants": [],
            "images": []
        }
    }

    # Optionen nur bei mehreren Varianten
    if len(product_data["variants"]) > 1 or any(v.get("variant_title") for v in product_data["variants"]):
        payload["product"]["options"] = [{"name": "Size"}]

    # Veröffentlichungsdatum
    published_at = product_data.get("published_at")
    if published_at:
        try:
            if isinstance(published_at, str):
                datetime.fromisoformat(published_at)
                payload["product"]["published_at"] = published_at
        except ValueError as e:
            print(f"⚠️ Ungültiges published_at Format: {e}")

    # Metadaten
    metadata_fields = ["vendor", "product_type", "tags", "handle", "created_at", "updated_at"]
    for field in metadata_fields:
        if field in product_data:
            payload["product"][field] = product_data[field]

    # Bilder (ohne Duplikate)
    image_urls = set()
    for variant in product_data["variants"]:
        for img_url in variant.get("images", []):
            image_urls.add(img_url)
    payload["product"]["images"] = [{"src": img} for img in image_urls]

    # Varianten
    for variant in product_data["variants"]:
        try:
            original_price = variant["price"]
            adjusted_price = calculate_adjusted_price(original_price)
            
            variant_payload = {
                "price": str(adjusted_price),
                "sku": variant["sku"],
                "inventory_quantity": CONFIG["DEFAULT_STOCK"] if variant["available"] else 0,
                "inventory_management": "shopify",
                "inventory_policy": "deny"
            }

            if "variant_title" in variant:
                variant_payload["option1"] = variant["variant_title"]

            # Optionale Felder
            optional_fields = ["barcode", "weight", "weight_unit", "taxable", "compare_at_price"]
            for field in optional_fields:
                if field in variant:
                    variant_payload[field] = variant[field]

            payload["product"]["variants"].append(variant_payload)
            
            print(f"💰 Preis angepasst: {original_price} → {adjusted_price}")
            
        except KeyError as e:
            print(f"⚠️ Wichtiges Variantenfeld fehlt: {e}")

    return payload

def validate_product_data(product: Dict) -> bool:
    """Validiert die Produktdaten"""
    required = ["title", "variants"]
    if not all(field in product for field in required):
        return False
    
    if not isinstance(product["variants"], list) or not product["variants"]:
        return False
    
    for v in product["variants"]:
        if not all(k in v for k in ["sku", "price", "available"]):
            return False
        if not isinstance(v["available"], bool):
            return False
            
    return True

def process_product(product: Dict, existing_products: List[Dict]) -> bool:
    """Verarbeitet ein einzelnes Produkt"""
    try:
        if not validate_product_data(product):
            print("❌ Ungültige Produktdaten")
            return False

        # Finde vorhandenes Produkt anhand der SKUs
        product_skus = {v["sku"] for v in product["variants"] if "sku" in v}
        existing_product = None
        variant_map = {}

        for p in existing_products:
            for v in p["variants"]:
                if v.get("sku") in product_skus:
                    existing_product = p
                    variant_map[v["sku"]] = v
            
            if existing_product:
                break

        if existing_product:
            print(f"🔄 Produkt '{product['title']}' existiert (ID: {existing_product['id']})")
            
            success = True
            for variant in product["variants"]:
                existing_variant = variant_map.get(variant["sku"])
                
                if existing_variant:
                    if not update_inventory(
                        existing_variant["inventory_item_id"],
                        variant["available"]
                    ):
                        success = False
                else:
                    print(f"⚠️ Neue Variante {variant['sku']} wird hinzugefügt")
                    success = False

            if success and len(variant_map) == len(product["variants"]):
                product_payload = build_product_payload(product, is_update=True)
                product_payload["product"]["id"] = existing_product["id"]
                
                response = make_shopify_request(
                    f"{product_url}{existing_product['id']}.json",
                    method="PUT",
                    json_data=product_payload
                )
                
                return response is not None
            else:
                print("⚠️ Nicht alle Varianten konnten aktualisiert werden")
                return False

        else:
            print(f"➕ Produkt '{product['title']}' existiert noch nicht. Füge hinzu...")
            
            if not any(v.get("available", False) for v in product["variants"]):
                print(f"⚠️ Keine verfügbaren Varianten, überspringe Produkt")
                return False

            product_payload = build_product_payload(product)
            response = make_shopify_request(api_url, method="POST", json_data=product_payload)

            if response:
                print(f"✅ Produkt mit angepasstem Preis hinzugefügt")
                
                if "published_at" not in product:
                    update_payload = {
                        "product": {
                            "id": response.json()["product"]["id"],
                            "published_at": datetime.now().isoformat()
                        }
                    }
                    make_shopify_request(
                        f"{product_url}{response.json()['product']['id']}.json",
                        method="PUT",
                        json_data=update_payload
                    )
                return True
            else:
                print(f"❌ Fehler beim Hinzufügen")
                return False

    except Exception as e:
        print(f"❌ Unerwarteter Fehler: {e}")
        return False

def process_brand_file(brand_file: str) -> int:
    """Verarbeitet eine Markendatei mit Produkten"""
    try:
        with open(brand_file, 'r', encoding='utf-8') as json_file:
            products_data = json.load(json_file)

        brand_name = os.path.basename(brand_file).split('.')[0]
        print(f"🔍 Verarbeite {brand_name} mit {len(products_data)} Produkten...")

        existing_products = get_existing_products()

        success_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as executor:
            futures = []
            for product in products_data:
                futures.append(executor.submit(process_product, product, existing_products))
                time.sleep(CONFIG["REQUEST_DELAY"])  # Pause zwischen Produkten

            for future in concurrent.futures.as_completed(futures):
                if future.result():
                    success_count += 1

        print(f"✅ {success_count}/{len(products_data)} Produkte aus {brand_name} erfolgreich verarbeitet!")
        return success_count

    except Exception as e:
        print(f"❌ Fehler beim Verarbeiten von {brand_file}: {e}")
        return 0

# Liste der Markendateien
brand_files = [
    'output/pesoclo.json',
    'output/6pm.json',
    'output/trendtvision.json',
    'output/reternity.json',
    'output/Systemic.json',
    'output/Vicinity.json',
    'output/derschutze.json',
    'output/MoreMoneyMoreLove.json',
    'output/Devourarchive.json',
    'output/statement-clo.json',
    'output/mosquets.json',
    'output/vacid.json',
    'output/root-atelier.json',
    'output/olakala.json',
    'output/eightyfiveclo.json',
    'output/atelier-roupa.json',
    'output/tarmac.clothing.json',
    'output/sourire-worldwide.json',
    'output/liju-gallery.json',
    'output/sacralite.json',
    'output/unvainstudios.json',
    'output/hunidesign.json',
    'output/deputydepartment.json',
    'output/99based.json'
]

if __name__ == "__main__":
    start_time = time.time()
    total_processed = 0
    seen_skus = set()

    # SKUs sammeln
    for brand_file in brand_files:
        try:
            with open(brand_file, 'r', encoding='utf-8') as f:
                products = json.load(f)
                for product in products:
                    for variant in product.get("variants", []):
                        if "sku" in variant:
                            seen_skus.add(variant["sku"])
        except Exception as e:
            print(f"❌ Fehler beim Lesen von {brand_file}: {e}")

    # Produkte verarbeiten
    for brand_file in brand_files:
        total_processed += process_brand_file(brand_file)
        get_existing_products(force_refresh=True)

    # Veraltete Produkte deaktivieren
    existing_products = get_existing_products(force_refresh=True)
    disabled_count = 0

    for product in existing_products:
        for variant in product["variants"]:
            sku = variant.get("sku")
            if sku and sku not in seen_skus:
                if update_inventory(variant["inventory_item_id"], available=False):
                    print(f"🚫 Bestand auf 0 für SKU {sku}")
                    disabled_count += 1

    total_time = time.time() - start_time
    print(f"✅ Bestand für {disabled_count} veraltete Produkte auf 0 gesetzt.")
    print(f"🏎️ Alle Dateien verarbeitet! Insgesamt {total_processed} Produkte in {total_time:.2f} Sekunden.")
