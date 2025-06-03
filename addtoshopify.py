import requests
import os
import json
from datetime import datetime
import concurrent.futures
import time
import math
from threading import Lock

# Shopify-Zugangsdaten
access_token = os.getenv("SHOPIFY_TOKEN")
shop_url = os.getenv("SHOPIFY_URL")
api_version = "2024-01"
LOCATION_ID = "108058247432"

# API-Endpunkte
api_url = f"https://{shop_url}/admin/api/{api_version}/products.json"
product_url = f"https://{shop_url}/admin/api/{api_version}/products/"
inventory_url = f"https://{shop_url}/admin/api/{api_version}/inventory_levels/set.json"

headers = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": access_token
}

# Rate Limiting Variablen
REQUEST_LOCK = Lock()
LAST_REQUEST_TIME = 0
MIN_REQUEST_INTERVAL = 0.5  # 500ms zwischen Anfragen (2/s)
MAX_WORKERS = 2  # Reduzierte Worker-Anzahl

# Cache für vorhandene Produkte
existing_products_cache = None
last_cache_update = 0
CACHE_TTL = 300  # 5 Minuten Cache Gültigkeit

def calculate_adjusted_price(original_price):
    """
    Berechnet den angepassten Preis:
    1. Fügt 7.5% zum Originalpreis hinzu
    2. Rundet auf X.99 auf
    """
    try:
        if isinstance(original_price, str):
            original_price = float(original_price.replace("€", "").strip())
        
        increased_price = original_price * 1.075
        
        if increased_price % 1 < 0.99:
            adjusted_price = math.floor(increased_price) + 0.99
        else:
            adjusted_price = math.ceil(increased_price) + 0.99
        
        return round(adjusted_price, 2)
    except (ValueError, TypeError) as e:
        print(f"⚠️ Preisberechnungsfehler für {original_price}: {e}")
        return original_price

def make_shopify_request(url, method="GET", json_data=None, max_retries=3):
    global LAST_REQUEST_TIME
    
    retries = 0
    while retries < max_retries:
        try:
            with REQUEST_LOCK:
                # Rate Limiting
                elapsed = time.time() - LAST_REQUEST_TIME
                if elapsed < MIN_REQUEST_INTERVAL:
                    sleep_time = MIN_REQUEST_INTERVAL - elapsed
                    time.sleep(sleep_time)
                
                LAST_REQUEST_TIME = time.time()
                
                if method == "GET":
                    response = requests.get(url, headers=headers)
                elif method == "POST":
                    response = requests.post(url, headers=headers, json=json_data)
                elif method == "PUT":
                    response = requests.put(url, headers=headers, json=json_data)

                # Handle Shopify Rate Limits
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 10))
                    print(f"⏳ Rate Limit erreicht - warte {retry_after}s")
                    time.sleep(retry_after)
                    continue
                    
                response.raise_for_status()
                return response
                
        except requests.exceptions.RequestException as e:
            retries += 1
            if retries == max_retries:
                print(f"❌ Fehler bei API-Anfrage: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    print(f"Fehlerdetails: {e.response.text}")
                return None
            time.sleep(2 ** retries)

def get_existing_products(force_refresh=False):
    global existing_products_cache, last_cache_update

    current_time = time.time()
    if force_refresh or existing_products_cache is None or (current_time - last_cache_update) > CACHE_TTL:
        print("🔄 Aktualisiere Produkt-Cache...")
        all_products = []
        url = f"{api_url}?limit=250"

        while url:
            response = make_shopify_request(url)
            if response:
                products = response.json().get("products", [])
                all_products.extend(products)

                if 'Link' in response.headers:
                    links = response.headers['Link']
                    next_page_url = None
                    for link in links.split(','):
                        if 'rel="next"' in link:
                            next_page_url = link[link.find('<') + 1:link.find('>')]
                            break
                    url = next_page_url
                else:
                    break
            else:
                break

        existing_products_cache = all_products
        last_cache_update = current_time

    return existing_products_cache

def update_inventory_bulk(inventory_updates):
    """Aktualisiert mehrere Inventory-Items in einem Request"""
    if not inventory_updates:
        return True
        
    payload = {
        "location_id": LOCATION_ID,
        "updates": [
            {
                "inventory_item_id": item["inventory_item_id"],
                "available": 1000 if item["available"] else 0
            }
            for item in inventory_updates
        ]
    }
    
    response = make_shopify_request(inventory_url, method="POST", json_data=payload)
    return response is not None

def build_product_payload(product_data, is_update=False):
    payload = {
        "product": {
            "title": product_data["title"],
            "body_html": product_data.get("body_html", ""),
            "variants": [],
            "images": []
        }
    }

    if len(product_data["variants"]) > 1 or any(v.get("variant_title") for v in product_data["variants"]):
        payload["product"]["options"] = [{"name": "Size"}]

    published_at = product_data.get("published_at")
    if published_at:
        try:
            if isinstance(published_at, str):
                datetime.fromisoformat(published_at)
                payload["product"]["published_at"] = published_at
        except ValueError:
            pass
    else:
        payload["product"]["published_at"] = datetime.now().isoformat()

    # Metadaten
    for field in ["vendor", "product_type", "tags", "handle", "created_at", "updated_at"]:
        if field in product_data:
            payload["product"][field] = product_data[field]

    # Bilder
    image_urls = set()
    for variant in product_data["variants"]:
        for img_url in variant.get("images", []):
            image_urls.add(img_url)
    payload["product"]["images"] = [{"src": img} for img in image_urls]

    # Varianten
    for variant in product_data["variants"]:
        try:
            adjusted_price = calculate_adjusted_price(variant["price"])
            
            variant_payload = {
                "price": str(adjusted_price),
                "sku": variant["sku"],
                "inventory_quantity": 1000 if variant["available"] else 0,
                "inventory_management": "shopify",
                "inventory_policy": "deny"
            }

            if "variant_title" in variant:
                variant_payload["option1"] = variant["variant_title"]

            for field in ["barcode", "weight", "weight_unit", "taxable", "compare_at_price"]:
                if field in variant:
                    variant_payload[field] = variant[field]

            payload["product"]["variants"].append(variant_payload)
            print(f"💰 Preis angepasst: {variant['price']} → {adjusted_price}")
            
        except KeyError as e:
            print(f"⚠️ Wichtiges Variantenfeld fehlt: {e}")

    return payload

def process_product(product, existing_products):
    try:
        if not validate_product_data(product):
            print("❌ Ungültige Produktdaten")
            return False

        # Erweitere Duplikatsprüfung
        product_skus = {v["sku"] for v in product["variants"]}
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
            
            # Sammle Inventory-Updates
            inventory_updates = []
            missing_variants = False
            
            for variant in product["variants"]:
                existing_variant = variant_map.get(variant["sku"])
                if existing_variant:
                    inventory_updates.append({
                        "inventory_item_id": existing_variant["inventory_item_id"],
                        "available": variant["available"]
                    })
                else:
                    print(f"⚠️ Neue Variante {variant['sku']}")
                    missing_variants = True

            # Führe Bulk-Update durch
            if inventory_updates:
                if not update_inventory_bulk(inventory_updates):
                    print("❌ Inventory-Update fehlgeschlagen")
                    return False

            # Produkt-Update nur wenn keine Varianten fehlen
            if not missing_variants:
                product_payload = build_product_payload(product, is_update=True)
                product_payload["product"]["id"] = existing_product["id"]
                
                response = make_shopify_request(
                    f"{product_url}{existing_product['id']}.json",
                    method="PUT",
                    json_data=product_payload
                )
                
                return response is not None
            return False

        else:
            print(f"➕ Produkt '{product['title']}' existiert noch nicht. Füge hinzu...")
            
            if not any(v.get("available", False) for v in product["variants"]):
                print(f"⚠️ Keine verfügbaren Varianten, überspringe Produkt")
                return False

            product_payload = build_product_payload(product)
            response = make_shopify_request(api_url, method="POST", json_data=product_payload)

            if response:
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
            return False

    except Exception as e:
        print(f"❌ Unerwarteter Fehler: {e}")
        return False
    
def validate_product_data(product):
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

def process_brand_file(brand_file):
    try:
        with open(brand_file, 'r', encoding='utf-8') as json_file:
            products_data = json.load(json_file)

        brand_name = os.path.basename(brand_file).split('.')[0]
        print(f"🔍 Verarbeite {brand_name} mit {len(products_data)} Produkten...")

        existing_products = get_existing_products()

        success_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for product in products_data:
                futures.append(executor.submit(process_product, product, existing_products))
                time.sleep(0.5)  # Verzögerung zwischen Tasks
            
            for future in concurrent.futures.as_completed(futures):
                if future.result():
                    success_count += 1

        print(f"✅ {success_count}/{len(products_data)} Produkte aus {brand_name} erfolgreich verarbeitet!")
        return success_count

    except Exception as e:
        print(f"❌ Fehler beim Verarbeiten von {brand_file}: {e}")
        return 0

if __name__ == "__main__":
    start_time = time.time()
    total_processed = 0
    seen_skus = set()

    # SKUs sammeln
    for brand_file in [
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
    ]:
        try:
            with open(brand_file, 'r', encoding='utf-8') as f:
                for product in json.load(f):
                    for variant in product.get("variants", []):
                        if "sku" in variant:
                            seen_skus.add(variant["sku"])
        except Exception as e:
            print(f"❌ Fehler beim Lesen von {brand_file}: {e}")

    # Produkte verarbeiten
    for brand_file in [
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
    ]:
        total_processed += process_brand_file(brand_file)
        get_existing_products(force_refresh=True)

    # Veraltete Produkte deaktivieren
    existing_products = get_existing_products(force_refresh=True)
    disabled_count = 0

    for product in existing_products:
        for variant in product["variants"]:
            if (sku := variant.get("sku")) and sku not in seen_skus:
                if update_inventory_bulk([{
                    "inventory_item_id": variant["inventory_item_id"],
                    "available": False
                }]):
                    print(f"🚫 Bestand auf 0 für SKU {sku}")
                    disabled_count += 1

    total_time = time.time() - start_time
    print(f"✅ Bestand für {disabled_count} veraltete Produkte auf 0 gesetzt.")
    print(f"🏎️ Alle Dateien verarbeitet! Insgesamt {total_processed} Produkte in {total_time:.2f} Sekunden.")
