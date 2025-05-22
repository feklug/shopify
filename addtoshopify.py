import requests
import os
import json
from datetime import datetime
import concurrent.futures
import time
import math

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
        
        # 7.5% Aufschlag
        increased_price = original_price * 1.075
        
        # Auf X.99 aufrunden
        if increased_price % 1 < 0.99:
            adjusted_price = math.floor(increased_price) + 0.99
        else:
            adjusted_price = math.ceil(increased_price) + 0.99
        
        return round(adjusted_price, 2)
    except (ValueError, TypeError) as e:
        print(f"⚠️ Preisberechnungsfehler für {original_price}: {e}")
        return original_price  # Fallback zum Originalpreis

def make_shopify_request(url, method="GET", json_data=None, max_retries=3):
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
        except requests.exceptions.RequestException as e:
            retries += 1
            if retries == max_retries:
                print(f"❌ Fehler bei API-Anfrage: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    print(f"Fehlerdetails: {e.response.text}")
                return None
            time.sleep(2 ** retries)  # Exponentielles Backoff

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

def update_inventory(inventory_item_id, available):
    payload = {
        "location_id": LOCATION_ID,
        "inventory_item_id": inventory_item_id,
        "available": 1000 if available else 0
    }
    response = make_shopify_request(inventory_url, method="POST", json_data=payload)
    return response is not None

def build_product_payload(product_data, is_update=False):
    payload = {
        "product": {
            "title": product_data["title"],
            "body_html": product_data.get("body_html", ""),
            "options": [{"name": "Size"}],
            "variants": [],
            "images": []
        }
    }

    # Veröffentlichungsdatum
    published_at = product_data.get("published_at")
    if published_at:
        try:
            if isinstance(published_at, str):
                datetime.fromisoformat(published_at)
                payload["product"]["published_at"] = published_at
            else:
                print("⚠️ published_at ist kein String, wird nicht übernommen")
        except ValueError as e:
            print(f"⚠️ Ungültiges published_at Format: {e}, wird nicht übernommen")
    else:
        payload["product"]["published_at"] = datetime.now().isoformat()

    # Metadaten
    metadata_fields = {
        "vendor": None,
        "product_type": None,
        "tags": None,
        "handle": None,
        "created_at": None,
        "updated_at": None
    }

    for field, default in metadata_fields.items():
        if field in product_data:
            if field.endswith("_at") and product_data[field]:
                try:
                    if isinstance(product_data[field], str):
                        dt = datetime.fromisoformat(product_data[field])
                        payload["product"][field] = dt.isoformat()
                    else:
                        payload["product"][field] = product_data[field]
                except ValueError:
                    print(f"⚠️ Ungültiges Datumsformat für {field}, wird übersprungen")
            else:
                payload["product"][field] = product_data[field]

    # Bilder
    image_urls = set()
    for variant in product_data["variants"]:
        for img_url in variant.get("images", []):
            image_urls.add(img_url)
    payload["product"]["images"] = [{"src": img} for img in image_urls]

    # Varianten mit korrigierter optionaler Felder-Behandlung
    for variant in product_data["variants"]:
        try:
            original_price = variant["price"]
            adjusted_price = calculate_adjusted_price(original_price)
            
            variant_payload = {
                "option1": variant["variant_title"],
                "price": str(adjusted_price),
                "sku": variant["sku"],
                "inventory_quantity": 1000 if variant["available"] else 0,
                "inventory_management": "shopify",
                "inventory_policy": "deny"
            }

            # Korrigierte Behandlung optionaler Felder
            optional_fields = {
                "barcode": None,
                "weight": None,
                "weight_unit": None,
                "taxable": None,
                "compare_at_price": None
            }
            
            for field, default in optional_fields.items():
                if field in variant:
                    variant_payload[field] = variant[field]

            payload["product"]["variants"].append(variant_payload)
            
            print(f"💰 Preis angepasst: {original_price} → {adjusted_price}")
            
        except KeyError as e:
            print(f"⚠️ Wichtiges Variantenfeld fehlt: {e}, Variante wird übersprungen")

    return payload

def process_product(product, existing_products):
    try:
        if "title" not in product or "variants" not in product or not product["variants"]:
            print("❌ Produktdaten unvollständig, überspringe Produkt")
            return False

        available_variant = next((v for v in product["variants"] if v["available"]), None)
        if not available_variant:
            print(f"❌ Keine verfügbare Variante für Produkt '{product['title']}', überspringe Produkt")
            return False

        first_sku = product["variants"][0]["sku"]
        existing_product = None
        existing_variant = None

        for p in existing_products:
            for v in p["variants"]:
                if v["sku"] == first_sku:
                    existing_product = p
                    existing_variant = v
                    break
            if existing_product:
                break

        if existing_product:
            print(f"🔄 Produkt '{product['title']}' existiert bereits. Aktualisiere...")

            success = update_inventory(
                inventory_item_id=existing_variant["inventory_item_id"],
                available=product["variants"][0]["available"]
            )

            if success:
                print(f"✅ Bestand für Variante {existing_variant['sku']} aktualisiert")
            else:
                print(f"❌ Fehler beim Aktualisieren des Bestands für Variante {existing_variant['sku']}")

            product_payload = build_product_payload(product, is_update=True)
            product_payload["product"]["id"] = existing_product["id"]

            response = make_shopify_request(
                f"{product_url}{existing_product['id']}.json",
                method="PUT",
                json_data=product_payload
            )

            if response:
                print(f"✅ Produkt inkl. Preisanpassung aktualisiert")
                return True
            else:
                print(f"❌ Fehler beim Aktualisieren")
                return False

        else:
            print(f"➕ Produkt '{product['title']}' existiert noch nicht. Füge hinzu...")

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

def process_brand_file(brand_file):
    try:
        with open(brand_file, 'r', encoding='utf-8') as json_file:
            products_data = json.load(json_file)

        brand_name = brand_file.split('/')[1].split('.')[0]
        print(f"🔍 Verarbeite {brand_name} mit {len(products_data)} Produkten...")

        existing_products = get_existing_products()

        success_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for product in products_data:
                futures.append(executor.submit(process_product, product, existing_products))

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
    'output/99based.json',
    'output/rarehumansclothing.json',
]

if __name__ == "__main__":
    start_time = time.time()
    total_processed = 0
    seen_skus = set()

    # Zuerst alle SKUs sammeln
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

    # Dann Produkte verarbeiten
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
