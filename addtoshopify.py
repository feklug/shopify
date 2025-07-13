import requests
import os
import json
from datetime import datetime
import concurrent.futures
import time
import math

api_version = "2024-01"
LOCATION_ID = "108058247432"
access_token = os.getenv("SHOPIFY_TOKEN")
shop_url = os.getenv("SHOPIFY_URL")

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

# Globaler SKU-Cache zur Duplikaterkennung
global_sku_cache = set()

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
            time.sleep(0.5)  # Rate Limiting: 2 Anfragen/Sekunde
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
    global existing_products_cache, last_cache_update, global_sku_cache

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

                # SKUs zum globalen Cache hinzufügen
                for product in products:
                    for variant in product.get('variants', []):
                        if 'sku' in variant:
                            global_sku_cache.add(variant['sku'])

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
            "variants": [],
            "images": []
        }
    }

    if len(product_data["variants"]) > 1 or any(v.get("variant_title") for v in product_data["variants"]):
        payload["product"]["options"] = [{"name": "Size"}]

    # Veröffentlichungszeitpunkt
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

    # Weitere Metadatenfelder
    metadata_fields = ["vendor", "product_type", "tags", "handle", "created_at", "updated_at"]
    for field in metadata_fields:
        if field in product_data and product_data[field]:
            if field.endswith("_at"):
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

    # Bilder sammeln
    seen_images = set()
    image_urls = []
    for variant in product_data["variants"]:
        for img_url in variant.get("images", []):
            if img_url not in seen_images:
                seen_images.add(img_url)
                image_urls.append(img_url)

    payload["product"]["images"] = [{"src": img} for img in image_urls]

    # Varianten verarbeiten
    for variant in product_data["variants"]:
        try:
            price = variant["price"]

            variant_payload = {
                "price": str(price),
                "sku": variant["sku"],
                "inventory_quantity": 1000 if variant["available"] else 0,
                "inventory_management": "shopify",
                "inventory_policy": "deny"
            }

            if "variant_title" in variant:
                variant_payload["option1"] = variant["variant_title"]

            # Optionale Felder
            for field in ["barcode", "weight", "weight_unit", "taxable", "compare_at_price"]:
                if field in variant:
                    variant_payload[field] = variant[field]

            payload["product"]["variants"].append(variant_payload)

            print(f"💰 Preis übernommen: {price}")

        except KeyError as e:
            print(f"⚠️ Wichtiges Variantenfeld fehlt: {e}, Variante wird übersprungen")

    return payload


def process_product(product, existing_products):
    try:
        # Frühzeitige Prüfung auf Fast Bundle
        if product.get("vendor") == "Fast Bundle":
            print(f"⏩ Fast Bundle Produkt '{product['title']}' wird übersprungen")
            return False
            
        if not validate_product_data(product):
            print("❌ Ungültige Produktdaten")
            return False

        # Prüfe auf Duplikate im globalen Cache
        product_skus = {v["sku"] for v in product["variants"] if "sku" in v}
        if any(sku in global_sku_cache for sku in product_skus):
            print(f"⏩ Produkt '{product['title']}' mit SKUs {product_skus} existiert bereits, überspringe...")
            return False

        existing_product = None
        variant_map = {}

        for p in existing_products:
            for v in p["variants"]:
                if v.get("sku") in product_skus:
                    existing_product = p
                    variant_map[v["sku"]] = v
            
            if existing_product:
                for v in existing_product["variants"]:
                    if v.get("sku") in product_skus and v["sku"] not in variant_map:
                        variant_map[v["sku"]] = v
                break

        if existing_product:
            print(f"🔄 Produkt '{product['title']}' existiert (ID: {existing_product['id']})")
            
            success = True
            for variant in product["variants"]:
                existing_variant = variant_map.get(variant["sku"])
                
                if existing_variant:
                    update_success = update_inventory(
                        existing_variant["inventory_item_id"],
                        variant["available"]
                    )
                    if not update_success:
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
                
                if response:
                    # Nach erfolgreichem Update SKUs zum Cache hinzufügen
                    for sku in product_skus:
                        global_sku_cache.add(sku)
                    return True
                else:
                    print("❌ Produktupdate fehlgeschlagen")
                    return False
            else:
                print("⚠️ Nicht alle Varianten konnten aktualisiert werden")
                return False

        else:
            print(f"➕ Produkt '{product['title']}' existiert noch nicht. Füge hinzu...")

            product_payload = build_product_payload(product)
            response = make_shopify_request(api_url, method="POST", json_data=product_payload)

            if response:
                print(f"✅ Produkt mit angepasstem Preis hinzugefügt")
                
                # Nach erfolgreichem Hinzufügen SKUs zum Cache hinzufügen
                for sku in product_skus:
                    global_sku_cache.add(sku)
                
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
    
def validate_product_data(product):
    required = ["title", "variants"]
    if not all(field in product for field in required):
        return False
    
    # Ausschluss von Fast Bundle Produkten
    if product.get("vendor") == "Fast Bundle":
        print("⏩ Fast Bundle Produkt wird übersprungen")
        return False
    
    if not isinstance(product["variants"], list) or not product["variants"]:
        return False
    
    # Prüfe, ob mindestens ein Bild vorhanden ist
    has_images = False
    for variant in product["variants"]:
        if "images" in variant and variant["images"]:
            has_images = True
            break
    
    if not has_images:
        print("⚠️ Produkt ohne Bilder wird übersprungen")
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

        brand_name = brand_file.split('/')[1].split('.')[0]
        print(f"🔍 Verarbeite {brand_name} mit {len(products_data)} Produkten...")

        existing_products = get_existing_products()

        success_count = 0
        batch_size = 10  # Verarbeite in Batches für bessere Performance
        for i in range(0, len(products_data), batch_size):
            batch = products_data[i:i + batch_size]
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:  # Reduzierte Worker
                futures = [executor.submit(process_product, product, existing_products) for product in batch]
                for future in concurrent.futures.as_completed(futures):
                    if future.result():
                        success_count += 1
            time.sleep(1)  # Kurze Pause zwischen Batches

        print(f"✅ {success_count}/{len(products_data)} Produkte aus {brand_name} erfolgreich verarbeitet!")
        return success_count

    except Exception as e:
        print(f"❌ Fehler beim Verarbeiten von {brand_file}: {e}")
        return 0

brand_files = [
    
'output/Loveloop.json',
'output/Valeuratelier.json',
'output/Vacayo Clothing.json',
'output/Nyhro.json',
'output/after errors.json',
'output/Elstar.json',
'output/xdaysleft.json',
'output/Omage.json',
'output/Creamstores.json',
'output/Dustyaffection.json',
'output/GTEDClo.json',
'output/DerangeStudios.json',
'output/EclipseStudios.json',
'output/StOnee.json',
'output/Orelien.json',
'output/Reerect.json',
'output/ProjectAvise.json',
'output/Oill.json',
'output/ReputeVision.json',
'output/FrankFillerStudios.json',
'output/Artcademy.json',
'output/Purpill.json',
'output/8rb4.json',
'output/MemoriesDontDie.json',
'output/AnotherState.json',
'output/ExitLife.json',
'output/YeuTheWorld.json',
'output/NoAnger.json',
'output/4Hearts.json',
'output/4thD.json'



]

if __name__ == "__main__":
    start_time = time.time()
    total_processed = 0
    seen_skus = set()

    # Initialisiere globalen SKU-Cache
    get_existing_products(force_refresh=True)

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
