import requests
import os
import json
from datetime import datetime
import concurrent.futures
import time

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
            # Hole die Produkte der aktuellen Seite
            response = make_shopify_request(url)
            if response:
                products = response.json().get("products", [])
                all_products.extend(products)

                # Überprüfe den Link-Header auf den nächsten Seiten-Link
                if 'Link' in response.headers:
                    links = response.headers['Link']
                    # Extrahiere den nächsten Seiten-Link
                    next_page_url = None
                    for link in links.split(','):
                        if 'rel="next"' in link:
                            next_page_url = link[link.find('<') + 1:link.find('>')]
                            break
                    url = next_page_url  # Setze die URL auf den nächsten Seiten-Link
                else:
                    break
            else:
                break

        existing_products_cache = all_products
        last_cache_update = current_time
    
    return existing_products_cache



def update_inventory(inventory_item_id, available):
    """Aktualisiert den Bestand basierend auf 'available'."""
    payload = {
        "location_id": LOCATION_ID,
        "inventory_item_id": inventory_item_id,
        "available": 1000 if available else 0
    }
    response = make_shopify_request(inventory_url, method="POST", json_data=payload)
    return response is not None

def build_product_payload(product_data, is_update=False):
    """Baut das Produkt-Payload mit published_at und anderen Feldern"""
    payload = {
        "product": {
            "title": product_data["title"],
            "body_html": product_data.get("body_html", ""),
            "options": [{"name": "Size"}],
            "variants": [],
            "images": []
        }
    }

    # Veröffentlichungsdatum verarbeiten
    published_at = product_data.get("published_at")
    if published_at:
        try:
            if isinstance(published_at, str):
                # Validierung des Datums
                datetime.fromisoformat(published_at)
                payload["product"]["published_at"] = published_at
            else:
                print("⚠️ published_at ist kein String, wird nicht übernommen")
        except ValueError as e:
            print(f"⚠️ Ungültiges published_at Format: {e}, wird nicht übernommen")
    else:
        payload["product"]["published_at"] = datetime.now().isoformat()
        print("ℹ️ published_at nicht angegeben, setze auf aktuelles Datum")

    # Metadaten verarbeiten
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
            # Besondere Behandlung für Datumsfelder
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
    
    # Bilder sammeln (einmalig pro Bild-URL)
    image_urls = set()
    for variant in product_data["variants"]:
        for img_url in variant.get("images", []):
            image_urls.add(img_url)
    payload["product"]["images"] = [{"src": img} for img in image_urls]

    # Varianten verarbeiten
    for variant in product_data["variants"]:
        try:
            variant_payload = {
                "option1": variant["variant_title"],
                "price": variant["price"],
                "sku": variant["sku"],
                "inventory_quantity": 1000 if variant["available"] else 0,
                "inventory_management": "shopify",
                "inventory_policy": "deny"
            }
            
            # Optionale Variantenfelder
            optional_fields = {
                "barcode": None,
                "weight": None,
                "weight_unit": None,
                "taxable": None
            }
            
            for field, default in optional_fields.items():
                if field in variant:
                    variant_payload[field] = variant[field]
            
            payload["product"]["variants"].append(variant_payload)
        except KeyError as e:
            print(f"⚠️ Wichtiges Variantenfeld fehlt: {e}, Variante wird übersprungen")
    
    return payload

def process_product(product, existing_products):
    """Verarbeitet ein einzelnes Produkt"""
    try:
        # Validierung der Produktdaten
        if "title" not in product or "variants" not in product or not product["variants"]:
            print("❌ Produktdaten unvollständig, überspringe Produkt")
            return False

        # Überprüfe, ob mindestens eine Variante verfügbar ist
        available_variant = next((v for v in product["variants"] if v["available"]), None)
        if not available_variant:
            print(f"❌ Keine verfügbare Variante für Produkt '{product['title']}', überspringe Produkt")
            return False

        # Suche nach vorhandenem Produkt basierend auf SKU der ersten Variante
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
            
            # Bestand aktualisieren
            success = update_inventory(
                inventory_item_id=existing_variant["inventory_item_id"],
                available=product["variants"][0]["available"]
            )
            
            if success:
                print(f"✅ Bestand für Variante {existing_variant['sku']} aktualisiert")
            else:
                print(f"❌ Fehler beim Aktualisieren des Bestands für Variante {existing_variant['sku']}")

            # Produktdetails aktualisieren
            product_payload = build_product_payload(product, is_update=True)
            product_payload["product"]["id"] = existing_product["id"]
            
            response = make_shopify_request(
                f"{product_url}{existing_product['id']}.json",
                method="PUT",
                json_data=product_payload
            )
            
            if response:
                print(f"✅ Produktdetails für '{product['title']}' erfolgreich aktualisiert")
                return True
            else:
                print(f"❌ Fehler beim Aktualisieren der Produktdetails")
                return False

        else:
            print(f"➕ Produkt '{product['title']}' existiert noch nicht. Füge hinzu...")
            
            product_payload = build_product_payload(product)
            response = make_shopify_request(api_url, method="POST", json_data=product_payload)
            
            if response:
                print(f"✅ Produkt '{product['title']}' erfolgreich hinzugefügt.")
                
                # Setze published_at wenn nicht schon gesetzt
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
                print(f"❌ Fehler beim Hinzufügen von '{product['title']}'")
                return False

    except Exception as e:
        print(f"❌ Unerwarteter Fehler bei der Verarbeitung von Produkt {product.get('title', 'Unbekannt')}: {e}")
        return False

def process_brand_file(brand_file):
    """Verarbeitet eine einzelne Markendatei"""
    try:
        with open(brand_file, 'r', encoding='utf-8') as json_file:
            products_data = json.load(json_file)

        brand_name = brand_file.split('/')[1].split('.')[0]
        print(f"🔍 Verarbeite {brand_name} mit {len(products_data)} Produkten...")

        # Hole vorhandene Produkte einmal pro Datei
        existing_products = get_existing_products()

        # Verarbeite Produkte mit ThreadPool
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
'output/pegador.json',
'output/sourire-worldwide.json',
'output/liju-gallery.json',
'output/sacralite.json',
'output/unvainstudios.json',
'output/hunidesign.json',
'output/deputydepartment.json',
'output/99based.json',
'output/rarehumansclothing.json',

]

# Hauptverarbeitung
if __name__ == "__main__":
    start_time = time.time()
    total_processed = 0
    
    # Verarbeite Markendateien nacheinander (aber Produkte parallel)
    for brand_file in brand_files:
        total_processed += process_brand_file(brand_file)
        # Cache nach jeder Datei aktualisieren
        get_existing_products(force_refresh=True)

    total_time = time.time() - start_time
    print(f"🏁 Alle Dateien verarbeitet! Insgesamt {total_processed} Produkte in {total_time:.2f} Sekunden.")
