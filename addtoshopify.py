import requests
import os
import json
from datetime import datetime
import time
import math
import logging
from typing import List, Dict, Optional, Union

# Konfiguration
CONFIG = {
    "CACHE_TTL": 300,  # 5 Minuten Cache Gültigkeit
    "MAX_REQUESTS_PER_SECOND": 2,
    "INVENTORY_DEFAULT": 1000,
    "BASE_PRICE_INCREASE": 1.075,
    "PRICE_ROUNDING": 0.99,
    "BATCH_SIZE": 50  # Für Bulk-Operationen
}

# Shopify-Zugangsdaten
access_token = os.getenv("SHOPIFY_TOKEN")
shop_url = os.getenv("SHOPIFY_URL")
api_version = "2024-01"
LOCATION_ID = "108058247432"

# API-Endpunkte
api_url = f"https://{shop_url}/admin/api/{api_version}/products.json"
product_url = f"https://{shop_url}/admin/api/{api_version}/products/"
inventory_url = f"https://{shop_url}/admin/api/{api_version}/inventory_levels/set.json"
bulk_inventory_url = f"https://{shop_url}/admin/api/{api_version}/inventory_levels/set.json"

headers = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": access_token
}

# Logging konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cache für vorhandene Produkte
existing_products_cache = None
last_cache_update = 0

class RateLimiter:
    """Hilfsklasse für Rate Limiting"""
    def __init__(self, calls_per_second):
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0

    def wait(self):
        """Wartet die erforderliche Zeit zwischen den Aufrufen"""
        now = time.time()
        elapsed = now - self.last_call
        wait_time = max(0, self.min_interval - elapsed)
        if wait_time > 0:
            time.sleep(wait_time)
        self.last_call = time.time()

# Rate Limiter initialisieren
limiter = RateLimiter(CONFIG["MAX_REQUESTS_PER_SECOND"])

def calculate_adjusted_price(original_price: Union[str, float]) -> float:
    """
    Berechnet den angepassten Preis:
    1. Fügt 7.5% zum Originalpreis hinzu
    2. Rundet auf X.99 auf
    """
    try:
        if isinstance(original_price, str):
            original_price = float(original_price.replace("€", "").strip())
        
        increased_price = original_price * CONFIG["BASE_PRICE_INCREASE"]
        
        if increased_price % 1 < CONFIG["PRICE_ROUNDING"]:
            adjusted_price = math.floor(increased_price) + CONFIG["PRICE_ROUNDING"]
        else:
            adjusted_price = math.ceil(increased_price) + CONFIG["PRICE_ROUNDING"]
        
        return round(adjusted_price, 2)
    except (ValueError, TypeError) as e:
        logger.error(f"Preisberechnungsfehler für {original_price}: {e}")
        return original_price

def make_shopify_request(url: str, method: str = "GET", json_data: Optional[Dict] = None, max_retries: int = 3) -> Optional[requests.Response]:
    """
    Führt eine API-Anfrage mit Rate Limiting und Retry-Logik durch
    """
    retries = 0
    while retries < max_retries:
        limiter.wait()  # Rate Limiting beachten
        
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
                logger.error(f"Fehler bei API-Anfrage {method} {url}: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    logger.error(f"Fehlerdetails: {e.response.text}")
                return None
            wait_time = 2 ** retries  # Exponentielles Backoff
            logger.warning(f"Warte {wait_time}s vor Wiederholung {retries}/{max_retries}")
            time.sleep(wait_time)

def get_all_paginated(url: str) -> List[Dict]:
    """
    Holt alle paginierten Ergebnisse von einem API-Endpunkt
    """
    all_results = []
    while url:
        response = make_shopify_request(url)
        if not response:
            break
            
        data = response.json()
        if 'products' in data:
            all_results.extend(data['products'])
        elif 'inventory_levels' in data:
            all_results.extend(data['inventory_levels'])
        
        # Paginierung prüfen
        if 'Link' in response.headers:
            links = response.headers['Link'].split(',')
            url = None
            for link in links:
                if 'rel="next"' in link:
                    url = link[link.find('<') + 1:link.find('>')]
                    break
        else:
            url = None
            
    return all_results

def get_existing_products(force_refresh: bool = False) -> List[Dict]:
    """
    Holt alle vorhandenen Produkte mit Caching
    """
    global existing_products_cache, last_cache_update
    
    current_time = time.time()
    if force_refresh or not existing_products_cache or (current_time - last_cache_update) > CONFIG["CACHE_TTL"]:
        logger.info("Aktualisiere Produkt-Cache...")
        try:
            existing_products_cache = get_all_paginated(api_url + "?limit=250")
            last_cache_update = current_time
            logger.info(f"Produkt-Cache aktualisiert, {len(existing_products_cache)} Produkte geladen")
        except Exception as e:
            logger.error(f"Cache-Update fehlgeschlagen: {e}")
            if not existing_products_cache:
                raise
                
    return existing_products_cache

def bulk_update_inventory(updates: List[Dict]) -> bool:
    """
    Führt Bulk-Inventory-Updates durch
    """
    if not updates:
        return True
        
    payload = {
        "location_id": LOCATION_ID,
        "updates": updates
    }
    
    response = make_shopify_request(bulk_inventory_url, method="POST", json_data=payload)
    if response:
        logger.info(f"Erfolgreich {len(updates)} Inventory-Updates durchgeführt")
        return True
    else:
        logger.error(f"Fehler bei Bulk-Inventory-Update für {len(updates)} Items")
        return False

def build_product_payload(product_data: Dict, is_update: bool = False) -> Dict:
    """
    Erstellt den Payload für Produktcreate/update
    """
    payload = {
        "product": {
            "title": product_data["title"],
            "body_html": product_data.get("body_html", ""),
            "variants": [],
            "images": []
        }
    }

    # Optionen hinzufügen
    if len(product_data["variants"]) > 1 or any(v.get("variant_title") for v in product_data["variants"]):
        payload["product"]["options"] = [{"name": "Size"}]

    # Veröffentlichungsdatum
    published_at = product_data.get("published_at")
    if published_at:
        try:
            if isinstance(published_at, str):
                datetime.fromisoformat(published_at)
                payload["product"]["published_at"] = published_at
            else:
                logger.warning("published_at ist kein String, wird nicht übernommen")
        except ValueError as e:
            logger.warning(f"Ungültiges published_at Format: {e}, wird nicht übernommen")
    else:
        payload["product"]["published_at"] = datetime.now().isoformat()

    # Metadaten
    metadata_fields = ["vendor", "product_type", "tags", "handle", "created_at", "updated_at"]
    for field in metadata_fields:
        if field in product_data:
            if field.endswith("_at") and product_data[field]:
                try:
                    if isinstance(product_data[field], str):
                        dt = datetime.fromisoformat(product_data[field])
                        payload["product"][field] = dt.isoformat()
                    else:
                        payload["product"][field] = product_data[field]
                except ValueError:
                    logger.warning(f"Ungültiges Datumsformat für {field}, wird übersprungen")
            else:
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
            original_price = variant["price"]
            adjusted_price = calculate_adjusted_price(original_price)
            
            variant_payload = {
                "price": str(adjusted_price),
                "sku": variant["sku"],
                "inventory_quantity": CONFIG["INVENTORY_DEFAULT"] if variant["available"] else 0,
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
            
            logger.info(f"Preis angepasst: {original_price} → {adjusted_price}")
            
        except KeyError as e:
            logger.error(f"Wichtiges Variantenfeld fehlt: {e}, Variante wird übersprungen")

    return payload

def validate_product_data(product: Dict) -> bool:
    """
    Validiert die Produktdaten
    """
    required = ["title", "variants"]
    if not all(field in product for field in required):
        logger.error("Produkt fehlt erforderliche Felder")
        return False
    
    if not isinstance(product["variants"], list) or not product["variants"]:
        logger.error("Keine oder ungültige Varianten")
        return False
    
    for v in product["variants"]:
        if not all(k in v for k in ["sku", "price", "available"]):
            logger.error(f"Varianten fehlen erforderliche Felder: {v}")
            return False
        if not isinstance(v["available"], bool):
            logger.error(f"Ungültiger available-Wert: {v['available']}")
            return False
            
    return True

def process_product(product: Dict, existing_products: List[Dict]) -> bool:
    """
    Verarbeitet ein einzelnes Produkt
    """
    try:
        if not validate_product_data(product):
            return False

        product_skus = {v["sku"] for v in product["variants"] if "sku" in v}
        existing_product = None
        variant_map = {}  # SKU -> existing_variant

        # Existierende Produkte durchsuchen
        for p in existing_products:
            for v in p["variants"]:
                if v.get("sku") in product_skus:
                    existing_product = p
                    variant_map[v["sku"]] = v
            
            if existing_product:
                break

        if existing_product:
            logger.info(f"Produkt '{product['title']}' existiert (ID: {existing_product['id']})")
            
            # Inventory-Updates sammeln
            inventory_updates = []
            for variant in product["variants"]:
                existing_variant = variant_map.get(variant["sku"])
                if existing_variant:
                    inventory_updates.append({
                        "inventory_item_id": existing_variant["inventory_item_id"],
                        "available": CONFIG["INVENTORY_DEFAULT"] if variant["available"] else 0
                    })
                else:
                    logger.warning(f"Neue Variante {variant['sku']} wird benötigt")

            # Bulk-Inventory-Update
            if inventory_updates:
                success = bulk_update_inventory(inventory_updates)
                if not success:
                    return False

            # Produktdaten aktualisieren wenn nötig
            if len(variant_map) == len(product["variants"]):
                product_payload = build_product_payload(product, is_update=True)
                product_payload["product"]["id"] = existing_product["id"]
                
                response = make_shopify_request(
                    f"{product_url}{existing_product['id']}.json",
                    method="PUT",
                    json_data=product_payload
                )
                return response is not None
            else:
                logger.warning("Nicht alle Varianten konnten aktualisiert werden")
                return False
        else:
            logger.info(f"Füge neues Produkt '{product['title']}' hinzu")
            
            if not any(v.get("available", False) for v in product["variants"]):
                logger.warning("Keine verfügbaren Varianten, überspringe Produkt")
                return False

            product_payload = build_product_payload(product)
            response = make_shopify_request(api_url, method="POST", json_data=product_payload)

            if response:
                logger.info("Produkt erfolgreich hinzugefügt")
                
                # Veröffentlichen falls nicht gesetzt
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
                logger.error("Fehler beim Hinzufügen des Produkts")
                return False

    except Exception as e:
        logger.error(f"Unerwarteter Fehler: {e}")
        return False

def process_brand_file(brand_file: str) -> int:
    """
    Verarbeitet eine Markendatei
    """
    try:
        with open(brand_file, 'r', encoding='utf-8') as json_file:
            products_data = json.load(json_file)

        brand_name = os.path.splitext(os.path.basename(brand_file))[0]
        logger.info(f"Verarbeite {brand_name} mit {len(products_data)} Produkten")

        existing_products = get_existing_products()
        success_count = 0

        for product in products_data:
            if process_product(product, existing_products):
                success_count += 1
            time.sleep(0.5)  # Zwischen Produkten pausieren

        logger.info(f"{success_count}/{len(products_data)} Produkte aus {brand_name} erfolgreich")
        return success_count

    except Exception as e:
        logger.error(f"Fehler beim Verarbeiten von {brand_file}: {e}")
        return 0

def main():
    start_time = time.time()
    total_processed = 0
    seen_skus = set()

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
            logger.error(f"Fehler beim Lesen von {brand_file}: {e}")

    # Dann Produkte verarbeiten
    for brand_file in brand_files:
        if os.path.exists(brand_file):
            total_processed += process_brand_file(brand_file)
            get_existing_products(force_refresh=True)
        else:
            logger.warning(f"Datei nicht gefunden: {brand_file}")

    # Veraltete Produkte deaktivieren
    existing_products = get_existing_products(force_refresh=True)
    disabled_updates = []

    for product in existing_products:
        for variant in product["variants"]:
            sku = variant.get("sku")
            if sku and sku not in seen_skus:
                disabled_updates.append({
                    "inventory_item_id": variant["inventory_item_id"],
                    "available": 0
                })

    # Bulk-Deaktivierung
    if disabled_updates:
        for i in range(0, len(disabled_updates), CONFIG["BATCH_SIZE"]):
            batch = disabled_updates[i:i+CONFIG["BATCH_SIZE"]]
            if bulk_update_inventory(batch):
                logger.info(f"Deaktiviert {len(batch)} veraltete Produkte")

    total_time = time.time() - start_time
    logger.info(f"Fertig! Insgesamt {total_processed} Produkte in {total_time:.2f} Sekunden verarbeitet.")

if __name__ == "__main__":
    main()
