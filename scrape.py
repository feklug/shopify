import requests
import json
import os

# Erstelle "output"-Ordner, wenn nicht vorhanden
os.makedirs("output", exist_ok=True)

# Dictionary mit Marken und deren Shopify-Produkt-URLs
base_urls = {
"Loveloop": "https://loveloopclo.com/collections/all/products.json?page=",
"Valeuratelier": "https://valeuratelier.de/collections/all/products.json?page=",
"Vacayo Clothing": "https://vacayo.de/collections/all/products.json?page=",
"Nyhro": "https://nyhro.com/collections/all/products.json?page=",
"after errors": "https://aftererrors.com/collections/all/products.json?page=",
"Elstar": "https://elstar-shop.com/collections/all/products.json?page=",
"xdaysleft": "https://xdaysleft-wear.com/collections/all/products.json?page=",
"Omage": "https://omage.xyz/collections/all/products.json?page=",
"Creamstores": "https://creamstores.com/collections/all/products.json?page=",
"Dustyaffection": "https://dustyaffection.com/collections/all/products.json?page=",
"GTEDClo": "https://gtedclo.com/collections/all/products.json?page=",
"DerangeStudios": "https://derangestudios.com/collections/all/products.json?page=",
"EclipseStudios": "https://eclipse-studios.de/collections/all/products.json?page=",
"StOnee": "https://st-onee.com/collections/all/products.json?page=",
"Orelien": "https://orelien-official.com/collections/all/products.json?page=",
"Reerect": "https://reerect.de/collections/all/products.json?page=",
"ProjectAvise": "https://www.projectavise.de/collections/all/products.json?page=",
"Oill": "https://oill.xyz/collections/all/products.json?page=",
"ReputeVision": "https://www.reputevision.com/collections/all/products.json?page=",
"FrankFillerStudios": "https://frankfillerstudios.de/collections/all/products.json?page=",
"Artcademy": "https://artcademy.store/collections/all/products.json?page=",
"Purpill": "https://www.purpill.eu/collections/all/products.json?page=",
"8rb4": "https://8rb4.com/collections/all/products.json?page=",
"MemoriesDontDie": "https://www.memoriesdontdie.de/collections/all/products.json?page=",
"AnotherState": "https://anotherstate.net/collections/all/products.json?page=",
"ExitLife": "https://exitlife.de/collections/all/products.json?page=",
"YeuTheWorld": "https://yeutheworld.com/collections/all/products.json?page=",
"NoAnger": "https://noanger.de/collections/all/products.json?page=",
"4Hearts": "https://www.4hearts.de/collections/all/products.json?page=",
"4thD": "https://4thd.de/collections/all/products.json?page="

}


# Dictionary zur Speicherung der Ergebnisse
brand_results = {}

for brand, base_url in base_urls.items():
    print(f"🔍 Scrape {brand}...")
    all_products = []
    page = 1
    product_count = 0

    while True:
        url = base_url + str(page)
        try:
            response = requests.get(url, timeout=10)
        except Exception as e:
            print(f"❌ Fehler bei {brand}, Seite {page}: {e}")
            break

        if response.status_code != 200:
            print(f"⚠️ Fehler {response.status_code} bei {brand}, Seite {page}")
            break

        try:
            data = response.json()
        except json.JSONDecodeError:
            print(f"⚠️ Keine gültige JSON-Antwort von {brand}, Seite {page}")
            break

        products = data.get("products", [])
        if not products:
            break

        for product in products:
            product_variants = []
            for variant in product.get("variants", []):
                product_variants.append({
                    "variant_title": variant.get("title", ""),
                    "price": variant.get("price", ""),
                    "sku": variant.get("sku", ""),
                    "available": variant.get("available", False),
                    "option1": variant.get("option1"),
                    "option2": variant.get("option2"),
                    "option3": variant.get("option3"),
                    "grams": variant.get("grams"),
                    "requires_shipping": variant.get("requires_shipping"),
                    "taxable": variant.get("taxable"),
                    "created_at": variant.get("created_at"),
                    "updated_at": variant.get("updated_at"),
                    "images": [img["src"] for img in product.get("images", [])]
                })

            all_products.append({
                "title": product.get("title", ""),
                "body_html": product.get("body_html", ""),
                "vendor": product.get("vendor", brand),
                "product_type": product.get("product_type", ""),
                "tags": product.get("tags", []),
                "handle": product.get("handle", ""),
                "created_at": product.get("created_at", ""),
                "updated_at": product.get("updated_at", ""),
                "published_at": product.get("published_at", ""),
                "images": [img["src"] for img in product.get("images", [])],
                "variants": product_variants,
                "product_count": len(product_variants)
            })

            product_count += 1

        page += 1

    # Speichern
    with open(f"output/{brand}.json", "w", encoding="utf-8") as json_file:
        json.dump(all_products, json_file, ensure_ascii=False, indent=4)

    # Ergebnis merken
    brand_results[brand] = product_count
    print(f"✅ {product_count} Produkte gespeichert in output/{brand}.json\n")

# 🔚 Zusammenfassung
print("📊 Zusammenfassung:")
for brand, count in brand_results.items():
    print(f"• {brand}: {count} Produkte")
