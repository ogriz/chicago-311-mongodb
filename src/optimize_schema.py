"""
Migracija 6 kolekcija (normalizovana shema) -> 1 kolekcija (embedded shema).
Koristi cache pristup: ucita sve lookup podatke u memoriju pa gradi dokumente.
"""

from tqdm import tqdm


OPTIMIZED_COLLECTION = "requests_optimized"


def migrate_to_embedded(database, batch_size=5000):
    print("Pocetak migracije u embedded shemu...")
    database[OPTIMIZED_COLLECTION].drop()

    print("  Ucitavanje locations u memoriju...")
    loc_cache = {}
    for loc in tqdm(database["locations"].find(), desc="  locations",
                    total=database["locations"].estimated_document_count()):
        loc_cache[loc["_id"]] = {
            "street_address": loc.get("street_address"),
            "zip_code": loc.get("zip_code"),
            "ward": loc.get("ward"),
            "police_district": loc.get("police_district"),
            "community_area": loc.get("community_area"),
            "latitude": loc.get("latitude"),
            "longitude": loc.get("longitude"),
        }

    detail_collections = {
        "vehicle_details": "vehicle",
        "sanitation_details": "sanitation",
        "environment_details": "environment",
        "building_details": "building",
    }

    detail_cache = {}
    for coll_name, detail_type in detail_collections.items():
        print(f"  Ucitavanje {coll_name} u memoriju...")
        for doc in tqdm(database[coll_name].find(), desc=f"  {coll_name}",
                        total=database[coll_name].estimated_document_count()):
            rid = doc["request_id"]
            detail = {k: v for k, v in doc.items() if k not in ("_id", "request_id")}
            detail_cache[rid] = detail

    print("  Gradnja embedded dokumenata...")
    total = database["requests"].estimated_document_count()
    batch = []
    count = 0

    for req in tqdm(database["requests"].find(), desc="  requests", total=total):
        loc_data = loc_cache.get(req.get("location_id"), {})

        embedded = {
            "service_request_number": req.get("service_request_number"),
            "request_type": req.get("request_type"),
            "creation_date": req.get("creation_date"),
            "completion_date": req.get("completion_date"),
            "status": req.get("status"),
            "current_activity": req.get("current_activity"),
            "most_recent_action": req.get("most_recent_action"),
            "location": loc_data,
            "details": detail_cache.get(req["_id"], {}),
        }

        batch.append(embedded)
        count += 1

        if len(batch) >= batch_size:
            database[OPTIMIZED_COLLECTION].insert_many(batch)
            batch.clear()

    if batch:
        database[OPTIMIZED_COLLECTION].insert_many(batch)

    final_count = database[OPTIMIZED_COLLECTION].count_documents({})
    print(f"\nMigracija zavrsena: {final_count:,} dokumenata u '{OPTIMIZED_COLLECTION}'")
    return final_count


def create_optimized_indexes(database):
    coll = database[OPTIMIZED_COLLECTION]
    print("Kreiranje indeksa za optimizovanu shemu...")

    coll.create_index([("request_type", 1), ("location.ward", 1)])
    coll.create_index([("location.community_area", 1), ("request_type", 1)])
    coll.create_index([("creation_date", 1), ("request_type", 1)])
    coll.create_index("status")
    coll.create_index("location.police_district")
    coll.create_index("location.street_address")
    coll.create_index("details.sub_type")
    coll.create_index("details.days_parked")
    coll.create_index("details.is_dangerous")
    coll.create_index("location.zip_code")

    print("Indeksi za optimizovanu shemu kreirani.")
