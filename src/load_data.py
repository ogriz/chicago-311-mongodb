import os
import pandas as pd
from datetime import datetime
from bson import ObjectId
from tqdm import tqdm

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

CSV_FILES = [
    {
        "file": "311-service-requests-abandoned-vehicles.csv",
        "detail_collection": "vehicle_details",
        "sub_type": None,
        "detail_mapping": {
            "License Plate": "license_plate",
            "Vehicle Make/Model": "vehicle_make_model",
            "Vehicle Color": "vehicle_color",
            "How Many Days Has the Vehicle Been Reported as Parked?": "days_parked",
        },
        "numeric_details": ["days_parked"],
    },
    {
        "file": "311-service-requests-alley-lights-out.csv",
        "detail_collection": "environment_details",
        "sub_type": "alley_light",
        "detail_mapping": {},
        "numeric_details": [],
    },
    {
        "file": "311-service-requests-garbage-carts.csv",
        "detail_collection": "sanitation_details",
        "sub_type": "garbage_cart",
        "detail_mapping": {
            "Number of Black Carts Delivered": "number_of_black_carts_delivered",
        },
        "numeric_details": ["number_of_black_carts_delivered"],
    },
    {
        "file": "311-service-requests-graffiti-removal.csv",
        "detail_collection": "environment_details",
        "sub_type": "graffiti",
        "detail_mapping": {
            "What Type of Surface is the Graffiti on?": "surface_type",
            "Where is the Graffiti located?": "graffiti_location",
        },
        "numeric_details": [],
    },
    {
        "file": "311-service-requests-pot-holes-reported.csv",
        "detail_collection": "environment_details",
        "sub_type": "pothole",
        "detail_mapping": {
            "NUMBER OF POTHOLES FILLED ON BLOCK": "number_of_potholes_filled_on_block",
        },
        "numeric_details": ["number_of_potholes_filled_on_block"],
    },
    {
        "file": "311-service-requests-rodent-baiting.csv",
        "detail_collection": "sanitation_details",
        "sub_type": "rodent_baiting",
        "detail_mapping": {
            "Number of Premises Baited": "number_of_premises_baited",
            "Number of Premises with Garbage": "number_of_premises_with_garbage",
            "Number of Premises with Rats": "number_of_premises_with_rats",
        },
        "numeric_details": [
            "number_of_premises_baited",
            "number_of_premises_with_garbage",
            "number_of_premises_with_rats",
        ],
    },
    {
        "file": "311-service-requests-sanitation-code-complaints.csv",
        "detail_collection": "sanitation_details",
        "sub_type": "sanitation_code",
        "detail_mapping": {
            "What is the Nature of this Code Violation?": "nature_of_code_violation",
        },
        "numeric_details": [],
    },
    {
        "file": "311-service-requests-street-lights-all-out.csv",
        "detail_collection": "environment_details",
        "sub_type": "street_light_all",
        "detail_mapping": {},
        "numeric_details": [],
    },
    {
        "file": "311-service-requests-street-lights-one-out.csv",
        "detail_collection": "environment_details",
        "sub_type": "street_light_one",
        "detail_mapping": {},
        "numeric_details": [],
    },
    {
        "file": "311-service-requests-tree-debris.csv",
        "detail_collection": "environment_details",
        "sub_type": "tree_debris",
        "detail_mapping": {
            "If Yes, where is the debris located?": "location_of_trees",
        },
        "numeric_details": [],
    },
    {
        "file": "311-service-requests-tree-trims.csv",
        "detail_collection": "environment_details",
        "sub_type": "tree_trim",
        "detail_mapping": {
            "Location of Trees": "location_of_trees",
        },
        "numeric_details": [],
    },
    {
        "file": "311-service-requests-vacant-and-abandoned-buildings-reported.csv",
        "detail_collection": "building_details",
        "sub_type": None,
        "detail_mapping": {
            "LOCATION OF BUILDING ON THE LOT (IF GARAGE, CHANGE TYPE CODE TO BGD).": "location_on_lot",
            "IS THE BUILDING DANGEROUS OR HAZARDOUS?": "is_dangerous",
            "IS BUILDING OPEN OR BOARDED?": "is_open_or_boarded",
            "IF THE BUILDING IS OPEN, WHERE IS THE ENTRY POINT?": "entry_point",
            "IS THE BUILDING CURRENTLY VACANT OR OCCUPIED?": "is_vacant_or_occupied",
            "IS THE BUILDING VACANT DUE TO FIRE?": "is_vacant_due_to_fire",
            "ANY PEOPLE USING PROPERTY? (HOMELESS, CHILDEN, GANGS)": "people_using_property",
        },
        "numeric_details": [],
        "is_vacant_building": True,
    },
]


def _find_column(columns, *candidates):
    col_map = {c.strip().upper(): c for c in columns}
    for candidate in candidates:
        if candidate.upper() in col_map:
            return col_map[candidate.upper()]
    return None


def _parse_date(val):
    if pd.isna(val) or val == "":
        return None
    try:
        if isinstance(val, str):
            return datetime.strptime(val[:19], "%Y-%m-%dT%H:%M:%S")
        return val
    except (ValueError, TypeError):
        return None


def _safe_float(val):
    if pd.isna(val) or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val):
    if pd.isna(val) or val == "":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _safe_str(val):
    if pd.isna(val) or val == "":
        return None
    return str(val).strip()


def _to_bool(val):
    if pd.isna(val) or val == "":
        return None
    s = str(val).strip().upper()
    if s in ("TRUE", "YES", "Y", "1"):
        return True
    if s in ("FALSE", "NO", "N", "0"):
        return False
    return s


def load_all_data(database, data_dir=None, batch_size=5000):
    if data_dir is None:
        data_dir = DATA_DIR

    location_cache = {}
    stats = {}

    for csv_config in CSV_FILES:
        filepath = os.path.join(data_dir, csv_config["file"])
        if not os.path.exists(filepath):
            print(f"UPOZORENJE: Fajl ne postoji: {filepath}")
            continue

        print(f"\n{'='*60}")
        print(f"Ucitavanje: {csv_config['file']}")
        print(f"{'='*60}")

        _load_single_csv(database, filepath, csv_config, location_cache, batch_size, stats)

    print(f"\n{'='*60}")
    print("STATISTIKA UCITAVANJA:")
    print(f"{'='*60}")
    for coll_name in ["requests", "locations", "vehicle_details",
                       "sanitation_details", "environment_details", "building_details"]:
        count = database[coll_name].count_documents({})
        print(f"  {coll_name}: {count:,} dokumenata")
        stats[coll_name] = count

    return stats


def _load_single_csv(database, filepath, csv_config, location_cache, batch_size, stats):
    is_vacant = csv_config.get("is_vacant_building", False)
    detail_collection_name = csv_config["detail_collection"]
    sub_type = csv_config["sub_type"]
    detail_mapping = csv_config["detail_mapping"]
    numeric_details = csv_config["numeric_details"]

    df = pd.read_csv(filepath, dtype=str, low_memory=False)
    columns = list(df.columns)
    total_rows = len(df)
    print(f"  Redova u CSV: {total_rows:,}")

    if is_vacant:
        col_creation = _find_column(columns, "DATE SERVICE REQUEST WAS RECEIVED")
        col_completion = None
        col_status = None
        col_srn = _find_column(columns, "SERVICE REQUEST NUMBER")
        col_type = _find_column(columns, "SERVICE REQUEST TYPE")
        col_activity = None
        col_action = None
        col_addr_num = _find_column(columns, "ADDRESS STREET NUMBER")
        col_addr_dir = _find_column(columns, "ADDRESS STREET DIRECTION")
        col_addr_name = _find_column(columns, "ADDRESS STREET NAME")
        col_addr_suf = _find_column(columns, "ADDRESS STREET SUFFIX")
        col_address = None
    else:
        col_creation = _find_column(columns, "Creation Date", "CREATION DATE")
        col_completion = _find_column(columns, "Completion Date", "COMPLETION DATE")
        col_status = _find_column(columns, "Status", "STATUS")
        col_srn = _find_column(columns, "Service Request Number", "SERVICE REQUEST NUMBER")
        col_type = _find_column(columns, "Type of Service Request", "TYPE OF SERVICE REQUEST")
        col_activity = _find_column(columns, "Current Activity", "CURRENT ACTIVITY")
        col_action = _find_column(columns, "Most Recent Action", "MOST RECENT ACTION")
        col_address = _find_column(columns, "Street Address", "STREET ADDRESS")
        col_addr_num = col_addr_dir = col_addr_name = col_addr_suf = None

    col_zip = _find_column(columns, "ZIP Code", "ZIP CODE", "ZIP")
    col_ward = _find_column(columns, "Ward", "WARD")
    col_district = _find_column(columns, "Police District", "POLICE DISTRICT")
    col_community = _find_column(columns, "Community Area", "COMMUNITY AREA")
    col_lat = _find_column(columns, "Latitude", "LATITUDE")
    col_lon = _find_column(columns, "Longitude", "LONGITUDE")

    request_docs = []
    detail_docs = []
    location_docs_to_insert = []

    for idx in tqdm(range(total_rows), desc=f"  {csv_config['file'][:30]}"):
        row = df.iloc[idx]

        if is_vacant:
            parts = []
            for c in [col_addr_num, col_addr_dir, col_addr_name, col_addr_suf]:
                if c and _safe_str(row.get(c)):
                    parts.append(_safe_str(row[c]))
            street_address = " ".join(parts) if parts else None
        else:
            street_address = _safe_str(row.get(col_address)) if col_address else None

        zip_code = _safe_str(row.get(col_zip)) if col_zip else None
        ward = _safe_int(row.get(col_ward)) if col_ward else None
        police_district = _safe_int(row.get(col_district)) if col_district else None
        community_area = _safe_int(row.get(col_community)) if col_community else None
        latitude = _safe_float(row.get(col_lat)) if col_lat else None
        longitude = _safe_float(row.get(col_lon)) if col_lon else None

        loc_key = (street_address or "", zip_code or "")
        if loc_key in location_cache:
            location_id = location_cache[loc_key]
        else:
            location_id = ObjectId()
            location_cache[loc_key] = location_id
            location_docs_to_insert.append({
                "_id": location_id,
                "street_address": street_address,
                "zip_code": zip_code,
                "ward": ward,
                "police_district": police_district,
                "community_area": community_area,
                "latitude": latitude,
                "longitude": longitude,
            })

        request_id = ObjectId()
        request_doc = {
            "_id": request_id,
            "service_request_number": _safe_str(row.get(col_srn)) if col_srn else None,
            "request_type": _safe_str(row.get(col_type)) if col_type else None,
            "creation_date": _parse_date(row.get(col_creation)) if col_creation else None,
            "completion_date": _parse_date(row.get(col_completion)) if col_completion else None,
            "status": _safe_str(row.get(col_status)) if col_status else None,
            "current_activity": _safe_str(row.get(col_activity)) if col_activity else None,
            "most_recent_action": _safe_str(row.get(col_action)) if col_action else None,
            "location_id": location_id,
        }
        request_docs.append(request_doc)

        detail_doc = {"_id": ObjectId(), "request_id": request_id}
        if sub_type:
            detail_doc["sub_type"] = sub_type

        for csv_col, field_name in detail_mapping.items():
            val = row.get(csv_col)
            if field_name in numeric_details:
                detail_doc[field_name] = _safe_int(val)
            elif field_name in ("is_dangerous", "is_vacant_due_to_fire", "people_using_property"):
                detail_doc[field_name] = _to_bool(val)
            else:
                detail_doc[field_name] = _safe_str(val)

        detail_docs.append(detail_doc)

        if len(request_docs) >= batch_size:
            _flush_batch(database, request_docs, detail_docs, location_docs_to_insert,
                         detail_collection_name)
            request_docs.clear()
            detail_docs.clear()
            location_docs_to_insert.clear()

    if request_docs:
        _flush_batch(database, request_docs, detail_docs, location_docs_to_insert,
                     detail_collection_name)

    file_key = csv_config["file"]
    stats[file_key] = total_rows


def _flush_batch(database, request_docs, detail_docs, location_docs, detail_collection_name):
    if location_docs:
        database["locations"].insert_many(location_docs, ordered=False)
    if request_docs:
        database["requests"].insert_many(request_docs, ordered=False)
    if detail_docs:
        database[detail_collection_name].insert_many(detail_docs, ordered=False)


def create_base_indexes(database):
    print("Kreiranje indeksa za base shemu...")

    database["requests"].create_index("request_type")
    database["requests"].create_index("creation_date")
    database["requests"].create_index("status")
    database["requests"].create_index("location_id")
    database["requests"].create_index("service_request_number")

    database["locations"].create_index("ward")
    database["locations"].create_index("community_area")
    database["locations"].create_index("police_district")
    database["locations"].create_index("zip_code")
    database["locations"].create_index("street_address")

    database["vehicle_details"].create_index("request_id")
    database["vehicle_details"].create_index("days_parked")

    database["sanitation_details"].create_index("request_id")
    database["sanitation_details"].create_index("sub_type")

    database["environment_details"].create_index("request_id")
    database["environment_details"].create_index("sub_type")

    database["building_details"].create_index("request_id")
    database["building_details"].create_index("is_dangerous")

    print("Indeksi kreirani uspesno.")


def drop_all_collections(database):
    for name in ["requests", "locations", "vehicle_details",
                  "sanitation_details", "environment_details", "building_details"]:
        database[name].drop()
    print("Sve kolekcije obrisane.")
