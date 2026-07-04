"""
10 aggregation pipeline-ova za base (normalizovanu) shemu sa $lookup.
Svaki upit vraca pipeline listu za prosleđivanje u execute_query().
"""


def query_1_resolution_by_type_area():
    """
    Pitanje 1 (Marko): Koje vrste prijava imaju najduze prosecno vreme resavanja
    i kako se to razlikuje po gradskim oblastima (community area)?
    Poslovni znacaj: otkriva probleme koji najvise opterecuju gradske sluzbe.
    """
    return [
        {"$match": {
            "creation_date": {"$ne": None},
            "completion_date": {"$ne": None}
        }},
        {"$lookup": {
            "from": "locations",
            "localField": "location_id",
            "foreignField": "_id",
            "as": "loc"
        }},
        {"$unwind": "$loc"},
        {"$match": {"loc.community_area": {"$ne": None}}},
        {"$addFields": {
            "resolution_days": {
                "$divide": [
                    {"$subtract": ["$completion_date", "$creation_date"]},
                    86400000
                ]
            }
        }},
        {"$group": {
            "_id": {
                "request_type": "$request_type",
                "community_area": "$loc.community_area"
            },
            "sum_days": {"$sum": "$resolution_days"},
            "count": {"$sum": 1}
        }},
        {"$match": {"count": {"$gte": 10}}},
        {"$addFields": {
            "area_avg_days": {"$divide": ["$sum_days", "$count"]}
        }},
        {"$group": {
            "_id": "$_id.request_type",
            "total_days": {"$sum": "$sum_days"},
            "total_count": {"$sum": "$count"},
            "num_areas": {"$sum": 1},
            "areas": {
                "$push": {
                    "community_area": "$_id.community_area",
                    "avg_days": "$area_avg_days"
                }
            }
        }},
        {"$addFields": {
            "overall_avg_days": {"$divide": ["$total_days", "$total_count"]},
            "worst_area_avg": {"$max": "$areas.avg_days"},
            "best_area_avg": {"$min": "$areas.avg_days"}
        }},
        {"$addFields": {
            "area_spread_days": {"$subtract": ["$worst_area_avg", "$best_area_avg"]},
            "worst_area": {"$arrayElemAt": [
                {"$filter": {
                    "input": "$areas",
                    "as": "a",
                    "cond": {"$eq": ["$$a.avg_days", "$worst_area_avg"]}
                }}, 0
            ]},
            "best_area": {"$arrayElemAt": [
                {"$filter": {
                    "input": "$areas",
                    "as": "a",
                    "cond": {"$eq": ["$$a.avg_days", "$best_area_avg"]}
                }}, 0
            ]}
        }},
        {"$sort": {"overall_avg_days": -1}},
        {"$project": {
            "_id": 0,
            "request_type": "$_id",
            "overall_avg_days": {"$round": ["$overall_avg_days", 2]},
            "total_count": 1,
            "num_areas": 1,
            "worst_community_area": "$worst_area.community_area",
            "worst_area_avg_days": {"$round": ["$worst_area_avg", 2]},
            "best_community_area": "$best_area.community_area",
            "best_area_avg_days": {"$round": ["$best_area_avg", 2]},
            "area_spread_days": {"$round": ["$area_spread_days", 2]}
        }}
    ]


def query_2_neglected_areas():
    """
    Pitanje 2 (Marko): Community areas gde je prosecno vreme resavanja infrastrukturnih
    problema vise od 1.5x gradskog proseka. Plus broj zalbi na vegetaciju.
    """
    return [
        {"$lookup": {
            "from": "locations",
            "localField": "location_id",
            "foreignField": "_id",
            "as": "loc"
        }},
        {"$unwind": "$loc"},
        {"$lookup": {
            "from": "environment_details",
            "localField": "_id",
            "foreignField": "request_id",
            "as": "env"
        }},
        {"$unwind": "$env"},
        {"$match": {
            "env.sub_type": {"$in": [
                "pothole", "street_light_all", "street_light_one",
                "tree_debris", "tree_trim"
            ]},
            "creation_date": {"$ne": None},
            "completion_date": {"$ne": None}
        }},
        {"$addFields": {
            "resolution_days": {
                "$divide": [
                    {"$subtract": ["$completion_date", "$creation_date"]},
                    86400000
                ]
            },
            "is_infra": {"$in": ["$env.sub_type", ["pothole", "street_light_all", "street_light_one"]]},
            "is_vegetation": {"$in": ["$env.sub_type", ["tree_debris", "tree_trim"]]}
        }},
        {"$facet": {
            "city_avg": [
                {"$match": {"is_infra": True}},
                {"$group": {"_id": None, "avg": {"$avg": "$resolution_days"}}}
            ],
            "infra_by_area": [
                {"$match": {"is_infra": True}},
                {"$group": {
                    "_id": "$loc.community_area",
                    "infra_avg_days": {"$avg": "$resolution_days"},
                    "infra_count": {"$sum": 1}
                }}
            ],
            "veg_by_area": [
                {"$match": {"is_vegetation": True}},
                {"$group": {
                    "_id": "$loc.community_area",
                    "vegetation_count": {"$sum": 1}
                }}
            ]
        }},
        {"$project": {
            "city_avg": {"$arrayElemAt": ["$city_avg.avg", 0]},
            "infra_by_area": 1,
            "veg_by_area": 1
        }},
        {"$unwind": "$infra_by_area"},
        {"$match": {
            "$expr": {
                "$gt": ["$infra_by_area.infra_avg_days", {"$multiply": ["$city_avg", 1.5]}]
            }
        }},
        {"$project": {
            "_id": 0,
            "community_area": "$infra_by_area._id",
            "avg_resolution_days": {"$round": ["$infra_by_area.infra_avg_days", 2]},
            "city_avg_days": {"$round": ["$city_avg", 2]},
            "ratio_vs_city": {"$round": [{"$divide": ["$infra_by_area.infra_avg_days", "$city_avg"]}, 2]},
            "infra_complaints": "$infra_by_area.infra_count",
            "vegetation_complaints": {
                "$ifNull": [
                    {"$arrayElemAt": [
                        {"$map": {
                            "input": {"$filter": {
                                "input": "$veg_by_area",
                                "as": "v",
                                "cond": {"$eq": ["$$v._id", "$infra_by_area._id"]}
                            }},
                            "as": "matched",
                            "in": "$$matched.vegetation_count"
                        }},
                        0
                    ]},
                    0
                ]
            }
        }},
        {"$sort": {"avg_resolution_days": -1}}
    ]


def query_3_hotspot_locations():
    """
    Pitanje 3 (Marko): Da li postoje lokacije (adrese) koje konstantno generisu
    veliki broj prijava i koje vrste problema u njima dominiraju?
    "Konstantno" = pojavljuju se kroz vise razlicitih godina.
    Poslovni znacaj: identifikacija problematicnih delova grada.
    """
    return [
        {"$match": {"creation_date": {"$ne": None}}},
        {"$lookup": {
            "from": "locations",
            "localField": "location_id",
            "foreignField": "_id",
            "as": "loc"
        }},
        {"$unwind": "$loc"},
        {"$match": {"loc.street_address": {"$ne": None}}},
        {"$addFields": {"year": {"$year": "$creation_date"}}},
        {"$group": {
            "_id": {
                "address": "$loc.street_address",
                "request_type": "$request_type"
            },
            "type_count": {"$sum": 1},
            "type_years": {"$addToSet": "$year"},
            "ward": {"$first": "$loc.ward"},
            "community_area": {"$first": "$loc.community_area"}
        }},
        {"$group": {
            "_id": "$_id.address",
            "total_requests": {"$sum": "$type_count"},
            "types": {
                "$push": {"type": "$_id.request_type", "count": "$type_count"}
            },
            "year_arrays": {"$push": "$type_years"},
            "ward": {"$first": "$ward"},
            "community_area": {"$first": "$community_area"}
        }},
        {"$addFields": {
            "distinct_years": {"$size": {"$reduce": {
                "input": "$year_arrays",
                "initialValue": [],
                "in": {"$setUnion": ["$$value", "$$this"]}
            }}},
            "num_distinct_types": {"$size": "$types"},
            "max_type_count": {"$max": "$types.count"}
        }},
        {"$match": {
            "total_requests": {"$gte": 20},
            "distinct_years": {"$gte": 3}
        }},
        {"$addFields": {
            "dominant": {"$arrayElemAt": [
                {"$filter": {
                    "input": "$types",
                    "as": "t",
                    "cond": {"$eq": ["$$t.count", "$max_type_count"]}
                }}, 0
            ]}
        }},
        {"$addFields": {
            "dominant_share_pct": {"$round": [
                {"$multiply": [
                    {"$divide": ["$dominant.count", "$total_requests"]}, 100
                ]}, 1
            ]}
        }},
        {"$sort": {"total_requests": -1}},
        {"$limit": 20},
        {"$project": {
            "_id": 0,
            "street_address": "$_id",
            "community_area": 1,
            "ward": 1,
            "total_requests": 1,
            "distinct_years": 1,
            "num_distinct_types": 1,
            "dominant_type": "$dominant.type",
            "dominant_count": "$dominant.count",
            "dominant_share_pct": 1
        }}
    ]


def query_4_problem_blocks():
    """
    Pitanje 4 (Marko): Problematični blokovi - adrese sa 5+ razlicitih tipova zalbi
    u jednoj godini.
    """
    return [
        {"$match": {"creation_date": {"$ne": None}}},
        {"$lookup": {
            "from": "locations",
            "localField": "location_id",
            "foreignField": "_id",
            "as": "loc"
        }},
        {"$unwind": "$loc"},
        {"$addFields": {
            "year": {"$year": "$creation_date"},
            "resolution_days": {
                "$cond": {
                    "if": {"$and": [
                        {"$ne": ["$completion_date", None]},
                        {"$ne": ["$creation_date", None]}
                    ]},
                    "then": {"$divide": [
                        {"$subtract": ["$completion_date", "$creation_date"]},
                        86400000
                    ]},
                    "else": None
                }
            }
        }},
        {"$group": {
            "_id": {
                "address": "$loc.street_address",
                "year": "$year"
            },
            "request_types": {"$addToSet": "$request_type"},
            "total_requests": {"$sum": 1},
            "avg_resolution_days": {"$avg": "$resolution_days"},
            "ward": {"$first": "$loc.ward"}
        }},
        {"$addFields": {
            "num_types": {"$size": "$request_types"}
        }},
        {"$match": {"num_types": {"$gte": 5}}},
        {"$sort": {"num_types": -1, "total_requests": -1}},
        {"$limit": 20},
        {"$project": {
            "_id": 0,
            "street_address": "$_id.address",
            "year": "$_id.year",
            "ward": 1,
            "distinct_complaint_types": "$num_types",
            "request_types": 1,
            "total_requests": 1,
            "avg_resolution_days": {"$round": ["$avg_resolution_days", 2]}
        }}
    ]


def query_5_top_types_per_area():
    """
    Pitanje 5 (Marko): Koji je najcesci tip prijave u svakoj gradskoj oblasti
    (community area) i koliki procenat svih prijava u toj oblasti predstavlja?
    Poslovni znacaj: sta je najcesci problem i koliko dominira u odnosu na ostale.
    """
    return [
        {"$lookup": {
            "from": "locations",
            "localField": "location_id",
            "foreignField": "_id",
            "as": "loc"
        }},
        {"$unwind": "$loc"},
        {"$match": {"loc.community_area": {"$ne": None}}},
        {"$group": {
            "_id": {
                "community_area": "$loc.community_area",
                "request_type": "$request_type"
            },
            "type_count": {"$sum": 1}
        }},
        {"$sort": {"type_count": -1}},
        {"$group": {
            "_id": "$_id.community_area",
            "total_requests": {"$sum": "$type_count"},
            "top_type": {"$first": "$_id.request_type"},
            "top_type_count": {"$first": "$type_count"},
            "num_types": {"$sum": 1}
        }},
        {"$addFields": {
            "top_type_pct": {"$round": [
                {"$multiply": [
                    {"$divide": ["$top_type_count", "$total_requests"]}, 100
                ]}, 1
            ]}
        }},
        {"$sort": {"_id": 1}},
        {"$project": {
            "_id": 0,
            "community_area": "$_id",
            "top_request_type": "$top_type",
            "top_type_count": 1,
            "total_requests": 1,
            "top_type_pct": 1,
            "num_types": 1
        }}
    ]


def query_6_rodent_community():
    """
    Pitanje 6 (Uroš): Koje community areas imaju najvise prijava za glodare
    i koliko je prosecno zarazenih objekata po prijavi? Top 15.
    """
    return [
        {"$lookup": {
            "from": "locations",
            "localField": "location_id",
            "foreignField": "_id",
            "as": "loc"
        }},
        {"$unwind": "$loc"},
        {"$lookup": {
            "from": "sanitation_details",
            "localField": "_id",
            "foreignField": "request_id",
            "as": "san"
        }},
        {"$unwind": "$san"},
        {"$match": {"san.sub_type": "rodent_baiting"}},
        {"$group": {
            "_id": "$loc.community_area",
            "complaint_count": {"$sum": 1},
            "total_premises_with_rats": {
                "$sum": {"$ifNull": ["$san.number_of_premises_with_rats", 0]}
            },
            "total_premises_baited": {
                "$sum": {"$ifNull": ["$san.number_of_premises_baited", 0]}
            }
        }},
        {"$sort": {"complaint_count": -1}},
        {"$limit": 15},
        {"$project": {
            "_id": 0,
            "community_area": "$_id",
            "complaint_count": 1,
            "total_premises_with_rats": 1,
            "total_premises_baited": 1
        }}
    ]


def query_7_abandoned_vehicles():
    """
    Pitanje 7 (Uroš): Top 10 ward-ova sa napustenim vozilima parkiranim 30+ dana.
    Koliko ih ima i koliko u proseku stoje?
    """
    return [
        {"$lookup": {
            "from": "locations",
            "localField": "location_id",
            "foreignField": "_id",
            "as": "loc"
        }},
        {"$unwind": "$loc"},
        {"$lookup": {
            "from": "vehicle_details",
            "localField": "_id",
            "foreignField": "request_id",
            "as": "veh"
        }},
        {"$unwind": "$veh"},
        {"$match": {"veh.days_parked": {"$gte": 30}}},
        {"$group": {
            "_id": "$loc.ward",
            "vehicle_count": {"$sum": 1},
            "avg_days_parked": {"$avg": "$veh.days_parked"},
            "max_days_parked": {"$max": "$veh.days_parked"}
        }},
        {"$sort": {"vehicle_count": -1}},
        {"$limit": 10},
        {"$project": {
            "_id": 0,
            "ward": "$_id",
            "vehicle_count": 1,
            "avg_days_parked": {"$round": ["$avg_days_parked", 1]},
            "max_days_parked": 1
        }}
    ]


def query_8_rodent_seasonal():
    """
    Pitanje 8 (Uroš): Sezonski obrazac prijava za glodare - prosek po mesecu
    (preko svih godina). Dupli group: prvo po (godina, mesec), pa po mesecu.
    """
    return [
        {"$lookup": {
            "from": "sanitation_details",
            "localField": "_id",
            "foreignField": "request_id",
            "as": "san"
        }},
        {"$unwind": "$san"},
        {"$match": {
            "san.sub_type": "rodent_baiting",
            "creation_date": {"$ne": None}
        }},
        {"$group": {
            "_id": {
                "year": {"$year": "$creation_date"},
                "month": {"$month": "$creation_date"}
            },
            "count": {"$sum": 1}
        }},
        {"$group": {
            "_id": "$_id.month",
            "avg_complaints": {"$avg": "$count"},
            "min_complaints": {"$min": "$count"},
            "max_complaints": {"$max": "$count"}
        }},
        {"$sort": {"_id": 1}},
        {"$project": {
            "_id": 0,
            "month": "$_id",
            "avg_complaints": {"$round": ["$avg_complaints", 1]},
            "min_complaints": 1,
            "max_complaints": 1
        }}
    ]


def query_9_unresolved_sanitation():
    """
    Pitanje 9 (Uroš): Koje community areas imaju najvise neresolvanih sanitarnih
    zalbi i koji je procenat neresolvanih? Top 15.
    """
    return [
        {"$lookup": {
            "from": "locations",
            "localField": "location_id",
            "foreignField": "_id",
            "as": "loc"
        }},
        {"$unwind": "$loc"},
        {"$lookup": {
            "from": "sanitation_details",
            "localField": "_id",
            "foreignField": "request_id",
            "as": "san"
        }},
        {"$unwind": "$san"},
        {"$group": {
            "_id": "$loc.community_area",
            "total_complaints": {"$sum": 1},
            "unresolved_count": {
                "$sum": {"$cond": [{"$ne": ["$status", "Completed"]}, 1, 0]}
            }
        }},
        {"$addFields": {
            "unresolved_pct": {
                "$round": [
                    {"$multiply": [
                        {"$divide": ["$unresolved_count", "$total_complaints"]},
                        100
                    ]}, 1
                ]
            }
        }},
        {"$sort": {"unresolved_count": -1}},
        {"$limit": 15},
        {"$project": {
            "_id": 0,
            "community_area": "$_id",
            "total_complaints": 1,
            "unresolved_count": 1,
            "unresolved_pct": 1
        }}
    ]


def query_10_dangerous_buildings():
    """
    Pitanje 10 (Uroš): Koje community areas imaju najvise opasnih zgrada?
    Broj opasnih, otvorenih i onih gde ljudi koriste imovinu. Top 15.
    """
    return [
        {"$lookup": {
            "from": "locations",
            "localField": "location_id",
            "foreignField": "_id",
            "as": "loc"
        }},
        {"$unwind": "$loc"},
        {"$lookup": {
            "from": "building_details",
            "localField": "_id",
            "foreignField": "request_id",
            "as": "bld"
        }},
        {"$unwind": "$bld"},
        {"$group": {
            "_id": "$loc.community_area",
            "total_buildings": {"$sum": 1},
            "dangerous_count": {
                "$sum": {"$cond": [{"$eq": ["$bld.is_dangerous", True]}, 1, 0]}
            },
            "open_count": {
                "$sum": {"$cond": [{"$eq": ["$bld.is_open_or_boarded", "Open"]}, 1, 0]}
            },
            "people_using_count": {
                "$sum": {"$cond": [{"$eq": ["$bld.people_using_property", True]}, 1, 0]}
            }
        }},
        {"$sort": {"dangerous_count": -1}},
        {"$limit": 15},
        {"$project": {
            "_id": 0,
            "community_area": "$_id",
            "total_buildings": 1,
            "dangerous_count": 1,
            "open_count": 1,
            "people_using_count": 1
        }}
    ]


QUERIES = {
    "Q1 - Vreme resavanja po tipu i oblasti": query_1_resolution_by_type_area,
    "Q2 - Zanemarene community areas": query_2_neglected_areas,
    "Q3 - Hotspot lokacije": query_3_hotspot_locations,
    "Q4 - Problematični blokovi": query_4_problem_blocks,
    "Q5 - Najcesci tip po oblasti": query_5_top_types_per_area,
    "Q6 - Glodari po community area": query_6_rodent_community,
    "Q7 - Napuštena vozila (30+ dana)": query_7_abandoned_vehicles,
    "Q8 - Sezonski obrazac glodara": query_8_rodent_seasonal,
    "Q9 - Neresolvane sanitarne žalbe": query_9_unresolved_sanitation,
    "Q10 - Opasne zgrade po community area": query_10_dangerous_buildings,
}
