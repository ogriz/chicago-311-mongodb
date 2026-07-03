"""
10 aggregation pipeline-ova za base (normalizovanu) shemu sa $lookup.
Svaki upit vraca pipeline listu za prosleđivanje u execute_query().
"""


def query_1_infrastructure_correlation():
    """
    Pitanje 1 (Marko): Za svaki ward, korelacija izmedju rupa na putu i ugasenih
    ulicnih svetala. Top 10 ward-ova sa kombinovanim problemima i prosecno vreme resavanja.
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
            "env.sub_type": {"$in": ["pothole", "street_light_all", "street_light_one"]}
        }},
        {"$addFields": {
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
                "ward": "$loc.ward",
                "type": "$env.sub_type"
            },
            "count": {"$sum": 1},
            "avg_resolution_days": {"$avg": "$resolution_days"}
        }},
        {"$group": {
            "_id": "$_id.ward",
            "types": {
                "$push": {
                    "type": "$_id.type",
                    "count": "$count",
                    "avg_resolution_days": "$avg_resolution_days"
                }
            },
            "total_count": {"$sum": "$count"}
        }},
        {"$sort": {"total_count": -1}},
        {"$limit": 10},
        {"$project": {
            "_id": 0,
            "ward": "$_id",
            "total_infrastructure_problems": "$total_count",
            "breakdown": "$types"
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


def query_3_seasonal_patterns():
    """
    Pitanje 3 (Marko): Sezonski obrasci za razlicite tipove problema po ward-u.
    Mesec sa najvise prijava po tipu i sezonski indeks (max/min).
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
            "env.sub_type": {"$in": ["pothole", "graffiti"]},
            "creation_date": {"$ne": None}
        }},
        {"$group": {
            "_id": {
                "ward": "$loc.ward",
                "type": "$env.sub_type",
                "month": {"$month": "$creation_date"}
            },
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}},
        {"$group": {
            "_id": {"ward": "$_id.ward", "type": "$_id.type"},
            "max_month": {"$first": "$_id.month"},
            "max_count": {"$first": "$count"},
            "min_count": {"$last": "$count"},
            "monthly_counts": {
                "$push": {"month": "$_id.month", "count": "$count"}
            }
        }},
        {"$addFields": {
            "seasonal_index": {
                "$cond": {
                    "if": {"$gt": ["$min_count", 0]},
                    "then": {"$round": [{"$divide": ["$max_count", "$min_count"]}, 2]},
                    "else": None
                }
            }
        }},
        {"$group": {
            "_id": "$_id.ward",
            "patterns": {
                "$push": {
                    "type": "$_id.type",
                    "peak_month": "$max_month",
                    "peak_count": "$max_count",
                    "seasonal_index": "$seasonal_index"
                }
            }
        }},
        {"$sort": {"_id": 1}},
        {"$limit": 20},
        {"$project": {
            "_id": 0,
            "ward": "$_id",
            "patterns": 1
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


def query_5_district_efficiency():
    """
    Pitanje 5 (Marko): Efikasnost resavanja po policijskom distriktu i kategoriji zalbe.
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
        {"$addFields": {
            "resolution_days": {
                "$divide": [
                    {"$subtract": ["$completion_date", "$creation_date"]},
                    86400000
                ]
            }
        }},
        {"$lookup": {
            "from": "environment_details",
            "localField": "_id",
            "foreignField": "request_id",
            "as": "env"
        }},
        {"$lookup": {
            "from": "sanitation_details",
            "localField": "_id",
            "foreignField": "request_id",
            "as": "san"
        }},
        {"$lookup": {
            "from": "vehicle_details",
            "localField": "_id",
            "foreignField": "request_id",
            "as": "veh"
        }},
        {"$lookup": {
            "from": "building_details",
            "localField": "_id",
            "foreignField": "request_id",
            "as": "bld"
        }},
        {"$addFields": {
            "category": {
                "$switch": {
                    "branches": [
                        {"case": {"$gt": [{"$size": "$env"}, 0]}, "then": "infrastruktura"},
                        {"case": {"$gt": [{"$size": "$san"}, 0]}, "then": "sanitacija"},
                        {"case": {"$gt": [{"$size": "$veh"}, 0]}, "then": "vozila"},
                        {"case": {"$gt": [{"$size": "$bld"}, 0]}, "then": "zgrade"},
                    ],
                    "default": "ostalo"
                }
            }
        }},
        {"$match": {"category": {"$ne": "ostalo"}}},
        {"$project": {
            "loc": 1, "resolution_days": 1, "category": 1
        }},
        {"$group": {
            "_id": {
                "district": "$loc.police_district",
                "category": "$category"
            },
            "avg_days": {"$avg": "$resolution_days"},
            "count": {"$sum": 1}
        }},
        {"$group": {
            "_id": "$_id.district",
            "categories": {
                "$push": {
                    "category": "$_id.category",
                    "avg_days": {"$round": ["$avg_days", 2]},
                    "count": "$count"
                }
            },
            "max_avg": {"$max": "$avg_days"},
            "min_avg": {"$min": "$avg_days"}
        }},
        {"$addFields": {
            "response_gap_days": {"$round": [{"$subtract": ["$max_avg", "$min_avg"]}, 2]}
        }},
        {"$sort": {"response_gap_days": -1}},
        {"$limit": 15},
        {"$project": {
            "_id": 0,
            "police_district": "$_id",
            "categories": 1,
            "response_gap_days": 1
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
    "Q1 - Infrastrukturna korelacija po ward-u": query_1_infrastructure_correlation,
    "Q2 - Zanemarene community areas": query_2_neglected_areas,
    "Q3 - Sezonski obrasci": query_3_seasonal_patterns,
    "Q4 - Problematični blokovi": query_4_problem_blocks,
    "Q5 - Efikasnost po distriktu": query_5_district_efficiency,
    "Q6 - Glodari po community area": query_6_rodent_community,
    "Q7 - Napuštena vozila (30+ dana)": query_7_abandoned_vehicles,
    "Q8 - Sezonski obrazac glodara": query_8_rodent_seasonal,
    "Q9 - Neresolvane sanitarne žalbe": query_9_unresolved_sanitation,
    "Q10 - Opasne zgrade po community area": query_10_dangerous_buildings,
}
