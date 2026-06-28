"""
10 aggregation pipeline-ova za optimizovanu (embedded) shemu - bez $lookup.
Koriste kolekciju 'requests_optimized'.
"""

COLLECTION = "requests_optimized"


def query_1_infrastructure_correlation():
    """
    Pitanje 1 (Marko): Korelacija rupa na putu i ugasenih svetala po ward-u.
    """
    return [
        {"$match": {
            "details.sub_type": {"$in": ["pothole", "street_light_all", "street_light_one"]}
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
                "ward": "$location.ward",
                "type": "$details.sub_type"
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
    Pitanje 2 (Marko): Community areas sa 1.5x prosecnim vremenom resavanja.
    """
    return [
        {"$match": {
            "details.sub_type": {"$in": [
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
            "is_infra": {"$in": ["$details.sub_type", ["pothole", "street_light_all", "street_light_one"]]},
            "is_vegetation": {"$in": ["$details.sub_type", ["tree_debris", "tree_trim"]]}
        }},
        {"$facet": {
            "city_avg": [
                {"$match": {"is_infra": True}},
                {"$group": {"_id": None, "avg": {"$avg": "$resolution_days"}}}
            ],
            "infra_by_area": [
                {"$match": {"is_infra": True}},
                {"$group": {
                    "_id": "$location.community_area",
                    "infra_avg_days": {"$avg": "$resolution_days"},
                    "infra_count": {"$sum": 1}
                }}
            ],
            "veg_by_area": [
                {"$match": {"is_vegetation": True}},
                {"$group": {
                    "_id": "$location.community_area",
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
    Pitanje 3 (Marko): Sezonski obrasci za rupe i grafite po ward-u.
    """
    return [
        {"$match": {
            "details.sub_type": {"$in": ["pothole", "graffiti"]},
            "creation_date": {"$ne": None}
        }},
        {"$group": {
            "_id": {
                "ward": "$location.ward",
                "type": "$details.sub_type",
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
    Pitanje 4 (Marko): Adrese sa 5+ razlicitih tipova zalbi u jednoj godini.
    """
    return [
        {"$match": {"creation_date": {"$ne": None}}},
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
                "address": "$location.street_address",
                "year": "$year"
            },
            "request_types": {"$addToSet": "$request_type"},
            "total_requests": {"$sum": 1},
            "avg_resolution_days": {"$avg": "$resolution_days"},
            "ward": {"$first": "$location.ward"}
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
    Pitanje 5 (Marko): Efikasnost resavanja po policijskom distriktu i kategoriji.
    """
    return [
        {"$match": {
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
            "category": {
                "$switch": {
                    "branches": [
                        {"case": {"$in": ["$details.sub_type", [
                            "pothole", "street_light_all", "street_light_one",
                            "alley_light", "graffiti", "tree_debris", "tree_trim"
                        ]]}, "then": "infrastruktura"},
                        {"case": {"$in": ["$details.sub_type", [
                            "rodent_baiting", "sanitation_code", "garbage_cart"
                        ]]}, "then": "sanitacija"},
                        {"case": {"$in": ["$request_type", [
                            "Abandoned Vehicle Complaint"
                        ]]}, "then": "vozila"},
                        {"case": {"$in": ["$request_type", [
                            "Vacant/Abandoned Building"
                        ]]}, "then": "zgrade"},
                    ],
                    "default": "ostalo"
                }
            }
        }},
        {"$match": {"category": {"$ne": "ostalo"}}},
        {"$group": {
            "_id": {
                "district": "$location.police_district",
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


def query_6_health_score():
    """
    Pitanje 6 (Uroš): Mesecni health score po community_area.
    """
    return [
        {"$match": {
            "details.sub_type": {"$in": ["rodent_baiting", "sanitation_code", "garbage_cart"]},
            "creation_date": {"$ne": None}
        }},
        {"$group": {
            "_id": {
                "community_area": "$location.community_area",
                "year": {"$year": "$creation_date"},
                "month": {"$month": "$creation_date"}
            },
            "rodent_count": {
                "$sum": {"$cond": [{"$eq": ["$details.sub_type", "rodent_baiting"]}, 1, 0]}
            },
            "sanitation_count": {
                "$sum": {"$cond": [{"$eq": ["$details.sub_type", "sanitation_code"]}, 1, 0]}
            },
            "garbage_count": {
                "$sum": {"$cond": [{"$eq": ["$details.sub_type", "garbage_cart"]}, 1, 0]}
            }
        }},
        {"$addFields": {
            "health_score": {
                "$add": [
                    {"$multiply": ["$rodent_count", 3]},
                    {"$multiply": ["$sanitation_count", 2]},
                    {"$multiply": ["$garbage_count", 1]}
                ]
            }
        }},
        {"$group": {
            "_id": "$_id.community_area",
            "avg_monthly_health_score": {"$avg": "$health_score"},
            "total_rodent": {"$sum": "$rodent_count"},
            "total_sanitation": {"$sum": "$sanitation_count"},
            "total_garbage": {"$sum": "$garbage_count"}
        }},
        {"$sort": {"avg_monthly_health_score": -1}},
        {"$limit": 15},
        {"$project": {
            "_id": 0,
            "community_area": "$_id",
            "avg_monthly_health_score": {"$round": ["$avg_monthly_health_score", 2]},
            "total_rodent_complaints": "$total_rodent",
            "total_sanitation_violations": "$total_sanitation",
            "total_garbage_complaints": "$total_garbage"
        }}
    ]


def query_7_urban_blight():
    """
    Pitanje 7 (Uroš): Urban blight - vozila, zgrade, grafiti po ward-u.
    """
    return [
        {"$facet": {
            "vehicles": [
                {"$match": {"request_type": "Abandoned Vehicle Complaint"}},
                {"$group": {
                    "_id": "$location.ward",
                    "vehicle_count": {"$sum": 1},
                    "avg_days_vehicles": {
                        "$avg": {
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
                    }
                }}
            ],
            "graffiti": [
                {"$match": {"details.sub_type": "graffiti"}},
                {"$group": {
                    "_id": "$location.ward",
                    "graffiti_count": {"$sum": 1},
                    "avg_days_graffiti": {
                        "$avg": {
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
                    }
                }}
            ],
            "buildings": [
                {"$match": {"request_type": "Vacant/Abandoned Building"}},
                {"$group": {
                    "_id": "$location.ward",
                    "building_count": {"$sum": 1},
                    "dangerous_count": {
                        "$sum": {"$cond": [{"$eq": ["$details.is_dangerous", True]}, 1, 0]}
                    },
                    "open_count": {
                        "$sum": {"$cond": [{"$eq": ["$details.is_open_or_boarded", "Open"]}, 1, 0]}
                    }
                }}
            ]
        }},
        {"$project": {
            "combined": {
                "$map": {
                    "input": "$vehicles",
                    "as": "v",
                    "in": {
                        "ward": "$$v._id",
                        "vehicle_count": "$$v.vehicle_count",
                        "avg_days_vehicles": "$$v.avg_days_vehicles",
                        "graffiti": {
                            "$arrayElemAt": [
                                {"$filter": {
                                    "input": "$graffiti",
                                    "as": "g",
                                    "cond": {"$eq": ["$$g._id", "$$v._id"]}
                                }}, 0
                            ]
                        },
                        "building": {
                            "$arrayElemAt": [
                                {"$filter": {
                                    "input": "$buildings",
                                    "as": "b",
                                    "cond": {"$eq": ["$$b._id", "$$v._id"]}
                                }}, 0
                            ]
                        }
                    }
                }
            }
        }},
        {"$unwind": "$combined"},
        {"$match": {
            "combined.graffiti": {"$ne": None},
            "combined.building": {"$ne": None}
        }},
        {"$project": {
            "_id": 0,
            "ward": "$combined.ward",
            "vehicle_count": "$combined.vehicle_count",
            "graffiti_count": "$combined.graffiti.graffiti_count",
            "building_count": "$combined.building.building_count",
            "dangerous_buildings": "$combined.building.dangerous_count",
            "open_buildings": "$combined.building.open_count",
            "avg_days_vehicles": {"$round": [{"$ifNull": ["$combined.avg_days_vehicles", 0]}, 2]},
            "avg_days_graffiti": {"$round": [{"$ifNull": ["$combined.graffiti.avg_days_graffiti", 0]}, 2]}
        }},
        {"$addFields": {
            "total_blight_score": {
                "$add": ["$vehicle_count", "$graffiti_count", "$building_count"]
            }
        }},
        {"$sort": {"total_blight_score": -1}},
        {"$limit": 15}
    ]


def query_8_rodent_trend():
    """
    Pitanje 8 (Uroš): YoY rast glodara po ward-u (30%+).
    """
    return [
        {"$match": {
            "details.sub_type": {"$in": ["rodent_baiting", "sanitation_code", "garbage_cart"]},
            "creation_date": {"$ne": None}
        }},
        {"$addFields": {"year": {"$year": "$creation_date"}}},
        {"$group": {
            "_id": {
                "ward": "$location.ward",
                "year": "$year",
                "sub_type": "$details.sub_type"
            },
            "count": {"$sum": 1}
        }},
        {"$group": {
            "_id": {"ward": "$_id.ward", "sub_type": "$_id.sub_type"},
            "yearly": {
                "$push": {"year": "$_id.year", "count": "$count"}
            }
        }},
        {"$unwind": "$yearly"},
        {"$sort": {"_id.ward": 1, "_id.sub_type": 1, "yearly.year": 1}},
        {"$group": {
            "_id": {"ward": "$_id.ward", "sub_type": "$_id.sub_type"},
            "years": {"$push": "$yearly"}
        }},
        {"$addFields": {
            "year_pairs": {
                "$map": {
                    "input": {"$range": [1, {"$size": "$years"}]},
                    "as": "i",
                    "in": {
                        "year": {"$arrayElemAt": ["$years.year", "$$i"]},
                        "current": {"$arrayElemAt": ["$years.count", "$$i"]},
                        "previous": {"$arrayElemAt": ["$years.count", {"$subtract": ["$$i", 1]}]},
                        "growth": {
                            "$cond": {
                                "if": {"$gt": [{"$arrayElemAt": ["$years.count", {"$subtract": ["$$i", 1]}]}, 0]},
                                "then": {
                                    "$divide": [
                                        {"$subtract": [
                                            {"$arrayElemAt": ["$years.count", "$$i"]},
                                            {"$arrayElemAt": ["$years.count", {"$subtract": ["$$i", 1]}]}
                                        ]},
                                        {"$arrayElemAt": ["$years.count", {"$subtract": ["$$i", 1]}]}
                                    ]
                                },
                                "else": 0
                            }
                        }
                    }
                }
            }
        }},
        {"$unwind": "$year_pairs"},
        {"$match": {
            "_id.sub_type": "rodent_baiting",
            "year_pairs.growth": {"$gte": 0.3},
            "year_pairs.previous": {"$gte": 10}
        }},
        {"$project": {
            "_id": 0,
            "ward": "$_id.ward",
            "year": "$year_pairs.year",
            "rodent_count": "$year_pairs.current",
            "previous_year_count": "$year_pairs.previous",
            "growth_pct": {"$round": [{"$multiply": ["$year_pairs.growth", 100]}, 1]}
        }},
        {"$sort": {"growth_pct": -1}},
        {"$limit": 20}
    ]


def query_9_response_inequality():
    """
    Pitanje 9 (Uroš): Response inequality index po community_area.
    """
    return [
        {"$match": {
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
            "is_sanitation": {"$in": ["$details.sub_type", ["rodent_baiting", "sanitation_code", "garbage_cart"]]},
            "is_infra": {"$in": ["$details.sub_type", [
                "pothole", "street_light_all", "street_light_one",
                "alley_light", "graffiti", "tree_debris", "tree_trim"
            ]]}
        }},
        {"$match": {"$or": [{"is_sanitation": True}, {"is_infra": True}]}},
        {"$facet": {
            "sanitation": [
                {"$match": {"is_sanitation": True}},
                {"$group": {
                    "_id": "$location.community_area",
                    "avg_san_days": {"$avg": "$resolution_days"},
                    "san_count": {"$sum": 1}
                }}
            ],
            "infrastructure": [
                {"$match": {"is_infra": True}},
                {"$group": {
                    "_id": "$location.community_area",
                    "avg_infra_days": {"$avg": "$resolution_days"},
                    "infra_count": {"$sum": 1}
                }}
            ]
        }},
        {"$project": {
            "combined": {
                "$map": {
                    "input": "$sanitation",
                    "as": "s",
                    "in": {
                        "community_area": "$$s._id",
                        "avg_san_days": "$$s.avg_san_days",
                        "san_count": "$$s.san_count",
                        "infra": {
                            "$arrayElemAt": [
                                {"$filter": {
                                    "input": "$infrastructure",
                                    "as": "i",
                                    "cond": {"$eq": ["$$i._id", "$$s._id"]}
                                }}, 0
                            ]
                        }
                    }
                }
            }
        }},
        {"$unwind": "$combined"},
        {"$match": {"combined.infra": {"$ne": None}}},
        {"$project": {
            "_id": 0,
            "community_area": "$combined.community_area",
            "avg_sanitation_days": {"$round": ["$combined.avg_san_days", 2]},
            "avg_infrastructure_days": {"$round": ["$combined.infra.avg_infra_days", 2]},
            "inequality_index": {
                "$round": [
                    {"$subtract": ["$combined.avg_san_days", "$combined.infra.avg_infra_days"]},
                    2
                ]
            },
            "sanitation_complaints": "$combined.san_count",
            "infrastructure_complaints": "$combined.infra.infra_count"
        }},
        {"$sort": {"inequality_index": -1}},
        {"$limit": 15}
    ]


def query_10_risk_map():
    """
    Pitanje 10 (Uroš): Risk map - kompozitni skor po community_area.
    """
    return [
        {"$facet": {
            "rodents": [
                {"$match": {"details.sub_type": "rodent_baiting"}},
                {"$group": {
                    "_id": "$location.community_area",
                    "total_premises_with_rats": {
                        "$sum": {"$ifNull": ["$details.number_of_premises_with_rats", 0]}
                    },
                    "rodent_complaints": {"$sum": 1}
                }}
            ],
            "unresolved_sanitation": [
                {"$match": {
                    "details.sub_type": {"$in": ["rodent_baiting", "sanitation_code", "garbage_cart"]}
                }},
                {"$group": {
                    "_id": "$location.community_area",
                    "total_san": {"$sum": 1},
                    "unresolved_san": {
                        "$sum": {"$cond": [{"$ne": ["$status", "Completed"]}, 1, 0]}
                    }
                }},
                {"$addFields": {
                    "unresolved_pct": {
                        "$cond": {
                            "if": {"$gt": ["$total_san", 0]},
                            "then": {"$multiply": [
                                {"$divide": ["$unresolved_san", "$total_san"]}, 100
                            ]},
                            "else": 0
                        }
                    }
                }}
            ],
            "old_vehicles": [
                {"$match": {"details.days_parked": {"$gte": 30}}},
                {"$group": {
                    "_id": "$location.community_area",
                    "old_vehicle_count": {"$sum": 1}
                }}
            ],
            "dangerous_buildings": [
                {"$match": {"request_type": "Vacant/Abandoned Building"}},
                {"$group": {
                    "_id": "$location.community_area",
                    "dangerous_count": {
                        "$sum": {"$cond": [{"$eq": ["$details.is_dangerous", True]}, 1, 0]}
                    },
                    "open_count": {
                        "$sum": {"$cond": [{"$eq": ["$details.is_open_or_boarded", "Open"]}, 1, 0]}
                    }
                }}
            ]
        }},
        {"$project": {
            "areas": {
                "$setUnion": [
                    "$rodents._id",
                    "$unresolved_sanitation._id",
                    "$old_vehicles._id",
                    "$dangerous_buildings._id"
                ]
            },
            "rodents": 1,
            "unresolved_sanitation": 1,
            "old_vehicles": 1,
            "dangerous_buildings": 1
        }},
        {"$unwind": "$areas"},
        {"$project": {
            "community_area": "$areas",
            "rodent": {
                "$arrayElemAt": [
                    {"$filter": {"input": "$rodents", "as": "r", "cond": {"$eq": ["$$r._id", "$areas"]}}}, 0
                ]
            },
            "sanitation": {
                "$arrayElemAt": [
                    {"$filter": {"input": "$unresolved_sanitation", "as": "s", "cond": {"$eq": ["$$s._id", "$areas"]}}}, 0
                ]
            },
            "vehicles": {
                "$arrayElemAt": [
                    {"$filter": {"input": "$old_vehicles", "as": "v", "cond": {"$eq": ["$$v._id", "$areas"]}}}, 0
                ]
            },
            "buildings": {
                "$arrayElemAt": [
                    {"$filter": {"input": "$dangerous_buildings", "as": "b", "cond": {"$eq": ["$$b._id", "$areas"]}}}, 0
                ]
            }
        }},
        {"$addFields": {
            "risk_score": {
                "$add": [
                    {"$multiply": [{"$ifNull": ["$rodent.total_premises_with_rats", 0]}, 0.01]},
                    {"$multiply": [{"$ifNull": ["$sanitation.unresolved_pct", 0]}, 10]},
                    {"$multiply": [{"$ifNull": ["$vehicles.old_vehicle_count", 0]}, 2]},
                    {"$multiply": [
                        {"$add": [
                            {"$ifNull": ["$buildings.dangerous_count", 0]},
                            {"$ifNull": ["$buildings.open_count", 0]}
                        ]}, 5
                    ]}
                ]
            }
        }},
        {"$sort": {"risk_score": -1}},
        {"$limit": 10},
        {"$project": {
            "_id": 0,
            "community_area": 1,
            "risk_score": {"$round": ["$risk_score", 2]},
            "premises_with_rats": {"$ifNull": ["$rodent.total_premises_with_rats", 0]},
            "rodent_complaints": {"$ifNull": ["$rodent.rodent_complaints", 0]},
            "unresolved_sanitation_pct": {"$round": [{"$ifNull": ["$sanitation.unresolved_pct", 0]}, 1]},
            "vehicles_30_plus_days": {"$ifNull": ["$vehicles.old_vehicle_count", 0]},
            "dangerous_buildings": {"$ifNull": ["$buildings.dangerous_count", 0]},
            "open_buildings": {"$ifNull": ["$buildings.open_count", 0]}
        }}
    ]


QUERIES = {
    "Q1 - Infrastrukturna korelacija po ward-u": query_1_infrastructure_correlation,
    "Q2 - Zanemarene community areas": query_2_neglected_areas,
    "Q3 - Sezonski obrasci": query_3_seasonal_patterns,
    "Q4 - Problematični blokovi": query_4_problem_blocks,
    "Q5 - Efikasnost po distriktu": query_5_district_efficiency,
    "Q6 - Health score po community area": query_6_health_score,
    "Q7 - Urban blight analiza": query_7_urban_blight,
    "Q8 - Trend glodara (YoY)": query_8_rodent_trend,
    "Q9 - Response inequality index": query_9_response_inequality,
    "Q10 - Risk map": query_10_risk_map,
}
