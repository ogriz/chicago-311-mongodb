import time


def execute_query(collection, pipeline, query_name="Upit"):
    start = time.time()
    results = list(collection.aggregate(pipeline, allowDiskUse=True))
    elapsed = time.time() - start
    print(f"{query_name}: {elapsed:.3f}s ({len(results)} rezultata)")
    return results, elapsed


def execute_query_explain(collection, pipeline, query_name="Upit"):
    explain = collection.aggregate(pipeline, allowDiskUse=True).explain()
    return explain
