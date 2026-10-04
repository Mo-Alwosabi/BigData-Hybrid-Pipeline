import argparse
import json
from src.mongo_setup import create_mongo_client, get_database
from config.settings import VALIDATED_COLLECTION

def sales_by_city(collection):
    pipeline = [
        {
            "$group": {
                "_id": "$city",
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": {"$ifNull": ["$total_amount", 0]}},
                "average_order": {"$avg": {"$ifNull": ["$total_amount", 0]}}
            }
        },
        {"$sort": {"total_sales": -1}},
        {"$limit": 20},
        {
            "$project": {
                "_id": 0,
                "city": "$_id",
                "order_count": 1,
                "total_sales": 1,
                "average_order": 1
            }
        }
    ]
    return list(collection.aggregate(pipeline, allowDiskUse=True))

def top_customers(collection):
    pipeline = [
        {
            "$group": {
                "_id": {
                    "customer_id": "$customer_id",
                    "customer_name": "$customer_name"
                },
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": {"$ifNull": ["$total_amount", 0]}}
            }
        },
        {"$sort": {"total_sales": -1}},
        {"$limit": 20},
        {
            "$project": {
                "_id": 0,
                "customer_id": "$_id.customer_id",
                "customer_name": "$_id.customer_name",
                "order_count": 1,
                "total_sales": 1
            }
        }
    ]
    return list(collection.aggregate(pipeline, allowDiskUse=True))

def sales_by_status(collection):
    pipeline = [
        {
            "$group": {
                "_id": "$status",
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": {"$ifNull": ["$total_amount", 0]}}
            }
        },
        {"$sort": {"total_sales": -1}},
        {
            "$project": {
                "_id": 0,
                "status": "$_id",
                "order_count": 1,
                "total_sales": 1
            }
        }
    ]
    return list(collection.aggregate(pipeline, allowDiskUse=True))

def sales_by_payment_method(collection):
    pipeline = [
        {
            "$group": {
                "_id": "$payment_method",
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": {"$ifNull": ["$total_amount", 0]}}
            }
        },
        {"$sort": {"total_sales": -1}},
        {
            "$project": {
                "_id": 0,
                "payment_method": "$_id",
                "order_count": 1,
                "total_sales": 1
            }
        }
    ]
    return list(collection.aggregate(pipeline, allowDiskUse=True))

def sales_by_delivery_type(collection):
    pipeline = [
        {
            "$group": {
                "_id": "$delivery_type",
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": {"$ifNull": ["$total_amount", 0]}}
            }
        },
        {"$sort": {"total_sales": -1}},
        {
            "$project": {
                "_id": 0,
                "delivery_type": "$_id",
                "order_count": 1,
                "total_sales": 1
            }
        }
    ]
    return list(collection.aggregate(pipeline, allowDiskUse=True))

REPORTS = {
    "sales_by_city": sales_by_city,
    "top_customers": top_customers,
    "sales_by_status": sales_by_status,
    "sales_by_payment_method": sales_by_payment_method,
    "sales_by_delivery_type": sales_by_delivery_type
}

def main():
    parser = argparse.ArgumentParser(description="Aggregation Reports")
    parser.add_argument(
        "--report",
        required=True,
        choices=REPORTS.keys()
    )
    args = parser.parse_args()

    client = None
    try:
        client = create_mongo_client()
        db = get_database(client)
        collection = db[VALIDATED_COLLECTION]
        result = REPORTS[args.report](collection)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    finally:
        if client is not None:
            client.close()

if __name__ == "__main__":
    main()