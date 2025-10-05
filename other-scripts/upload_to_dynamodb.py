import json
import os
from decimal import Decimal
from datetime import datetime, timezone
from dotenv import load_dotenv
import boto3

# 載入 .env
load_dotenv()

# 從環境變數讀取 AWS Key & Region
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

TABLE_NAME = "yelp-restaurants"

# 建立 DynamoDB client
dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)
table = dynamodb.Table(TABLE_NAME)


def to_decimal(obj):
    """將 float 轉成 Decimal (DynamoDB 需要)"""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, list):
        return [to_decimal(x) for x in obj]
    if isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    return obj


def map_item(src):
    """把 JSON 的餐廳資料轉成 DynamoDB 格式"""
    coords = src.get("coordinates", {}) or {}
    inserted = src.get("insertedAtTimestamp") or datetime.now(timezone.utc).isoformat()

    item = {
        "BusinessID": src.get("business_id"),   # Partition Key
        "Name": src.get("name", ""),
        "Address": src.get("address", ""),
        "Coordinates": {
            "Latitude": to_decimal(coords.get("latitude", 0)),
            "Longitude": to_decimal(coords.get("longitude", 0)),
        },
        "NumberOfReviews": src.get("review_count", 0),
        "Rating": to_decimal(src.get("rating", 0)),
        "ZipCode": src.get("zip_code", ""),
        "Cuisine": src.get("cuisine", ""),
        "insertedAtTimestamp": inserted,
    }
    return item


def main():
    with open("yelp_restaurants.json", "r", encoding="utf-8") as f:
        restaurants = json.load(f)

    for r in restaurants:
        item = map_item(r)
        table.put_item(Item=item)

    print(f"✅ Uploaded {len(restaurants)} restaurants to DynamoDB table '{TABLE_NAME}'")


if __name__ == "__main__":
    main()
