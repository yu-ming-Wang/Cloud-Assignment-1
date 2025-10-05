import os
import json
import boto3
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
import requests

# load environment variables
load_dotenv()

# DynamoDB
dynamodb = boto3.resource(
    'dynamodb',
    region_name=os.getenv("AWS_REGION")
)
table = dynamodb.Table("yelp-restaurants")


# OpenSearch
OPENSEARCH_ENDPOINT = os.getenv("OPENSEARCH_ENDPOINT")
OPENSEARCH_USER = os.getenv("OPENSEARCH_USER")
OPENSEARCH_PASS = os.getenv("OPENSEARCH_PASS")
INDEX_NAME = "restaurants"

# create index
def create_index():
    url = f"{OPENSEARCH_ENDPOINT}/{INDEX_NAME}"
    mapping = {
        "mappings": {
            "properties": {
                "BusinessID": {"type": "keyword"},
                "Cuisine": {"type": "keyword"}
            }
        }
    }
    resp = requests.put(
        url,
        auth=HTTPBasicAuth(OPENSEARCH_USER, OPENSEARCH_PASS),
        headers={"Content-Type": "application/json"},
        data=json.dumps(mapping)
    )
    print("Create index response:", resp.text)

# bulk upload
def bulk_upload(items):
    bulk_data = ""
    for item in items:
        action = {"index": {"_index": INDEX_NAME, "_id": item["BusinessID"]}}
        bulk_data += json.dumps(action) + "\n"
        doc = {
            "BusinessID": item["BusinessID"],
            "Cuisine": item["Cuisine"] 
        }
        bulk_data += json.dumps(doc) + "\n"

    url = f"{OPENSEARCH_ENDPOINT}/_bulk"
    resp = requests.post(
        url,
        auth=HTTPBasicAuth(OPENSEARCH_USER, OPENSEARCH_PASS),
        headers={"Content-Type": "application/json"},
        data=bulk_data.encode("utf-8")
    )
    print("Bulk upload response:", resp.text[:200])

def main():
    create_index()
    response = table.scan()
    items = response["Items"]
    print(f"Fetched {len(items)} items from DynamoDB")
    bulk_upload(items)

if __name__ == "__main__":
    main()
