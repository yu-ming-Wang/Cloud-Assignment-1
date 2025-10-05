import os
import requests
import json
from datetime import datetime, timezone
from dotenv import load_dotenv

# load Yelp API Key from .env
load_dotenv()
YELP_API_KEY = os.getenv("YELP_API_KEY")

HEADERS = {"Authorization": f"Bearer {YELP_API_KEY}"}

# 10 types of cuisine
CUISINES = [
    "chinese",
    "korean",
    "mexican",
    "japanese",
    "indian",
    "thai",
    "italian",
    "french",
    "greek",
    "american"
]

def fetch_yelp_restaurants(cuisine, limit=50, max_results=200):
    """fetch restaurants of a certain type, paginated"""
    url = "https://api.yelp.com/v3/businesses/search"
    restaurants = []
    offset = 0

    while offset < max_results:
        params = {
            "term": f"{cuisine} restaurants",
            "location": "Manhattan, NY",
            "limit": limit,
            "offset": offset,
        }
        resp = requests.get(url, headers=HEADERS, params=params)
        data = resp.json()

        businesses = data.get("businesses", [])
        if not businesses:
            break  

        for b in businesses:
            restaurant = {
                "business_id": b["id"],
                "name": b["name"],
                "address": " ".join(b["location"].get("display_address", [])),
                "coordinates": b.get("coordinates", {}),
                "review_count": b.get("review_count", 0),
                "rating": b.get("rating", 0),
                "zip_code": b["location"].get("zip_code", ""),
                "cuisine": cuisine,
                "insertedAtTimestamp": datetime.now(timezone.utc).isoformat(),
            }
            restaurants.append(restaurant)

        offset += limit

    return restaurants


def main():
    all_restaurants = []

    for cuisine in CUISINES:
        print(f"Fetching {cuisine} restaurants...")
        data = fetch_yelp_restaurants(cuisine)
        all_restaurants.extend(data)

    # 去重複（避免 Yelp 重覆回傳）
    unique_restaurants = {r["business_id"]: r for r in all_restaurants}
    print(f"Total unique restaurants: {len(unique_restaurants)}")

    # 存成 JSON 檔
    with open("yelp_restaurants.json", "w", encoding="utf-8") as f:
        json.dump(list(unique_restaurants.values()), f, indent=2)

    print("Saved data to yelp_restaurants.json")


if __name__ == "__main__":
    main()
