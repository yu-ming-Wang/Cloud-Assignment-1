import json
import random
import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.session import get_session

# Config
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/767828756359/Q1"
OPENSEARCH_ENDPOINT = "https://search-cc-hw1-yuming-xpxvjm3flmqfbl2ziszhq5wm74.aos.us-east-1.on.aws"
INDEX_NAME = "restaurants"
DYNAMO_TABLE = "yelp-restaurants"
REGION = "us-east-1"

# AWS Clients
sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")
ses = boto3.client("ses")
table = dynamodb.Table(DYNAMO_TABLE)


# OpenSearch API request
def signed_es_request(query):
    creds = boto3.Session().get_credentials()
    session = get_session()

    request = AWSRequest(
        method="POST",
        url=f"{OPENSEARCH_ENDPOINT}/{INDEX_NAME}/_search",
        data=json.dumps(query).encode("utf-8"),
        headers={"Content-Type": "application/json"}
    )

    SigV4Auth(creds, "es", REGION).add_auth(request)

    # use boto3 http session
    import urllib3
    http = urllib3.PoolManager()
    response = http.request(
        "POST",
        request.url,
        body=request.body,
        headers=dict(request.headers)
    )
    return response


def lambda_handler(event, context):
    print("LF2 triggered!")

    # 1. poll message from SQS 
    resp = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=5
    )

    if "Messages" not in resp:
        print("No messages in queue.")
        return {"status": "empty"}

    msg = resp["Messages"][0]
    body = json.loads(msg["Body"])
    print("Got message:", body)

    cuisine = body.get("Cuisine")
    email = body.get("Email")
    people = body.get("NumberOfPeople", "some friends")
    dining_time = body.get("DiningTime", "soon")
    location = body.get("Location", "your area")

    try:# 2. Query from OpenSearch 
        query = {
            "query": {"term": {"Cuisine": cuisine}},
            "size": 50
        }

        res = signed_es_request(query)
        print("OpenSearch status:", res.status)

        raw_response = res.data.decode("utf-8")
        print("OpenSearch response (truncated):", raw_response[:500])

        try:
            hits = json.loads(raw_response)["hits"]["hits"]
        except Exception as e:
            print("Failed to parse OpenSearch response:", str(e))
            return {"status": "opensearch_error", "raw_response": raw_response[:500]}

        if not hits:
            print("No restaurants found for cuisine:", cuisine)
            return {"status": "no_results"}

        # Random select 5 restaurants 
        picks = random.sample(hits, min(5, len(hits)))

        recommendations = []
        for i, pick in enumerate(picks, 1):
            business_id = pick["_source"]["BusinessID"]
            dynamo_res = table.get_item(Key={"BusinessID": business_id})
            item = dynamo_res.get("Item", {})
            name = item.get("Name", "Unknown Restaurant")
            address = item.get("Address", "Unknown Address")
            recommendations.append(f"{i}. {name}, located at {address}")

        # 3. Send Email
        subject = f"Your {cuisine} restaurant suggestions 🍴"
        body_text = (
            f"Hello!\n\n"
            f"Based on your request for {cuisine} food in {location} "
            f"for {people} at {dining_time}, here are some suggestions:\n\n"
            + "\n".join(recommendations)
            + "\n\nEnjoy your meal! 😋"
        )

        ses.send_email(
            Source="yw8988@nyu.edu",  
            Destination={"ToAddresses": [email]},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Text": {"Data": body_text}}
            }
        )

        # 4. Delete queue message
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"])

        return {"status": "success", "recommended": recommendations, "email": email}

    except Exception as e:
       
        print(f"[ERROR] requestId={context.aws_request_id}, reason={str(e)}")
        raise e  

