import json
import datetime
import boto3

# construct Lex Runtime client
lex_client = boto3.client("lexv2-runtime")

# Configuration
BOT_ID = "7CVVK86EAQ"         # My Lex bot Id
BOT_ALIAS_ID = "TSTALIASID"   # My bot alias Id (test)
LOCALE_ID = "en_US"

def lambda_handler(event, context):
    print("=== Incoming Event ===")
    print(json.dumps(event, indent=2))

    # 1. from API Gateway request get user input
    body = json.loads(event.get("body", "{}"))

    try:
        user_message = body["messages"][0]["unstructured"]["text"]
    except (KeyError, IndexError, TypeError):
        user_message = ""

    if not user_message.strip():
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "No user message found"})
        }

    # 2. call Lex
    lex_response = lex_client.recognize_text(
        botId=BOT_ID,
        botAliasId=BOT_ALIAS_ID,
        localeId=LOCALE_ID,
        sessionId="api-session",   # can be replaced with userId
        text=user_message
    )

    print("=== Lex Response ===")
    print(json.dumps(lex_response, indent=2))

    # 3. from Lex response get reply
    messages = lex_response.get("messages", [])
    if messages:
        bot_reply = messages[0].get("content", "(Lex gave no text)")
    else:
        bot_reply = "(No response from bot)"

    # 4. wrap as API Gateway reply format
    response_body = {
        "messages": [
            {
                "type": "unstructured",
                "unstructured": {
                    "id": "bot1",
                    "text": bot_reply,
                    "timestamp": datetime.datetime.utcnow().isoformat()
                }
            }
        ]
    }

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(response_body)
    }
