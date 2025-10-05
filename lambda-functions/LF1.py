import json
import boto3

# Enable SQS
sqs = boto3.client('sqs')
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/767828756359/Q1"


def lambda_handler(event, context):
    print("Lambda invoked!\n", json.dumps(event))

    # Lex V2's intent name
    intent_name = event['sessionState']['intent']['name']

    if intent_name == "GreetingIntent":
        return handle_greeting_intent(event)
    elif intent_name == "ThankYouIntent":
        return handle_thankyou_intent(event)
    elif intent_name == "DiningSuggestionsIntent":
        return handle_dining_suggestions_intent(event)
    else:
        return fallback_response(event, intent_name)


def handle_greeting_intent(event):
    return close(event, "Fulfilled", "Hi there, how can I help?")


def handle_thankyou_intent(event):
    return close(event, "Fulfilled", "You’re welcome!")


def handle_dining_suggestions_intent(event):
    slots = event['sessionState']['intent'].get('slots', {})
    print("Received slots from Lex:", json.dumps(slots, indent=2))

    # normalize slots
    normalized_slots = normalize_slots(slots)
    print("Normalized slots:", json.dumps(normalized_slots, indent=2))

    # send to SQS
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(normalized_slots)
    )

    email = normalized_slots.get("Email", "unknown")
    return close(
        event,
        "Fulfilled",
        f"Thanks, I’ve received your request. I’ll email you at {email} with restaurant suggestions!"
    )


def fallback_response(event, intent_name):
    return close(event, "Fulfilled", f"Intent {intent_name} not implemented yet.")


# 🔹 Helper: Lex slots → clean dict
def normalize_slots(slots):
    clean = {}
    for key, val in slots.items():
        if val and "value" in val:
            clean[key] = val["value"].get("interpretedValue") or val["value"].get("originalValue")
        else:
            clean[key] = None
    return clean


# Lex V2 response format
def close(event, fulfillment_state, message):
    return {
        "sessionState": {
            "dialogAction": {
                "type": "Close"
            },
            "intent": {
                "name": event['sessionState']['intent']['name'],
                "state": fulfillment_state
            }
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": message
            }
        ]
    }
