import base64
import json
import os
import requests
from fastapi import FastAPI, Request

from google.cloud import tasks_v2
from google.cloud.tasks_v2 import HttpMethod

app = FastAPI()

# -------------------------------------------------------------------
# ENVIRONMENT VARIABLES (set these in Cloud Run)
# -------------------------------------------------------------------
PROJECT_ID = os.getenv("PROJECT_ID")                      # your GCP project (e.g. total-pier-471215-b0)
QUEUE_ID = os.getenv("QUEUE_ID", "gmail-to-bubble")       # Cloud Tasks queue name
QUEUE_LOCATION = os.getenv("QUEUE_LOCATION", "europe-west1")

# Bubble endpoints
BUBBLE_CREDS_URL = os.getenv("BUBBLE_CREDS_URL")          # Bubble: get-google-creds
BUBBLE_MESSAGE_ADDED_URL = os.getenv("BUBBLE_MESSAGE_ADDED_URL")  # Bubble: gmail_message_added
BUBBLE_API_KEY = os.getenv("BUBBLE_API_KEY")              # stored in Secret Manager

# -------------------------------------------------------------------
# BUBBLE HELPERS
# -------------------------------------------------------------------
def fetch_creds_from_bubble(email: str):
    """Pull Gmail access/refresh tokens from Bubble."""
    headers = {
        "Authorization": f"Bearer {BUBBLE_API_KEY}",
        "Content-Type": "application/json"
    }

    resp = requests.post(
        BUBBLE_CREDS_URL,
        json={"emailAddress": email},
        headers=headers,
        timeout=20
    )

    resp.raise_for_status()
    data = resp.json().get("response", {})
    return data.get("access_token"), data.get("refresh_token")


# -------------------------------------------------------------------
# GMAIL HELPERS
# -------------------------------------------------------------------
def auth_headers(token: str):
    return {"Authorization": f"Bearer {token}"}


def gmail_history(email: str, access_token: str, start_history_id: str):
    """Fetch Gmail history filtered to messageAdded events."""
    url = f"https://gmail.googleapis.com/gmail/v1/users/{email}/history"
    params = {
        "startHistoryId": start_history_id,
        "historyTypes": "messageAdded"
    }

    r = requests.get(url, headers=auth_headers(access_token), params=params, timeout=30)

    if r.status_code == 404:
        return None  # history too old

    r.raise_for_status()
    return r.json()


# -------------------------------------------------------------------
# CLOUD TASKS (fan-out messages to Bubble)
# -------------------------------------------------------------------
def enqueue_to_bubble(payload: dict):
    """Enqueue one messageAdded event to Bubble using Cloud Tasks."""
    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(PROJECT_ID, QUEUE_LOCATION, QUEUE_ID)

    task = {
        "http_request": {
            "http_method": HttpMethod.POST,
            "url": BUBBLE_MESSAGE_ADDED_URL,
            "headers": {
                "Authorization": f"Bearer {BUBBLE_API_KEY}",
                "Content-Type": "application/json"
            },
            "body": json.dumps(payload).encode(),
        }
    }

    client.create_task(parent=parent, task=task)


# -------------------------------------------------------------------
# OPTIONAL EXACT-FORWARD BACK TO BUBBLE (UNMODIFIED ENVELOPE)
# -------------------------------------------------------------------
def forward_exact_envelope_to_bubble(envelope: dict):
    """
    Sends EXACTLY the same Pub/Sub JSON envelope Bubble receives today.
    No modifications. This preserves messageId, publishTime, data, etc.
    """
    headers = {
        "Authorization": f"Bearer {BUBBLE_API_KEY}",
        "Content-Type": "application/json"
    }

    resp = requests.post(
        BUBBLE_MESSAGE_ADDED_URL,
        json=envelope,    # ← EXACT PASS-THROUGH
        headers=headers,
        timeout=20
    )
    resp.raise_for_status()


# -------------------------------------------------------------------
# PUB/SUB → CLOUD RUN ENDPOINT
# -------------------------------------------------------------------
@app.post("/push")
async def pubsub_push(request: Request):
    envelope = await request.json()  # entire Pub/Sub push envelope

    if "message" not in envelope:
        return {"status": "ignored"}

    msg = envelope["message"]

    if "data" not in msg:
        return {"status": "missing-data"}

    # Decode base64 pubsub.data
    raw = base64.b64decode(msg["data"]).decode("utf-8")
    payload = json.loads(raw)

    email = payload.get("emailAddress")
    history_id = str(payload.get("historyId"))

    if not email or not history_id:
        return {"status": "missing-fields"}

    # Get Gmail access token from Bubble
    access_token, refresh_token = fetch_creds_from_bubble(email)
    if not access_token:
        return {"status": "no-gmail-creds"}

    # Gmail history filter (messageAdded only)
    history = gmail_history(email, access_token, history_id)

    # If Gmail says historyId is too old → you must re-watch
    if history is None:
        return {"status": "history-too-old"}

    # Extract message IDs
    new_messages = []
    for h in history.get("history", []):
        for added in h.get("messagesAdded", []):
            mid = added.get("message", {}).get("id")
            if mid:
                new_messages.append(mid)

    # Fan out each message to Bubble (recommended)
    for message_id in new_messages:
        enqueue_to_bubble({
            "emailAddress": email,
            "historyId": history_id,
            "messageId": message_id
        })

    # OPTIONAL:
    # If you want Bubble to receive EXACT Pub/Sub JSON as before, uncomment:
    # forward_exact_envelope_to_bubble(envelope)

    return {"status": "ok", "forwarded_count": len(new_messages)}


# HEALTH CHECK
@app.get("/health")
def health():
    return {"ok": True}
