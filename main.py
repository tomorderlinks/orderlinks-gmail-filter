import base64
import json
import os
import requests
from fastapi import FastAPI, Request

from google.cloud import tasks_v2
from google.cloud.tasks_v2 import HttpMethod

app = FastAPI()

# --- Config via env vars ---
PROJECT_ID = os.getenv("PROJECT_ID")                       # e.g. total-pier-471215-b0
QUEUE_ID = os.getenv("QUEUE_ID", "gmail-to-bubble")        # create this queue below
QUEUE_LOCATION = os.getenv("QUEUE_LOCATION", "europe-west1")

BUBBLE_CREDS_URL = os.getenv("BUBBLE_CREDS_URL")           # https://app.orderlinks.co/api/1.1/wf/get-google-creds
BUBBLE_API_KEY = os.getenv("BUBBLE_API_KEY")               # store in Secret Manager
BUBBLE_MESSAGE_ADDED_URL = os.getenv("BUBBLE_MESSAGE_ADDED_URL")
# e.g. https://app.orderlinks.co/api/1.1/wf/gmail_message_added

# --------- Helpers ---------
def _auth_headers(access_token: str):
    return {"Authorization": f"Bearer {access_token}"}

def fetch_creds_from_bubble(email: str):
    headers = {"Authorization": f"Bearer {BUBBLE_API_KEY}", "Content-Type": "application/json"}
    resp = requests.post(BUBBLE_CREDS_URL, json={"emailAddress": email}, headers=headers, timeout=20)
    resp.raise_for_status()
    data = resp.json().get("response", {})
    return data.get("access_token"), data.get("refresh_token")

def gmail_list_history(email: str, access_token: str, start_history_id: str):
    url = f"https://gmail.googleapis.com/gmail/v1/users/{email}/history"
    params = {"startHistoryId": start_history_id, "historyTypes": "messageAdded"}
    r = requests.get(url, headers=_auth_headers(access_token), params=params, timeout=30)
    # If 404, startHistoryId is too old (you must re-watch)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()

def enqueue_to_bubble(payload: dict):
    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(PROJECT_ID, QUEUE_LOCATION, QUEUE_ID)
    task = {
        "http_request": {
            "http_method": HttpMethod.POST,
            "url": BUBBLE_MESSAGE_ADDED_URL,
            "headers": {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {BUBBLE_API_KEY}",
            },
            "body": json.dumps(payload).encode(),
        }
    }
    client.create_task(parent=parent, task=task)

# --------- Pub/Sub push endpoint ---------
@app.post("/push")
async def pubsub_push(request: Request):
    envelope = await request.json()
    if "message" not in envelope:
        return {"status": "ignored"}

    # Pub/Sub message decoding
    data_b64 = envelope["message"].get("data")
    if not data_b64:
        return {"status": "no-data"}

    notif = json.loads(base64.b64decode(data_b64).decode("utf-8"))
    email = notif.get("emailAddress")
    history_id = str(notif.get("historyId"))

    if not email or not history_id:
        return {"status": "missing-fields"}

    # Get Gmail tokens from Bubble
    access_token, _ = fetch_creds_from_bubble(email)
    if not access_token:
        return {"status": "no-creds"}

    # Get only messagesAdded since startHistoryId
    history = gmail_list_history(email, access_token, history_id)
    if history is None:
        # history too old -> trigger re-watch logic in your system
        return {"status": "history-too-old"}

    new_message_ids = []
    for h in history.get("history", []):
        for added in h.get("messagesAdded", []):
            mid = added.get("message", {}).get("id")
            if mid:
                new_message_ids.append(mid)

    # Fan-out to Bubble via Cloud Tasks (one task per message)
    for mid in new_message_ids:
        enqueue_to_bubble({
            "emailAddress": email,
            "historyId": history_id,
            "messageId": mid
        })

    return {"status": "ok", "count": len(new_message_ids)}

@app.get("/health")
def health():
    return {"ok": True}
