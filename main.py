import base64
import json
import os
import requests
from fastapi import FastAPI, Request

app = FastAPI()

# Environment variables
BUBBLE_CREDS_URL = os.getenv("BUBBLE_CREDS_URL")
BUBBLE_MESSAGE_ADDED_URL = os.getenv("BUBBLE_MESSAGE_ADDED_URL")
BUBBLE_API_KEY = os.getenv("BUBBLE_API_KEY")

def fetch_creds_from_bubble(email: str):
    """Get Gmail tokens from Bubble."""
    headers = {
        "Authorization": f"Bearer {BUBBLE_API_KEY}",
        "Content-Type": "application/json"
    }
    
    try:
        resp = requests.post(
            BUBBLE_CREDS_URL,
            json={"emailAddress": email},
            headers=headers,
            timeout=20
        )
        resp.raise_for_status()
        data = resp.json().get("response", {})
        return data.get("access_token"), data.get("refresh_token")
    except Exception as e:
        print(f"Error fetching creds: {e}")
        return None, None

def gmail_history(email: str, access_token: str, start_history_id: str):
    """Fetch Gmail history for messageAdded only."""
    url = f"https://gmail.googleapis.com/gmail/v1/users/{email}/history"
    params = {
        "startHistoryId": start_history_id,
        "historyTypes": "messageAdded"
    }
    
    try:
        r = requests.get(
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            params=params,
            timeout=30
        )
        
        if r.status_code == 404:
            return None
        
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Error fetching history: {e}")
        return None

def forward_to_bubble(payload: dict):
    """Send message to Bubble."""
    headers = {
        "Authorization": f"Bearer {BUBBLE_API_KEY}",
        "Content-Type": "application/json"
    }
    
    try:
        resp = requests.post(
            BUBBLE_MESSAGE_ADDED_URL,
            json=payload,
            headers=headers,
            timeout=20
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"Error forwarding to Bubble: {e}")
        return False

@app.post("/push")
async def pubsub_push(request: Request):
    try:
        envelope = await request.json()
        print(f"Received: {json.dumps(envelope)}")
        
        if "message" not in envelope:
            print("No message in envelope")
            return {"status": "ignored"}
        
        msg = envelope["message"]
        
        if "data" not in msg:
            print("No data in message")
            return {"status": "missing-data"}
        
        # Decode Pub/Sub data
        raw = base64.b64decode(msg["data"]).decode("utf-8")
        payload = json.loads(raw)
        print(f"Decoded payload: {payload}")
        
        email = payload.get("emailAddress")
        history_id = str(payload.get("historyId"))
        
        if not email or not history_id:
            print("Missing email or historyId")
            return {"status": "missing-fields"}
        
        # Get Gmail credentials from Bubble
        access_token, refresh_token = fetch_creds_from_bubble(email)
        
        if not access_token:
            print(f"No credentials for {email}")
            return {"status": "no-gmail-creds"}
        
        # Check Gmail history
        history = gmail_history(email, access_token, history_id)
        
        if history is None:
            print("History too old or error")
            return {"status": "history-too-old"}
        
        # Extract new message IDs
        new_messages = []
        for h in history.get("history", []):
            for added in h.get("messagesAdded", []):
                mid = added.get("message", {}).get("id")
                if mid:
                    new_messages.append(mid)
        
        print(f"Found {len(new_messages)} new messages")
        
        # Forward each message to Bubble
        forwarded = 0
        for message_id in new_messages:
            success = forward_to_bubble({
                "emailAddress": email,
                "historyId": history_id,
                "messageId": message_id
            })
            if success:
                forwarded += 1
        
        return {"status": "ok", "forwarded_count": forwarded}
    
    except Exception as e:
        print(f"Error in /push: {e}")
        return {"status": "error", "detail": str(e)}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/")
def root():
    return {"status": "healthy", "service": "gmail-filter"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
