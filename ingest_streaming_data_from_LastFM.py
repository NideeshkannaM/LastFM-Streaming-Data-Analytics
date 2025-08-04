import requests
import json
import time
from azure.eventhub import EventHubProducerClient, EventData

API_KEY = 'b12d34f6a89ab1234cd56ef78a90bcde'
USERNAME = 'nideesh123'
EVENT_HUB_CONN_STR = (
    'Endpoint=sb://streamingdatanid.servicebus.windows.net/;'
    'SharedAccessKeyName=NideeshSharedAccessKey;'
    'SharedAccessKey=lY8xy5ZabcdEFGHijklmnOpQRStUVWxYZaBcDeFgHI='
)

EVENT_HUB_NAME = 'lastfm-stream' 
FETCH_INTERVAL_SEC = 30
# -------------------------------

producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENT_HUB_CONN_STR,
    eventhub_name=EVENT_HUB_NAME
)

def fetch_recent_tracks():
    url = f"https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user={USERNAME}&api_key={API_KEY}&format=json&limit=5"
    try:
        response = requests.get(url)
        data = response.json()
        tracks = data.get("recenttracks", {}).get("track", [])
        events = []
        for track in tracks:
            if 'date' not in track:
                continue
            event = {
                "user": USERNAME,
                "artist": track.get("artist", {}).get("#text", ""),
                "track": track.get("name", ""),
                "album": track.get("album", {}).get("#text", ""),
                "timestamp": track["date"]["uts"],
                "datetime": track["date"]["#text"]
            }
            events.append(event)
        return events
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

def send_to_eventhub(events):
    if not events:
        return
    try:
        batch = producer.create_batch()
        for event in events:
            batch.add(EventData(json.dumps(event)))
        producer.send_batch(batch)
        print(f"Sent {len(events)} events to Event Hub.")
    except Exception as e:
        print(f"Error sending to Event Hub: {e}")

if __name__ == "__main__":
    while True:
        events = fetch_recent_tracks()
        send_to_eventhub(events)
        time.sleep(FETCH_INTERVAL_SEC)
