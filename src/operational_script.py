import json
import logging
from typing import Dict
import requests
from typing import Iterator, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
API_ENDPOINT = "https://api.streampro.dummy/v1/users/last-watched"
EVENTS_FILE_PATH = "data/events.json"

def _event_generator(file_path: str) -> Iterator[Tuple[str, str, str]]:
    """
    Generator that yields (user_id, video_id, timestamp_str) from valid events.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue

                try:
                    event = json.loads(line)
                    
                    event_name = event.get('event_name')
                    if event_name not in ('watch_time', 'like', 'heart', 'session_start'):
                        continue
                        
                    user_id = event.get('user_id')
                    video_id = event.get('video_id')
                    timestamp_str = event.get('timestamp')

                    if user_id and video_id and timestamp_str:
                        yield user_id, video_id, timestamp_str
                            
                except json.JSONDecodeError:
                    logging.warning(f"Error parsing JSON line: {line.strip()[:50]}...")
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}. Ensure it is present in the working directory.")

def get_last_watched_videos(file_path: str) -> Dict[str, str]:
    """
    Consumes the event generator to find the most recently watched video for each user.
    """
    user_latest_video = {}
    user_latest_timestamp = {}

    for user_id, video_id, timestamp_str in _event_generator(file_path):
        if user_id not in user_latest_timestamp or timestamp_str > user_latest_timestamp[user_id]:
            user_latest_timestamp[user_id] = timestamp_str
            user_latest_video[user_id] = video_id

    return user_latest_video

def send_last_watched_update(user_id: str, video_id: str):
    """
    Sends an HTTP POST request to update the user's last watched video.
    """
    payload = {
        "user_id": user_id,
        "last_watched_video_id": video_id
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer mockup_api_token_123"
    }
    
    try:
        # Mocking the request execution since there is no real endpoint
        logging.info(f"Mocking POST to {API_ENDPOINT} for User: {user_id}, Video: {video_id}")
        # response = requests.post(API_ENDPOINT, json=payload, headers=headers, timeout=5)
        # response.raise_for_status()
        
        # Simulate a successful response
        logging.info(f"Successfully updated last watched video for user {user_id}")
        return True
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to update user {user_id}: {e}")
        return False

def main():
    logging.info(f"Reading events from {EVENTS_FILE_PATH}...")
    
    last_watched_map = get_last_watched_videos(EVENTS_FILE_PATH)
    print(last_watched_map)
    
    if not last_watched_map:
        logging.warning("No valid video watch events found.")
        return

    logging.info(f"Found latest videos for {len(last_watched_map)} users. Syncing to REST API...")
    
    success_count = 0
    failure_count = 0
    
    for user_id, video_id in last_watched_map.items():
        success = send_last_watched_update(user_id, video_id)
        if success:
            success_count += 1
        else:
            failure_count += 1
            
    logging.info("--- Sync Summary ---")
    logging.info(f"Users Processed: {len(last_watched_map)}")
    logging.info(f"Success: {success_count}")
    logging.info(f"Failures: {failure_count}")

if __name__ == "__main__":
    main()
