import os
import requests
import time
import json
import redis
from datetime import datetime

def process_file(path, url, session) :
    try:
        with open(path, "r") as f :
            for line in f :
                try:
                    data = json.loads(line)
                    data['sent_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                    session.post(url, json=data)

                except Exception as e:
                    print(f"Error parsing line: {e}")

    except Exception as e:
        print(f"Could not read file {path}: {e}")


if __name__ == "__main__" :
    # ENV
    REDIS_HOST = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT = os.getenv("REDIS_PORT", "6379")
    API_PORT = os.getenv("API_PORT", "3000")
    ENDPOINT = f"{os.getenv('TARGET_API', 'http://localhost')}:{API_PORT}/user"

    # Connect to redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    
    with requests.Session() as session :
        session.headers.update({'Content-Type': 'application/json'})

        while True:
            
            if os.path.exists("/data/users/_SUCCESS"):
                 # Create file queue
                if r.setnx("lock", "1"):                    
                    files = sorted(os.listdir("/data/users"))

                    for file in files:
                        if file.startswith("part-"):
                            r.rpush("file_queue", os.path.join("/data/users", file))
                                        
                    os.remove("/data/users/_SUCCESS") # Ensure no more worker will try to lock again
                    r.delete("lock")

            file_bytes = r.lpop("file_queue")
            
            if file_bytes:
                f = file_bytes.decode('utf-8')
                print(f"Processing file: {f}...", flush=True)
                process_file(f, ENDPOINT, session)
                print(f"File {f} done.", flush=True)
            else:
                time.sleep(2)
