import os
import requests
import time
import json
from datetime import datetime

def keep_worker_files(worker_id, total_workers, path) -> list :
    if not os.path.exists(path) :
        raise FileNotFoundError(f"Path '{path}' does not exist.")
    if not os.path.isdir(path):
            raise NotADirectoryError(f"Path '{path}' is not a directory.")
    
    files = sorted(os.listdir(path))
    keep = []

    for file in files :
        file = file.split('-') # Explode filename to extract part and id
        if file[0] == "part":
            if int(file[1]) % total_workers == worker_id : # Check if the worker will process the file
                keep.append(os.path.join(path, "-".join(file))) # Rebuild file name
    
    return keep

def process_file(path, url, session) :
    with open(path, "r") as f :
        for line in f :
            data = json.loads(line)
            data['sent_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            session.post(url, json=data)

if __name__ == "__main__" :
    # Waiting spark to generate data
    while 1 :
        if os.path.exists("/data/users/_SUCCESS") : break
        time.sleep(2)

    # ENV
    WORKER_ID = int(os.getenv("WORKER_ID",0))
    TOTAL_WORKERS = int(os.getenv("TOTAL_WORKERS", 1))
    API_PORT = os.getenv("API_PORT", "3000")
    ENDPOINT = f"{os.getenv('TARGET_API', 'http://localhost')}:{API_PORT}/user"
    
    with requests.Session() as session :
        session.headers.update({'Content-Type': 'application/json'})

        files_to_process = keep_worker_files(WORKER_ID, TOTAL_WORKERS, "/data/users")

        for f in files_to_process :
            print(f"Processing file: {f}...", flush=True)
            process_file(f, ENDPOINT, session)
            print(f"File {f} processed.", flush=True)

    print("Work done !", flush=True)
