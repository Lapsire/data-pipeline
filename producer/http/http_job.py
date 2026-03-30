import os
import json
import redis
import asyncio
import aiohttp
from datetime import datetime

async def send_batch(session, url, batch, semaphore):
    async with semaphore:
        try:
            async with session.post(url, json=batch) as response:
                if response.status != 200:
                    print(f"HTTP Error: {response.status}")
        except Exception as e:
            print(f"Request failed: {e}")

async def process_file(path, url, concurrency, batch_size):
    semaphore = asyncio.Semaphore(concurrency)
    tasks = []
    
    # HTTP async client
    async with aiohttp.ClientSession(headers={'Content-Type': 'application/json'}) as session:
        batch = []
        try:
            with open(path, "r") as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        data['sent_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                        batch.append(data)
                        
                        # If batch is full, we send
                        if len(batch) >= batch_size:
                            tasks.append(asyncio.create_task(send_batch(session, url, batch.copy(), semaphore)))
                            batch.clear()
                            
                    except Exception as e:
                        print(f"Parsing error: {e}")
                        
                # Send the lines that have not been sent
                if batch:
                    tasks.append(asyncio.create_task(send_batch(session, url, batch, semaphore)))
            
            if tasks:
                await asyncio.gather(*tasks)
                
        except Exception as e:
            print(f"File not readable {path}: {e}")

async def main():
    # ENV
    REDIS_HOST = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT = os.getenv("REDIS_PORT", "6379")
    API_PORT = os.getenv("API_PORT", "3000")
    ENDPOINT = f"{os.getenv('TARGET_API', 'http://localhost')}:{API_PORT}/users/batch" ## Here we call batch
    CONCURRENCY = int(os.getenv("CONCURRENCY", 10))
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", 500))

    # Connect to redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    
    while True:
        if os.path.exists("/data/users/_SUCCESS"):
            if r.setnx("lock", "1"):                    
                files = sorted(os.listdir("/data/users"))
                for file in files:
                    if file.startswith("part-"):
                        r.rpush("file_queue", os.path.join("/data/users", file))
                                    
                os.remove("/data/users/_SUCCESS")
                r.delete("lock")

        file_bytes = r.lpop("file_queue")
        
        if file_bytes:
            f = file_bytes.decode('utf-8')
            print(f"Processing file: {f}...", flush=True)
            
            # Async process
            await process_file(f, ENDPOINT, CONCURRENCY, BATCH_SIZE)
            
            print(f"File {f} processed.", flush=True)
        else:
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main())
