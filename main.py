from fastapi import FastAPI, Query
import httpx
import asyncio
import logging
from typing import List
from collections import defaultdict
import re
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_STORE = []
INVERTED_INDEX = defaultdict(list)
BASE_URL = "https://november7-730026606190.europe-west1.run.app/messages/"
CONCURRENCY_LIMIT = 5  # Number of parallel requests

def tokenize(text: str) -> List[str]:
    return re.findall(r'\w+', text.lower())

async def fetch_batch_smart(client, skip, limit):
    """
    Same robust function: Retries 5xx/429, splits batch on 4xx.
    """
    try:
        response = await client.get(
            BASE_URL, 
            params={"skip": skip, "limit": limit},
            timeout=20.0 # Increased timeout slightly for safety
        )
        
        if response.status_code == 200:
            return response.json().get("items", [])
            
        if response.status_code == 429 or response.status_code >= 500:
            logger.warning(f"Transient error {response.status_code} at skip={skip}. Retrying...")
            await asyncio.sleep(2)
            return await fetch_batch_smart(client, skip, limit)
            
    except (httpx.RequestError, httpx.TimeoutException) as e:
        logger.warning(f"Network error at skip={skip}: {e}. Retrying...")
        await asyncio.sleep(1)
        return await fetch_batch_smart(client, skip, limit)

    # Granular Fallback for hard errors
    if limit > 1:
        mid = limit // 2
        left = await fetch_batch_smart(client, skip, mid)
        right = await fetch_batch_smart(client, skip + mid, limit - mid)
        return left + right

    logger.error(f"Permanently skipping corrupt record at skip={skip}")
    return []

async def fetch_all_data():
    all_items = []
    skip = 0
    limit = 100
    
    # We reuse a single client for connection pooling efficiency
    async with httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=20)) as client:
        more_data = True
        
        while more_data:
            tasks = []
            # Create a batch of 5 parallel tasks
            for i in range(CONCURRENCY_LIMIT):
                current_skip = skip + (i * limit)
                tasks.append(fetch_batch_smart(client, current_skip, limit))
            
            logger.info(f"Fetching parallel batch starting at skip={skip}...")
            
            # Run them all at once
            results = await asyncio.gather(*tasks)
            
            # Process results in order to keep data sorted
            for batch_items in results:
                if not batch_items:
                    more_data = False # Stop if we hit an empty page
                
                all_items.extend(batch_items)
                
                # If a page isn't full, we reached the end
                if len(batch_items) < limit:
                    more_data = False
            
            if not more_data:
                break
                
            # Move the skip pointer forward by the total amount we just fetched
            skip += (limit * CONCURRENCY_LIMIT)

    return all_items

async def ingest_data():
    logger.info("Starting High-Speed Ingestion...")
    items = await fetch_all_data()
    
    if items:
        DATA_STORE.clear()
        INVERTED_INDEX.clear()
        DATA_STORE.extend(items)
        
        # Indexing is fast in memory, no need to optimize this part usually
        for idx, record in enumerate(DATA_STORE):
            content = record.get("message", "")
            tokens = set(tokenize(str(content)))
            for token in tokens:
                INVERTED_INDEX[token].append(idx)
        logger.info(f"Ingestion Complete. Total records: {len(DATA_STORE)}")
    else:
        logger.warning("No items fetched.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await ingest_data()
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/search")
async def search(q: str = Query(..., min_length=1), limit: int = 10, offset: int = 0):
    search_tokens = tokenize(q)
    if not search_tokens: return {"count": 0, "results": []}
    
    if search_tokens[0] in INVERTED_INDEX:
        result_indices = set(INVERTED_INDEX[search_tokens[0]])
    else:
        return {"count": 0, "results": []}
    
    for token in search_tokens[1:]:
        if token in INVERTED_INDEX:
            result_indices.intersection_update(INVERTED_INDEX[token])
        else:
            return {"count": 0, "results": []}
            
    matched_indices = list(result_indices)
    results = [DATA_STORE[i] for i in matched_indices[offset:offset+limit]]
    return {"count": len(matched_indices), "results": results}

@app.get("/health")
def health():
    return {"status": "ok", "records_indexed": len(DATA_STORE)}