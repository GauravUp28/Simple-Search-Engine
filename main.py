from fastapi import FastAPI, Query
import httpx
from typing import List
from collections import defaultdict
import re
from contextlib import asynccontextmanager

DATA_STORE = []
INVERTED_INDEX = defaultdict(list)
BASE_URL = "https://november7-730026606190.europe-west1.run.app/messages/"

def tokenize(text: str) -> List[str]:
    return re.findall(r'\w+', text.lower())

async def ingest_data():
    """Fetches all pages of data and builds the search index."""
    global DATA_STORE, INVERTED_INDEX
    
    print("Status: Starting data ingestion...")
    
    DATA_STORE.clear()
    INVERTED_INDEX.clear()
        
    async with httpx.AsyncClient() as client:
        skip = 0
        limit = 100
        more_data_available = True
        
        while more_data_available:
            try:
                response = await client.get(
                    BASE_URL, 
                    params={"skip": skip, "limit": limit},
                    timeout=10.0
                )
                response.raise_for_status()
                data = response.json()
                
                new_items = data.get("items", [])
                
                if not new_items:
                    break
                
                start_index = len(DATA_STORE)
                DATA_STORE.extend(new_items)
                
                for relative_idx, record in enumerate(new_items):
                    absolute_idx = start_index + relative_idx
                    content = record.get("message", "")
                    tokens = set(tokenize(str(content)))
                    for token in tokens:
                        INVERTED_INDEX[token].append(absolute_idx)
                
                if len(new_items) < limit:
                    more_data_available = False
                else:
                    skip += limit
                    print(f"Fetched {len(DATA_STORE)} records so far...")
                    
            except Exception as e:
                print(f"Error during ingestion on skip={skip}: {e}")
                more_data_available = False

    print(f"Status: Ingestion complete. Indexed {len(DATA_STORE)} records total.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await ingest_data()
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/search")
async def search(
    q: str = Query(..., min_length=1),
    limit: int = 10,
    offset: int = 0
):
    search_tokens = tokenize(q)
    if not search_tokens:
        return {"count": 0, "results": []}
    
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
    
    total_matches = len(matched_indices)
    start = offset
    end = offset + limit
    sliced_indices = matched_indices[start:end]
    
    results = [DATA_STORE[i] for i in sliced_indices]
    
    return {
        "count": total_matches,
        "limit": limit,
        "offset": offset,
        "results": results
    }

@app.get("/health")
def health():
    return {"status": "ok", "records_indexed": len(DATA_STORE)}