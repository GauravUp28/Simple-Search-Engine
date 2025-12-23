from fastapi import FastAPI, Query
import httpx
from typing import List
from collections import defaultdict
import re

app = FastAPI()

# Global in-memory storage
DATA_STORE = []
INVERTED_INDEX = defaultdict(list)
BASE_URL = "https://november7-730026606190.europe-west1.run.app/messages/"

def tokenize(text: str) -> List[str]:
    """Helper to clean and split text into tokens."""
    return re.findall(r'\w+', text.lower())

async def ingest_data():
    """Fetches ALL pages of data and builds the search index."""
    global DATA_STORE, INVERTED_INDEX
    
    print("Status: Starting data ingestion...")
    DATA_STORE = [] # Clear existing data
    INVERTED_INDEX = defaultdict(list) # Clear existing index
    
    async with httpx.AsyncClient() as client:
        skip = 0
        limit = 100
        more_data_available = True
        
        while more_data_available:
            try:
                # Fetch page with pagination params
                response = await client.get(
                    BASE_URL, 
                    params={"skip": skip, "limit": limit},
                    timeout=10.0
                )
                response.raise_for_status()
                data = response.json()
                
                # Extract items using the correct key from your screenshot
                new_items = data.get("items", [])
                
                if not new_items:
                    break
                
                # Add to main storage
                start_index = len(DATA_STORE)
                DATA_STORE.extend(new_items)
                
                # Index these new items immediately
                for relative_idx, record in enumerate(new_items):
                    absolute_idx = start_index + relative_idx
                    content = record.get("message", "")
                    tokens = set(tokenize(str(content)))
                    for token in tokens:
                        INVERTED_INDEX[token].append(absolute_idx)
                
                # Check if we need to fetch more
                # If we got fewer items than the limit, we reached the end
                if len(new_items) < limit:
                    more_data_available = False
                else:
                    skip += limit
                    print(f"Fetched {len(DATA_STORE)} records so far...")
                    
            except Exception as e:
                print(f"Error during ingestion on skip={skip}: {e}")
                more_data_available = False

    print(f"Status: Ingestion complete. Indexed {len(DATA_STORE)} records total.")

@app.on_event("startup")
async def startup_event():
    await ingest_data()

@app.get("/search")
async def search(
    q: str = Query(..., min_length=1),
    limit: int = 10,
    offset: int = 0
):
    search_tokens = tokenize(q)
    if not search_tokens:
        return {"count": 0, "results": []}
    
    # Intersection logic (AND search)
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
    
    # Pagination for the SEARCH results
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