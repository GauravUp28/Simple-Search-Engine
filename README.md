# Simple Search Engine

A high-performance, in-memory search engine built with Python (FastAPI). This service ingests message data from an external source and exposes a fast search endpoint.

**[🔴 Live Demo](https://simple-search-engine-pwy3.onrender.com/search?q=reservation)**

## Goal

The goal of this project is to build a simple search engine on top of a provided data source. The system exposes a public API endpoint that users can query to receive a paginated list of matching records, with response times under 100ms.

## Data Source

This service ingests data from the following API:
👉 **[Swagger Documentation](https://november7-730026606190.europe-west1.run.app/docs#/default/get_messages_messages__get)**

## Features

-   **Fast Search:** Uses an in-memory inverted index to guarantee sub-100ms response times.
-   **Pagination:** Supports `limit` and `offset` for browsing results.
-   **Auto-Ingestion:** Automatically fetches and indexes data on server startup.

## Setup & Usage

1.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Run the Server:**
    ```bash
    uvicorn main:app --reload
    ```

3.  **Search Endpoint:**
    `GET /search?q=query&limit=10&offset=0`

## Bonus 1: Design Notes

### Alternative Approaches Considered

1.  **Naive Linear Scan:**
    * *Approach:* Iterate through the entire list of messages for every search request.
    * *Why rejected:* It has $O(N)$ time complexity. As the dataset grows, latency would increase linearly, risking the 100ms SLA.

2.  **External Search Engine (Elasticsearch/Algolia):**
    * *Approach:* Push data to a dedicated search service.
    * *Why rejected:* Adds significant infrastructure complexity and network latency overhead. For a dataset that fits in memory, a local index is faster and simpler.

3.  **In-Memory Inverted Index (Chosen Approach):**
    * *Approach:* Pre-compute a map of `token -> list of document IDs`.
    * *Why chosen:* Provides $O(1)$ lookup time. This is the optimal strategy for read-heavy, low-latency requirements on moderate datasets.

## Bonus 2: Data Insights

To further optimize latency from ~50-80ms down to <30ms, we could implement the following:

1.  **Edge Deployment:**
    Deploy the API on Edge networks (e.g., Cloudflare Workers or AWS Lambda @ Edge) to physically move the compute closer to the user, reducing network round-trip time (RTT).

2.  **Protocol Buffers (gRPC):**
    Switch from JSON to Protobuf or MsgPack. JSON serialization/deserialization is CPU-intensive in Python; binary formats are significantly faster to parse.

3.  **Compiled Extensions:**
    Rewrite the core intersection logic in Rust (via PyO3) or Go. Python's set operations are fast, but a compiled language avoids the interpreter overhead entirely.

4.  **Aggressive Caching:**
    Implement strict `Cache-Control` headers for the search endpoint. Identical queries should be served directly by the CDN or browser cache, resulting in effectively 0ms server latency.