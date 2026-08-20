# AMT Memory REST API - endpoint examples (for gateway end-to-end testing)

Five endpoints. The examples are copy-paste `curl`. The **only** things that
change between calling the service directly vs through the InferencePlatform
gateway are `BASE_URL` and the two auth headers, so every call below is written
against `$BASE_URL`.

## Live test endpoint (temporary)

A public test instance is deployed and validated end to end (ingest -> durable
extraction -> recall). Auth is **off** on this instance so you can test without
keys. It talks to the real Cosmos + Foundry, so facts/episodes are really
produced (asynchronously, ~30-90s after ingest).

```bash
export BASE_URL="https://amt-test-app.gentlemoss-fc34be50.westus3.azurecontainerapps.io"
```

This URL is temporary (public, unauthenticated, for integration testing). The
production instance will be internal + Private Link behind the gateway, at which
point `BASE_URL` becomes `https://<gateway-host>/memory` and the two auth headers
below apply.

### What it is wired to

Everything runs in subscription **CosmosDB AI Testing**, resource group
**shahra-rg**:

| Component                       | Value                                                                                          |
|---------------------------------|------------------------------------------------------------------------------------------------|
| Public endpoint (Container App) | `amt-test-app` -> `https://amt-test-app.gentlemoss-fc34be50.westus3.azurecontainerapps.io`     |
| Cosmos account                  | `memory-benchmark-2` (West US 2)                                                               |
| Cosmos database                 | `ai_memory`                                                                                    |
| Function App                    | `func-75gifj5ewm3ec`                                                                           |
| Containers                      | `amt-rest-facts`, `amt-rest-turns`, `amt-rest-summaries`, `amt-rest-counter`, `amt-rest-lease` |
| AI Foundry                      | `ragchat-oai` - chat `gpt-5.4`, embeddings `text-embedding-3-large` (1536 dims)                |
| Processing                      | Durable Function App `func-75gifj5ewm3ec` (`MEMORY_PROCESSOR_OWNER=durable`)                   |
| Auth                            | none on this instance (open for testing)                                                       |

### How processing works (why search is not instant)

The endpoint only **writes the turn** and returns `201` immediately. A separate
Azure Durable Function App reads new turns off the Cosmos change feed and runs
extraction in the background:

- **Facts** are extracted every couple of turns.
- **Episodes** are closed when a conversation boundary forms (a >30 min gap
  between turns via `created_at`, or a long segment).
- **Reconcile** runs on cadence, and can also be triggered inline (endpoint 4).

So after ingesting, wait ~30-90s (occasionally a bit longer on a cold start)
before facts/episodes appear in search. Ingest is immediate; extraction is
asynchronous.

## Setup

```bash
export BASE_URL="https://amt-test-app.gentlemoss-fc34be50.westus3.azurecontainerapps.io"
```

Notes:

- `user_id` and `thread_id` are **path** segments. `thread_id` is an arbitrary
  string (not a GUID).
- Facts, episodes, and summaries are **derived automatically** on cadence from
  the turns you ingest - you do not call an API to create them. They appear in
  search once the background pipeline has processed enough turns.
- Processing is asynchronous when the durable Function App owns it, so after
  ingesting, allow time (or poll) before facts/episodes show up in search.

---

## 1. Ingest a turn (create memory)

`POST /users/{user_id}/threads/{thread_id}/memory`

```bash
curl -sS -X POST "$BASE_URL/users/alice/threads/t1/memory" \
  -H "Content-Type: application/json" \
  -d '{
    "role": "user",
    "content": "I am planning a trip to Lisbon in October and I prefer window seats.",
    "created_at": "2026-08-18T09:00:00Z"
  }'
```

Body fields: `role` (required), `content` (required), and optional `tags`
(string array), `metadata` (object), `salience` (number), `created_at`
(ISO-8601; set it to control episode time-gaps).

Response `201 Created`:

```json
{
  "id": "3456fa8f-351e-49c2-be1b-c269e8251d38"
}
```

---

## 2. Search memories (facts + episodes)

`POST /users/{user_id}/search`

```bash
curl -sS -X POST "$BASE_URL/users/alice/search" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What are the user travel preferences?",
    "top_k": 5
  }'
```

Only `query` is required. Optional: `top_k` (1-50, default 5),
`include_episodes` (default `true`), `memory_types`, `min_confidence`,
`min_salience`, `tags_any`, `tags_all`, `exclude_tags`, `created_after`,
`created_before`.

Response `200 OK`:

```json
{
  "items": [
    {
      "id": "f1a2...",
      "memory_type": "fact",
      "content": "User prefers window seats.",
      "tags": [
        "sys:agent-fact"
      ],
      "confidence": 0.9,
      "salience": 0.6,
      "created_at": "2026-08-18T09:00:05Z",
      "thread_id": "t1",
      "score": 0.83
    }
  ],
  "count": 1,
  "truncated": false
}
```

---

## 3. Search episodes only

`POST /users/{user_id}/search/episodes`

```bash
curl -sS -X POST "$BASE_URL/users/alice/search/episodes" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "trip planning",
    "top_k": 5
  }'
```

Required: `query`. Optional: `top_k` (default 5), `min_salience`.

Response `200 OK` (same envelope; `memory_type` is `episodic`):

```json
{
  "items": [
    {
      "id": "e1b2...",
      "memory_type": "episodic",
      "content": "Planned a Lisbon trip: October arrival, window seat, hotel near Alfama under $1200.",
      "created_at": "2026-08-18T09:05:00Z",
      "thread_id": "t1",
      "score": 0.71
    }
  ],
  "count": 1,
  "truncated": false
}
```

---

## 4. Reconcile memories

`POST /users/{user_id}/reconcile`

```bash
curl -sS -X POST "$BASE_URL/users/alice/reconcile" \
  -H "Content-Type: application/json" \
  -d '{}'
```

Body optional: `{ "n": 50 }` to override how many recent facts are considered
(defaults to the configured dedup pool size). Reconciliation also runs
automatically on cadence; this is an explicit inline run.

Response `200 OK`:

```json
{
  "kept": 7,
  "merged": 0,
  "contradicted": 0
}
```

---

## 5. Health

`GET /health`

```bash
curl -sS "$BASE_URL/health"       # direct mode
# Behind the gateway /health is not routed publicly (it is the origin probe
# path); use it directly against the service host, not via /memory.
```

Response `200 OK`:

```json
{
  "status": "ok",
  "processor_owner": "durable"
}
```

---

## Full end-to-end sequence (ingest -> derive -> recall)

Posts two short sessions ~45 min apart (via `created_at`) so an episode
boundary forms, then recalls facts + episodes. Add the two auth headers behind
the gateway.

```bash
BASE_URL="${BASE_URL:?set BASE_URL first}"
U=alice ; T=t1

post() { curl -sS -X POST "$BASE_URL/users/$U/threads/$T/memory" \
  -H "Content-Type: application/json" -d "$1" ; echo ; }

# Session A (planning)
post '{"role":"user","content":"Plan my Lisbon trip, arriving October 3rd.","created_at":"2026-08-16T09:00:00Z"}'
post '{"role":"agent","content":"Booked Oct 3 into Lisbon; window seat held on TAP.","created_at":"2026-08-16T09:01:00Z"}'
post '{"role":"user","content":"Find a hotel near Alfama under $1200.","created_at":"2026-08-16T09:02:00Z"}'
post '{"role":"agent","content":"Reserved Memmo Alfama within your $1200 budget.","created_at":"2026-08-16T09:03:00Z"}'

# Session B (different topic ~45 min later -> closes session A into an episode)
post '{"role":"user","content":"Different topic - expense last week client dinner.","created_at":"2026-08-16T09:48:00Z"}'
post '{"role":"agent","content":"Sure, what was the total and which client?","created_at":"2026-08-16T09:49:00Z"}'
post '{"role":"user","content":"$180 for the Contoso account on my corporate card.","created_at":"2026-08-16T09:50:00Z"}'
post '{"role":"agent","content":"Logged $180 to Contoso against your corporate card.","created_at":"2026-08-16T09:51:00Z"}'

# Give the background pipeline time to extract (longer for the durable/cloud path)
sleep 45

# Recall
curl -sS -X POST "$BASE_URL/users/$U/search" \
  -H "Content-Type: application/json" \
  -d '{"query":"travel preferences and budget","top_k":10}'
echo
curl -sS -X POST "$BASE_URL/users/$U/search/episodes" \
  -H "Content-Type: application/json" -d '{"query":"trip planning","top_k":5}'
echo
curl -sS -X POST "$BASE_URL/users/$U/reconcile" \
  -H "Content-Type: application/json" -d '{}'
echo
```

## Error responses

Errors are `application/problem+json`:

```json
{
  "type": "about:blank",
  "title": "Validation Error",
  "status": 422,
  "detail": "..."
}
```

Common: `422` invalid body (missing `content`, `top_k` out of 1-50), `401`/`403`
auth (behind the gateway), `503` when the backing store is unavailable.