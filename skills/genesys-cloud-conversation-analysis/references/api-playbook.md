# API playbook

Endpoints, job lifecycle, paging, rate limits, retry rules, and permissions for pulling
conversation detail data out of Genesys Cloud. All paths are relative to the regional API
host (for example `https://api.usw2.pure.cloud`); the login host is the matching
`https://login.<region>` host.

## Contents

- Authentication
- The async detail job, step by step
- Query body shapes
- Paging rules
- Rate limits and retry
- Name resolution endpoints
- Enrichment endpoints (recordings, transcripts, evaluations)
- Permissions checklist

## Authentication

Prefer Authorization Code with PKCE for anything a person runs: the token belongs to the
signed-in user, carries their permissions and divisions, and no client secret is handled.
Use client credentials only for unattended automation, and scope that OAuth client to the
analytics and routing read permissions it needs.

Send `Authorization: Bearer <token>` on every request. Tokens expire; on HTTP 401 refresh or
re-authenticate once, then fail loudly rather than looping.

## The async detail job, step by step

Synchronous `POST /api/v2/analytics/conversations/details/query` caps the interval at 7
days and pages of 100, and it is rate-limited harder. Use it only for a quick look at a
few records. Everything else goes through the job:

| Step | Call | Notes |
| --- | --- | --- |
| Submit | `POST /api/v2/analytics/conversations/details/jobs` | Body below. Returns `{ "jobId": "..." }`. |
| Poll | `GET /api/v2/analytics/conversations/details/jobs/{jobId}` | `state` is `QUEUED`, `PENDING`, `FULFILLED`, `FAILED`, `CANCELLED`, or `EXPIRED`. |
| Page | `GET /api/v2/analytics/conversations/details/jobs/{jobId}/results?pageSize=1000&cursor=...` | First call without `cursor`. Repeat while the response has a `cursor`. |
| Clean up | `DELETE /api/v2/analytics/conversations/details/jobs/{jobId}` | Frees the org's concurrent-job slot. Do it even after failure. |

Polling guardrails that have proven necessary: poll every 3 seconds, stop after 30
minutes or roughly 600 polls, and stop after 5 consecutive poll errors. A job that sits in
`QUEUED` for a long time is usually the org's concurrent job cap, not a problem with your
query; look for and delete stale jobs.

A fulfilled job may report a `dataAvailabilityDate`. If it is earlier than your interval
end, the job did not cover the whole window; say so in the report.

## Query body shapes

Minimal job body:

```json
{
  "interval": "2026-09-01T04:00:00.000Z/2026-09-02T04:00:00.000Z",
  "order": "asc",
  "orderBy": "conversationStart"
}
```

Filters attach at two levels. Getting the level wrong yields an empty result, not an error.

- `conversationFilters` match conversation-level dimensions: `originatingDirection`,
  `divisionId`, `conversationId`, `externalTag`, `conversationInitiator`.
- `segmentFilters` match session and segment dimensions: `mediaType`, `queueId`,
  `userId`, `wrapUpCode`, `purpose`, `direction`, `ani`, `dnis`, `segmentType`,
  `disconnectType`, `flowId`, `skillId`, `languageId`, `requestedRoutingSkillId`.

Each filter is `{ "type": "and" | "or", "predicates": [ { "dimension": "...", "value": "..." } ] }`.
Predicates can also use `"operator": "matches"` with `"value"` or `"exists"` with no
value. Filter on IDs, never on names.

Example: inbound voice conversations that touched one queue.

```json
{
  "interval": "2026-09-01T04:00:00.000Z/2026-09-08T04:00:00.000Z",
  "order": "asc",
  "orderBy": "conversationStart",
  "conversationFilters": [
    { "type": "and", "predicates": [ { "dimension": "originatingDirection", "value": "inbound" } ] }
  ],
  "segmentFilters": [
    { "type": "and", "predicates": [ { "dimension": "mediaType", "value": "voice" } ] },
    { "type": "and", "predicates": [ { "dimension": "queueId", "value": "<queue guid>" } ] }
  ]
}
```

Intervals are UTC. Build them from the local business window and state the timezone in
the report. Keep one job to at most a calendar month; split longer ranges into monthly
jobs and merge the JSONL files.

## Paging rules

- Use `pageSize=1000` on job results; smaller pages just cost more calls.
- Keep a set of cursors already used. If the API returns one you have seen, stop; you are
  in a loop, not fetching more data.
- Cap total pages (500 pages is half a million conversations) so a runaway loop cannot
  exhaust memory or the rate budget.
- Append each page to a local JSONL file before requesting the next one. Every later
  stage reads that file.
- Never fetch pages of the same job in parallel. Cursors are sequential.

## Rate limits and retry

Genesys enforces per-token and per-org limits and answers with HTTP 429 and a
`Retry-After` header (seconds, occasionally an HTTP date). Analytics endpoints have lower
ceilings than CRUD endpoints, and job results count against them.

Retry policy that behaves well against the platform:

- Retry on 429, 500, 502, 503, 504. Do not retry 400 (fix the query), 401 (re-auth once),
  403 (permission), or 404.
- On 429, sleep for `Retry-After` when present, otherwise use the backoff below.
- Backoff: start at 1 second, double each attempt, cap at 30 seconds, add up to 0.25
  seconds of jitter, and give up after 5 attempts with the last error surfaced.
- Log every retry with status, attempt number, and delay. Silent retries hide a limit you
  are about to hit harder.

Habits that avoid 429 in the first place:

- One job at a time per token; serialize page fetches.
- Resolve names from local caches; a lookup per row is the classic 429 generator.
- Batch user lookups (50 IDs per call) instead of `GET /api/v2/users/{id}` per agent.
- Do not poll faster than every 3 seconds.

## Name resolution endpoints

| Kind | Endpoint | Paging | Notes |
| --- | --- | --- | --- |
| Queues | `GET /api/v2/routing/queues` | `pageSize=100&pageNumber=n` until `pageNumber >= pageCount` | `entities[].id`, `.name`. Use `?name=` to find an ID for a filter. |
| Wrap-up codes | `GET /api/v2/routing/wrapupcodes` | same | Code IDs appear on `wrapup` segments as `wrapUpCode`. |
| Divisions | `GET /api/v2/authorization/divisions` | same | Conversation-level `divisionIds`. |
| Skills | `GET /api/v2/routing/skills` | same | `requestedRoutingSkillIds` on sessions. |
| Languages | `GET /api/v2/routing/languages` | same | `requestedLanguageId` on sessions. |
| Users | `GET /api/v2/users?id=a,b,c&state=any&pageSize=N` | batch of up to 50 IDs per call, one request per batch | Only IDs present in your data. `state=any` includes inactive and deleted users. `entities[].name`. |
| Flows | inline in the record | none | `sessions[].flow.flowName` and `.flowId`. `GET /api/v2/flows?id=` only if a name is blank. |
| Campaigns | `GET /api/v2/outbound/campaigns/{id}` | per ID, cache | `sessions[].outboundCampaignId` on dialer calls. |
| Edges / sites | `GET /api/v2/telephony/providers/edges/{id}` | per ID, cache | Rarely needed; `sessions[].edgeId`. |

Cache every catalog as `id -> name` for the session and reuse it across jobs. Refresh only
on demand. When a catalog fails to load (usually a permission), keep the others and show
IDs for that column.

## Enrichment endpoints (single-conversation work)

| Need | Endpoint | Permission |
| --- | --- | --- |
| Live or full conversation object | `GET /api/v2/conversations/{conversationId}` | `conversation:communication:view` |
| Recording list | `GET /api/v2/conversations/{conversationId}/recordings` | `recording:recording:view` |
| Transcript URL | `GET /api/v2/speechandtextanalytics/conversations/{conversationId}/communications/{communicationId}/transcripturl` | `speechAndTextAnalytics:data:view` |
| Sentiment and topics | `GET /api/v2/speechandtextanalytics/conversations/{conversationId}` | `speechAndTextAnalytics:data:view` |
| Evaluation detail | `GET /api/v2/quality/conversations/{conversationId}/evaluations/{evaluationId}` | `quality:evaluation:view` |
| Participant attributes (live) | `GET /api/v2/conversations/{conversationId}` then `participants[].attributes` | same as conversation |

The analytics record already carries `evaluations[]`, `surveys[]`, and `resolutions[]`
summaries, and `participants[].attributes` for participant data. Pull the detail endpoints
only when the summary is not enough.

## Permissions checklist

| Data | Permission |
| --- | --- |
| Conversation detail jobs and queries | `analytics:conversationDetail:view` |
| Queues | `routing:queue:view` |
| Wrap-up codes | `routing:wrapupCode:view` |
| Divisions | `authorization:division:view` (division membership also limits what you see) |
| Skills and languages | `routing:skill:view`, `routing:language:view` |
| Users | no extra permission for basic name lookup in most orgs |
| Recordings | `recording:recording:view` |
| Transcripts and sentiment | `speechAndTextAnalytics:data:view` |
| Evaluations | `quality:evaluation:view` (or `quality:evaluation:viewAgentEvaluations` for own) |

A 403 on one catalog is a reason to fall back to IDs for that column, not to abort the
analysis.
