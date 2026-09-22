---
name: genesys-cloud-conversation-analysis
description: Query, organize, and analyze Genesys Cloud conversation detail records end to end - submit and page analytics detail jobs without tripping rate limits, resolve queue / wrap-up / division / skill / language / user / flow IDs to names, build one clean profile per conversation from the nested participant > session > segment > metric structure, and turn a batch into a KPI report or a single conversation into a case summary. Use this whenever someone mentions Genesys Cloud conversations, interactions, the analytics API, conversation details jobs, queue or agent performance, abandon rate, handle time, MOS, wrap-up codes, IVR flows, or wants a timeline or write-up of what happened on a call, chat, email, or message - even if they do not say "analytics" or name an endpoint.
---

# Genesys Cloud conversation analysis

Genesys Cloud exposes what happened on a conversation as a deeply nested analytics record
(conversation > participants > sessions > segments and metrics), keyed almost entirely by
IDs. Raw records are hard to read, easy to double count, and slow to fetch if you page
carelessly. This skill is the working method for going from "I need to understand these
conversations" to a defensible report or case summary.

The method has six stages. Do them in order; each stage protects the next one.

1. Scope the question (interval, channels, queues, what "good" looks like).
2. Query with the async detail job, page with the cursor, keep the raw records.
3. Resolve IDs to names with the reference endpoints, only for IDs you actually hold.
4. Profile each conversation once (IDs only, seconds not milliseconds).
5. Analyze: per-conversation columns for a case, breakdown tables for a batch.
6. Report with the data window, units, headline finding, and a next action.

Reference files (read when the stage needs them):

- `references/api-playbook.md` - endpoints, job lifecycle, paging, rate limits, retry rules, permissions.
- `references/data-model.md` - the record structure, participant purposes, segment types, disconnect semantics, flows, quality data.
- `references/metrics-dictionary.md` - every metric and derived column with units and the exact rule that produces it.
- `references/report-templates.md` - batch report layout, observation thresholds, and the case summary template.

## Stage 1: Scope before you query

Ask (or decide) four things before touching the API, because they change the query body and
the report shape:

- **Interval.** Genesys intervals are ISO-8601 `start/end` in UTC. Convert the local business
  window explicitly and say which timezone the report uses. Keep the job under a month; split
  larger ranges by calendar month so one failed job does not lose everything.
- **Channel and direction.** `mediaType` (voice, chat, email, message, callback, cobrowse,
  screenshare, video) is a segment dimension; `originatingDirection` (inbound, outbound) is a
  conversation dimension. Filter at the right level or the filter silently matches nothing.
- **Population.** Queue IDs, division IDs, agent user IDs, wrap-up codes. Resolve names to
  IDs first (search the reference endpoint by name) so the query filters on IDs.
- **Question type.** A single conversation needs the full segment timeline and, if
  permitted, the transcript. A batch needs queue, agent, hour, media, wrap-up, disconnect,
  flow, and quality breakdowns. Decide now which one you are building.

## Stage 2: Query and collect

Use the asynchronous detail job for anything beyond a handful of records. The synchronous
query endpoint caps the interval at seven days and pages of 100 and is rate-limited harder.

The loop, spelled out in `references/api-playbook.md`:

1. `POST /api/v2/analytics/conversations/details/jobs` with `interval`, optional
   `conversationFilters` / `segmentFilters`, and `order` / `orderBy`.
2. Poll `GET .../jobs/{jobId}` every few seconds until `state` is `FULFILLED`. Stop on
   `FAILED`, `CANCELLED`, or `EXPIRED`, and stop after a sane ceiling (30 minutes) rather
   than polling forever.
3. Page `GET .../jobs/{jobId}/results?pageSize=1000&cursor=...` until no cursor is returned.
   Track cursors you have seen; a repeated cursor means a loop, not more data.
4. `DELETE` the job when done so it does not count against the org's concurrent job cap.

Persist every page to local JSONL as it arrives. Analysis is iterative, and re-pulling from
the API is the slowest and most rate-limited part of the whole process. Later stages should
work from the file, never from a second API pull.

Rate limits are per OAuth token and per org. Honour `Retry-After` on HTTP 429, back off
exponentially with jitter on 5xx, and never fan out parallel pages of the same job.

## Stage 3: Resolve names, narrowly

Analytics records carry IDs for queues, wrap-up codes, divisions, skills, languages, and
users. Flows are the exception: `flowName` is inline, `flowId` is also present.

Two resolution patterns, and the choice matters:

- **Small, org-wide catalogs** (queues, wrap-up codes, divisions, skills, languages): page
  the list endpoint once with `pageSize=100`, cache `id -> name`, reuse across jobs.
- **Large or data-dependent catalogs** (users): collect the distinct `userId` values from the
  agent participants you actually hold, then fetch in batches with
  `GET /api/v2/users?id=a,b,c&state=any&pageSize=N` (50 per call). `state=any` keeps
  deactivated and deleted agents resolvable; they still own last month's calls. Never page the
  whole user directory to name twenty agents.

Each lookup type fails independently (usually a missing permission). Log the failure, fall
back to the raw ID, and keep going. A report with IDs in one column beats no report.

Keep names out of the cached profile. Store IDs on the profile and apply names at output
time, so a refreshed lookup never forces a reprofile of fifty thousand records.

## Stage 4: Profile each conversation once

One pass over participants > sessions > segments > metrics produces a flat profile. The
rules that keep the profile honest (details and reasons in `references/data-model.md`):

- **Milliseconds to seconds.** Every `t*` metric arrives in milliseconds. Convert once, at
  profile time, and label every output column with its unit.
- **Only sum agent-work metrics from agent participants.** `tHandle`, `tTalk`, `tHeld`,
  `tAcw`, `tAlert`, `nTransferred`, and friends are emitted on `agent`/`user` participants.
  Summing a same-named emission from another purpose double counts handle time.
- **Alert-only agents are not the agent path.** A participant that only has `alert` segments
  (rang, never answered) was offered the call, not engaged. Exclude them from AgentName and
  AgentPath, but count `tNotResponding` against them in the agent table.
- **Order paths by time, not array order.** Queue path and agent path are ordered by the
  earliest segment start for each ID; the participants array is not chronological.
- **Final wrap-up and final disconnect are the latest ones.** Wrap-up code comes from the
  last `wrapup` segment. The disconnect that ended the conversation is the latest
  `disconnectType` that is not `peer` and not a `*transfer*` type; peer and transfer
  disconnects are hand-offs, not endings.
- **Boolean outcomes come from metric presence.** Offered = `nOffered > 0`, Answered =
  `tAnswered` emitted, Abandoned = `tAbandon` emitted, ShortAbandon = `tShortAbandon`
  emitted, OverSla = `nOverSla > 0`. Do not infer these from segment types.
- **Self-served** means the Genesys `selfServed` flag, or (when absent) flow-only with no
  queue offer, no engaged agent, and no voicemail.
- **Voice quality** is the minimum MOS across sessions (`mediaStatsMinConversationMos` at
  the conversation level, or per-session `mediaEndpointStats`). Below 3.5 is poor.
- **Error codes are rare, so never drop them.** Collect every segment `errorCode` with the
  purpose that carried it and the leg's `disconnectType`. The agent WebRTC drop codes
  (`...webrtc.endpoint.disconnect.iceIdleDetection`, `...dtlsPeerDisconnect`) point at
  agent network or desktop problems, not the platform; see `references/data-model.md`.

## Stage 5: Analyze

For a **batch**, build the breakdown tables in `references/report-templates.md`: queues
(offered / answered / abandoned from ACD sessions, handle metrics from agent sessions routed
through that queue), agents, hour of day, date, media and direction, wrap-up codes,
disconnects, IVR flows, voice quality, divisions, plus outlier lists (longest, lowest MOS).
Compute percentiles with nearest-rank, not interpolation, so p90 is a real conversation.

For a **single conversation or case**, produce the segment timeline (time, participant
purpose, segment type, queue, wrap-up, disconnect) and the per-session participant view,
then narrate it. Pull the transcript only if the question needs what was said and the
token has `speechAndTextAnalytics` permission.

Before drawing conclusions, sanity check the population: count records, confirm the
interval you got matches the one you asked for (jobs can trim to data availability), and
check how many rows still show raw IDs.

## Stage 6: Report

Every report or export identifies the data window (with timezone), the units, the
headline finding, and the recommended next action. Follow the layouts in
`references/report-templates.md` and apply its observation thresholds (abandon above 5%,
service level below 80%, transfers above 15%, poor MOS above 2%, system disconnects above
5%, unanswered alerts of 5 or more per agent) as reference points, not verdicts.

Redact by default. `participantName`, `ani`, `dnis`, `externalContactId`, wrap-up notes,
and participant attributes routinely contain personal data. Keep conversation IDs (they are
the audit key) and drop or mask the rest unless the reader is entitled to it.

## Things that go wrong

- Handle time doubled: a non-agent participant emitted `tHandle`. Filter by purpose.
- Agent path shows someone who never spoke: alert-only participant included.
- Abandon rate looks wrong: counted `tAbandon` from agent sessions or included short abandons.
- Empty result set with filters: media type applied as a conversation filter, or a queue
  name used where a queue ID was required.
- 429 storms: parallel page fetches or per-agent user lookups. Serialize and batch.
- Names missing for some agents: users fetched with `state=active`. Use `state=any`.
- Times off by hours: interval sent in local time without an offset. Use UTC with `Z`.
- Report says "0 seconds" everywhere: milliseconds never converted, then rounded.
