# Report templates

Two deliverables: a batch report over many conversations, and a case summary of one
conversation. Both start with the data window, units, and headline, because a reader who
skims only the first lines must still know what they are looking at.

## Contents

- Batch report layout
- Observation thresholds
- Case summary template
- Output formats
- Redaction rules

## Batch report layout

Use this order. Drop a section only when the data cannot support it (say so in one line).

```markdown
# Conversation report: <population in plain words>

Data window: <local start> to <local end> (<timezone>), <N> conversations, source: <job id or file>.
Units: seconds unless stated; rates are percentages of the stated denominator.

## Headline
One or two sentences: the single most important finding and the recommended next action.

## KPIs
| Metric | Value | Denominator |
| --- | --- | --- |
| Conversations | | |
| Offered / Answered / Abandoned (rate) | | offered |
| Short abandons | | offered |
| Service level | | offered |
| Average speed of answer (p50 / p90) | | answered |
| AHT (p50 / p90) | | handled agent sessions |
| Average talk / hold / ACW | | sessions that emitted each |
| Transfer rate | | reached an agent |
| Self-served share | | all |
| Voicemail share | | all |
| Poor MOS share (< 3.5) | | conversations with MOS |
| Evaluated (avg score) / Surveyed (avg NPS) | | all |

## Observations
Rule-based findings (see thresholds), each with the number and the table to look at.

## Breakdowns
### Queues       - offered, answered, abandoned, abandon %, SL %, ASA, AHT, talk, hold, ACW, transfers
### Agents       - conversations, handled, AHT, talk, hold, holds, ACW, total handle hrs, alert, not responding, transfers, outbound
### Hour of day  - conversations, share, abandon %, AHT (local time)
### Date         - same by day
### Media and direction
### Wrap-up codes - of conversations that reached an agent
### Disconnects  - by DisconnectedBy and DisconnectType
### Error codes  - every segment errorCode: conversations, share, purposes, disconnect types, meaning
### IVR flows    - entries, exit reasons, outcome failures, self-served share
### Voice quality - MOS buckets, codecs, latency
### Divisions

## Outliers
- Longest conversations (top 10) with queue, agent, disconnect.
- Lowest MOS (top 10) with codecs and latency.
- Most transferred, if transfer rate was flagged.
- Conversations carrying a segment error code (most recent 25) with agent, queue, disconnect, and codes.

## Method and caveats
Interval requested vs returned, rows with unresolved IDs, filters applied, anything excluded.
```

## Observation thresholds

These are reference points that make a report say something, not verdicts. State the
number, the threshold, and where to look.

| Observation | Trigger | Point the reader at |
| --- | --- | --- |
| High abandon rate | abandoned / offered > 5% | Queues table, hour of day |
| Service level miss | (offered - overSla) / offered < 80% | Queues table, agents alert times |
| High transfer rate | transferred / reached-agent > 15% | Queues and Wrap-up codes (routing fit) |
| Poor voice quality | MinMos < 3.5 share > 2% | Lowest MOS table, codecs, latency |
| Platform-ended calls | system + error + timeout disconnects > 5% | Disconnects, SIP / Q.850 codes |
| Segment error codes | any conversation with an errorCode | Error codes table; name the most common code |
| Agent WebRTC drops | any `...webrtc.endpoint.disconnect.iceIdleDetection` or `...dtlsPeerDisconnect` | Agent network, VPN/NAT idle timeouts, desktop stability |
| Slowest queue | highest AHT with a meaningful handled count | Queues table |
| Unanswered alerts | any agent with tNotResponding count >= 5 | Agents table; presence and alert-timeout settings |
| Flow failures | outcome failure share rising on one flow | IVR flows table |
| Peak load | busiest hour and its share of volume | Hour of day |

Always add the denominator to the sentence ("312 of 4,180 offered") so the reader can
judge the weight of the finding.

## Case summary template

For one conversation (a complaint, an escalation, a quality review, an incident).

```markdown
# Conversation <conversationId>

When: <local start> to <local end> (<timezone>), <duration>. Channel: <media>, <direction>.
Customer: <redacted or entitled identifier>. Division: <name>. External tag: <tag or none>.

## What happened
Three to six sentences in plain language: how it arrived (flow / DNIS), where it queued and
for how long, who handled it and for how long, transfers and holds, how it ended, what
wrap-up was applied.

## Timeline
| Time (local) | Party | Event | Detail |
| --- | --- | --- | --- |
| 10:02:14 | ivr | flow "Main Menu" | exit: TRANSFER to queue Billing |
| 10:03:01 | acd | delay in Billing | waited 84 s |
| 10:04:25 | agent Ada Directory | alert 6 s, interact 9 m 40 s | 2 holds (1 m 12 s) |
| 10:14:11 | agent Ada Directory | wrap-up | code "Refund issued" |
| 10:14:11 | customer | disconnect endpoint | customer hung up |

## Outcome
Wrap-up code and note (note redacted unless entitled), disconnect, transfers, evaluation
and survey results, resolution flags.

## Signals worth noting
Anomalies: long hold, repeated alerts, poor MOS, error / SIP codes, flow outcome failures,
routing override (requested vs used), missing recording.

## Sources
Analytics job or file, whether transcript / recording / evaluation detail was consulted.
```

If a transcript was available, add a "Key moments" section with two to five short quotes
tied to timeline rows; do not paste the whole transcript.

## Output formats

- Human-readable first (Markdown or HTML), then a machine copy of the same numbers (JSON for
  the report, CSV for the per-conversation rows). The JSON must carry the data window and
  units so it stands alone.
- Per-conversation CSV columns follow the metrics dictionary names so files from different
  runs line up.
- Keep IDs next to names (`QueueName` and `QueueId`, `AgentName` and `AgentUserId`) so a
  reader can chase a row back into Genesys.

## Redaction rules

Redact by default; un-redact only for an entitled reader and say so in the report header.

| Field | Default |
| --- | --- |
| CustomerName, participantName of customer / external | replace with `[redacted]` or a stable hash |
| Ani, Dnis, remote, addressFrom, addressTo | mask all but the last 4 characters |
| ExternalContactId | drop |
| WrapUpNote | drop or summarize without personal data |
| Participant attributes | include only keys the reader asked for |
| Transcript quotes | only what the case needs; never customer identifiers |
| ConversationId, queue, agent name, timings, outcomes | keep |
