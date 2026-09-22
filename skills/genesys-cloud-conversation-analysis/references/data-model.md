# Conversation detail data model

How a Genesys Cloud analytics conversation detail record is shaped, what each level means,
and the interpretation rules that keep an analysis from double counting or misreading it.

## Contents

- The four levels
- Conversation level
- Participants and purposes
- Sessions
- Segments and segment types
- Metrics
- Disconnect semantics
- Flows (IVR and Architect)
- Quality data: MOS, evaluations, surveys
- Customer identity and personal data
- Profile checklist

## The four levels

```text
conversation
  participants[]           one per party (customer, IVR, queue, each agent, ...)
    sessions[]             one per media leg for that party (a transfer creates a new session)
      segments[]           what the party was doing over time (alert, interact, hold, wrapup, ...)
      metrics[]            named counters and timers emitted for that session (ms for t*, count for n*)
      flow                 Architect flow details when the session ran a flow
      mediaEndpointStats[] voice quality per media leg
```

The record is not chronological at any level. Order by segment start time when building
paths and timelines.

## Conversation level

| Field | Meaning |
| --- | --- |
| `conversationId` | The audit key. Keep it on every output row. |
| `conversationStart`, `conversationEnd` | UTC ISO-8601. Duration is the difference; an open conversation has no end. |
| `originatingDirection` | `inbound` or `outbound`. Conversation-level filter dimension. |
| `divisionIds[]` | Divisions the conversation belongs to. |
| `conversationInitiator` | Purpose that started it (`customer`, `agent`, `system`, ...). |
| `customerParticipation` | `true` when a customer party took part. |
| `selfServed` | Genesys flag; when absent, infer flow-only with no queue, agent, or voicemail. |
| `externalTag` | Free-text tag set by integrations. Useful join key to external case systems. |
| `mediaStatsMinConversationMos`, `mediaStatsMinConversationRFactor` | Lowest voice quality across the conversation. |
| `evaluations[]`, `surveys[]`, `resolutions[]` | Quality and outcome summaries (see below). |

## Participants and purposes

Each participant has `participantId`, `purpose`, `participantName`, `userId` (agents),
`externalContactId`, `externalOrganizationId`, `flaggedReason`, `attributes` (key-value
participant data set by flows or integrations), and `sessions[]`.

| Purpose | Who | How to treat it |
| --- | --- | --- |
| `customer` | The external party on inbound | Identity, ANI/DNIS, response-time metrics. Personal data. |
| `external` | The external party on outbound, or a third party | Same as customer; outbound calls may have only this. |
| `ivr` | Architect flow / IVR | Time in IVR (`tIvr`, `tFlow`), flow outcomes. |
| `acd` | The queue | `nOffered`, `tAnswered`, `tAbandon`, `tAcd`, `nOverSla`, `tShortAbandon`. Queue attribution. |
| `agent` | A user handling the interaction | Handle metrics, hold, ACW, transfers. Has `userId`. |
| `user` | A user not in an ACD role (internal call, consult) | Treat as agent for naming and work metrics. |
| `voicemail` | Voicemail system | `tVoicemail`; marks the conversation as reached voicemail. |
| `outbound` / `dialer` | Dialer campaign legs | `outboundCampaignId`, `tDialing`, `tContacting`. |
| `workflow`, `api`, `campaign`, `group`, `fax`, `manual` | Automation and edge cases | Rarely relevant to handle analysis; keep the counts. |

Rules:

- **Agent identity.** Key agents by `userId`. `participantName` is often blank for agents in
  analytics data; resolve the name from the users endpoint and fall back to the record name,
  then the ID.
- **Engaged versus alerted.** An agent whose sessions only contain `alert` segments (and
  emit `tAlert` / `tNotResponding` but no `tTalk`, `tHandle`, or `interact`) was rung and
  did not answer. Exclude from the agent path and AgentName; count in NotResponding.
- **Customer identity.** Prefer the `customer` participant; on outbound use `external`.
  Take `ani`, `dnis`, `remote` from that participant's first session.

## Sessions

One session per media leg for a participant. Fields that matter:

| Field | Meaning |
| --- | --- |
| `sessionId`, `mediaType`, `direction` | Leg identity, channel, inbound/outbound for that leg. |
| `ani`, `dnis`, `remote`, `addressFrom`, `addressTo` | Addresses. Personal data. |
| `provider` | Edge, PureCloud WebRTC, Genesys Cloud Messaging, ... |
| `requestedRoutings[]`, `usedRouting` | Standard, Bullseye, Preferred, Manual, Last, Predictive. Compare requested vs used to find manual overrides. |
| `requestedRoutingSkillIds[]`, `requestedLanguageId` | ACD requirements. |
| `recording` | `true` when the leg was recorded. |
| `flow` | Architect flow details (see Flows). |
| `outboundCampaignId`, `outboundContactId` | Dialer attribution. |
| `mediaEndpointStats[]` | Codecs, MOS, R-factor, latency, jitter, packet loss per media endpoint. |
| `metrics[]`, `segments[]` | See below. |

A transfer produces a new session for the receiving party (and often for the queue). Count
transfers from `nTransferred`, `nBlindTransferred`, `nConsultTransferred`, not from session
counts.

## Segments and segment types

Each segment has `segmentStart`, `segmentEnd`, `segmentType`, `queueId`, `wrapUpCode`,
`wrapUpNote`, `wrapUpTags`, `disconnectType`, `errorCode`, `sipResponseCodes[]`,
`q850ResponseCodes[]`, `conference`, `sourceConversationId`, `destinationConversationId`.

| Segment type | On which purpose | Meaning |
| --- | --- | --- |
| `alert` | agent | Ringing / offered to this agent. |
| `interact` | any | Actively connected. |
| `hold` | agent | Customer on hold. Count for HoldCount. |
| `wrapup` | agent | After-call work. Carries `wrapUpCode`, `wrapUpNote`. |
| `ivr` | ivr | In a flow. |
| `dialing`, `contacting` | agent/outbound | Outbound attempt phases. |
| `delay` | acd | Waiting in queue. Carries `queueId`. |
| `system` | various | Platform-driven transition. |
| `voicemail` | voicemail | Recording a voicemail. |
| `transmitting`, `sharing`, `scheduled`, `parked`, `converting`, `uploading` | media-specific | Keep for the timeline, rarely aggregated. |

Rules:

- **Queue attribution.** A segment with `queueId` on an `acd` or `agent` session attributes
  that session to the queue. Order the queue path by earliest segment start per queue.
- **Wrap-up.** The final wrap-up code is the one on the latest `wrapup` segment across agent
  sessions. Earlier wrap-ups belong to earlier legs (before a transfer).
- **Timeline.** Sort every segment across all participants by `segmentStart`; show purpose,
  type, queue, wrap-up, disconnect, and error codes. That is the case timeline.

## Metrics

`metrics[]` items are `{ name, value, emitDate }`. `t*` values are milliseconds; `n*` values
are counts. Sum per conversation, and also keep the emission count so averages (response
time per message turn) divide by the right denominator.

Attribution rules:

- Agent-work metrics (`tHandle`, `tTalk`, `tTalkComplete`, `tHeld`, `tHeldComplete`, `tAcw`,
  `tAlert`, `tNotResponding`, `tDialing`, `tContacting`, `tAgentResponseTime`,
  `nTransferred`, `nBlindTransferred`, `nConsultTransferred`, `nConsult`, `nConnected`,
  `nOutbound`) are summed only from `agent` / `user` participants.
- Queue metrics (`nOffered`, `tAnswered`, `tAbandon`, `tShortAbandon`, `tAcd`, `nOverSla`)
  come from `acd` sessions.
- Customer metrics (`tUserResponseTime`) come from the customer participant.

The full list with units and derived columns is in `metrics-dictionary.md`.

## Disconnect semantics

`disconnectType` appears on segments when a leg ends. Values: `endpoint` (the party hung
up), `client` (ended from the agent UI or client app), `system`, `error`, `timeout`,
`transfer`, `transfer.conference`, `transfer.consult`, `transfer.forward`, `transfer.noanswer`,
`transfer.notavailable`, `peer` (the other side of a hand-off), `spam`, `uncallable`,
`other`.

Which one "ended" the conversation: take the latest disconnect that is not `peer` and does
not start with `transfer`; those are hand-offs, not endings. Report the purpose that carried
it (customer, agent, acd, ivr, ...) as DisconnectedBy and the type as DisconnectType.
`system`, `error`, and `timeout` together are the platform-ended share; above about 5% is
worth investigating.

### Segment error codes

A segment that ended abnormally carries `errorCode` next to `disconnectType`
(often `ERROR`). Most conversations have none, so an error code is always worth
surfacing: list every distinct code per conversation, note which purpose carried the
segment, and aggregate by code across the batch. The full catalogue is at
[help.genesys.cloud/articles/error-codes](https://help.genesys.cloud/articles/error-codes/).
Two codes matter disproportionately because they mean the agent side dropped the call:

| Code | Meaning | Where to look |
| --- | --- | --- |
| `error.ininedgecontrol.connection.webrtc.endpoint.disconnect.iceIdleDetection` | The WebRTC media path went idle (ICE). The agent's network dropped, a NAT or VPN idle timer fired, or the workstation slept. | Agent network stability, VPN/NAT idle timeouts, power settings |
| `error.ininedgecontrol.connection.webrtc.endpoint.disconnect.dtlsPeerDisconnect` | The agent WebRTC endpoint tore down the secure (DTLS) media session: the browser or desktop app closed, crashed, or lost connectivity. | Agent desktop stability, browser updates, network |

Family prefixes: `error.ininedgecontrol.connection.webrtc.*` is the agent WebRTC phone;
`error.ininedgecontrol.*` is Edge or telephony. Pair an error code with the agent, queue,
media type, and time of day to tell a single bad desktop from a site-wide network fault.

## Flows (IVR and Architect)

`session.flow` has `flowId`, `flowName`, `flowVersion`, `flowType` (INBOUNDCALL,
INBOUNDCHAT, INBOUNDEMAIL, INBOUNDSHORTMESSAGE, OUTBOUNDCALL, WORKFLOW, BOT, ...),
`entryType`, `entryReason`, `exitReason` (for example `DISCONNECT`, `TRANSFER`,
`FLOW_DISCONNECT`, `USER_INPUT_TIMEOUT`), `transferType`, `transferTargetName`,
`transferTargetAddress`, `issuedCallback`, `startingLanguage`, `endingLanguage`, and
`outcomes[]` (`flowOutcomeId`, `flowOutcome`, `flowOutcomeValue` SUCCESS/FAILURE,
`flowOutcomeStartTimestamp`, `flowOutcomeEndTimestamp`).

Flow path is every distinct `flowName` in first-seen order. Flow outcome failures are the
count of `flowOutcomeValue == FAILURE`; a rising failure share on one flow usually means a
data-action or transfer target problem.

## Quality data: MOS, evaluations, surveys

- **MOS** ranges 1 to 5. Take the minimum across `mediaEndpointStats[]` (or the
  conversation-level minimum). Below 3.5 is poor; below 3.0 is unusable. Pair with `codecs`,
  `maxLatencyMs`, and `receivedPackets` / `discardedPackets` for diagnosis.
- **Evaluations** (`evaluations[]`): `evaluationId`, `evaluatorId`, `userId` (the agent),
  `formId`, `formName`, `totalScore`, `totalCriticalScore`, `released`, `rescored`,
  `deleted`, `queueId`. Average `totalScore` per conversation for the grid; per agent for
  the report.
- **Surveys** (`surveys[]`): `surveyId`, `surveyFormId`, `surveyStatus`, `totalScore`,
  `promoterScore` (NPS 0 to 10), `userId`, `queueId`. Only count completed surveys.
- **Resolutions** (`resolutions[]`): `nNextContactAvoided` and related next-contact fields.

## Customer identity and personal data

Personal data lives in: `participantName`, `ani`, `dnis`, `remote`, `addressFrom`,
`addressTo`, `externalContactId`, `wrapUpNote`, `participants[].attributes`, and
transcripts. Redact these by default in anything exported or pasted into a summary.
Conversation ID, queue, agent name (an employee, not a customer), timings, and outcomes
are safe to share internally.

## Profile checklist

A per-conversation profile is complete when it holds:

1. Identity and window: `conversationId`, start and end (UTC and local), duration.
2. Shape: direction, first media type, participant purposes with counts, division IDs.
3. Journey: queue path (IDs, time-ordered), agent keys (engaged only, time-ordered), flow
   path, requested and used routing, requested skill and language IDs.
4. Outcome: offered / answered / abandoned / short-abandon / over-SLA booleans,
   agent-connected, transferred with counts, voicemail, self-served, final wrap-up code ID
   and note, disconnect purpose and type.
5. Timings in seconds: queue wait, alert, talk, hold, ACW, handle, IVR, flow, wait,
   dialing, contacting, voicemail, average customer and agent response times.
6. Quality: min MOS, min R-factor, codecs, max latency, recorded, evaluation and survey
   averages, error / SIP / Q.850 codes.
7. Customer identity fields, kept separate so redaction is a single switch.

IDs only. Apply names at output time.
