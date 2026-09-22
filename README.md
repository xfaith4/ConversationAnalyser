# ConversationAnalyser

WPF tool for Genesys Cloud conversation detail analytics. It submits an async
`/api/v2/analytics/conversations/details/jobs` query, collects every page of results,
and presents them as a per-conversation grid, a drill-down detail panel, and a
collection-level report.

## Files

- `GenesysConvAnalyzer.ps1` - main WPF app
- `src/auth/PkceAuth.ps1` - OAuth 2.0 Authorization Code + PKCE (browser sign-in, local callback listener, token exchange/refresh; no UI code)
- `src/ui/UiApiRetry.ps1` - retry/backoff helper used by the app
- `src/analysis/ConversationAnalysis.ps1` - field extraction, flat rows, report aggregation, and HTML/JSON rendering (no UI or network code)
- `GenesysConvAnalyzer.config.example.json` - template for the per-machine config file
- `tests/PkceAuth.Tests.ps1` - Pester tests for the PKCE helpers

## Run

```powershell
.\GenesysConvAnalyzer.ps1
```

Or specify a config file:

```powershell
.\GenesysConvAnalyzer.ps1 -ConfigPath .\GenesysConvAnalyzer.config.json
```

## Authentication (PKCE)

The app signs in through your browser using the OAuth 2.0 Authorization Code grant with
PKCE ([Genesys docs](https://developer.genesys.cloud/authorization/platform-auth/use-pkce)).
Users never handle a client secret: the token belongs to the signed-in user and carries
that user's permissions, so they need analytics permissions in Genesys Cloud.

### One-time setup (admin)

1. In Genesys Cloud, go to **Admin > Integrations > OAuth** and add a client with grant
   type **Code Authorization**. No secret is used by this app.
2. Add `http://localhost:8085/callback/` as an **Authorized redirect URI** (any free
   port works; keep it identical to the value configured below).
3. Give users the **Client ID** and redirect URI. Neither is a secret.

### Where the Client ID and redirect URI come from

Highest precedence first:

1. Environment variables `GENESYS_CLIENT_ID`, `GENESYS_REDIRECT_URI`, `GENESYS_SCOPE`,
   `GENESYS_AUTH_MODE`, `GENESYS_REGION`.
2. The config file next to the script. Copy `GenesysConvAnalyzer.config.example.json` to
   `GenesysConvAnalyzer.config.json` and fill it in. The real file is git-ignored.
3. `$script:AuthDefaults` near the top of `GenesysConvAnalyzer.ps1`, for a build that ships
   with the values baked in.

The app writes the region, client ID, and redirect URI back to the config file after a
successful sign-in. It never writes a secret.

### Sign-in flow

Click **Sign in**. The app starts a listener on the redirect URI, opens the Genesys login
page in your default browser, captures the returned code, and exchanges it for a token.
Clicking **Sign in** again uses the refresh token silently when the OAuth client has
refresh tokens enabled, and falls back to the browser otherwise. If the listener port is
already in use, a prompt asks you to paste the redirect URL from the browser instead.

### Client credentials (automation only)

Set `authMode` to `client_credentials` (config file or `GENESYS_AUTH_MODE`) and provide the
secret through the `GENESYS_CLIENT_SECRET` environment variable. The UI has no secret
field and the secret is never stored. With both values present the app authenticates on
startup.

### Tests

```powershell
Invoke-Pester -Path .\tests\PkceAuth.Tests.ps1 -Output Detailed
```

Works in Windows PowerShell 5.1 and PowerShell 7+.

## Results tab

- **Grid**: one row per conversation with about 90 standard columns (see the column
  reference below). **Column Selector** picks which standard columns the grid shows and
  adds participant attribute columns (`A:<key>`). Header tooltips explain each column.
- **Resolve Names**: looks up queue, wrap-up code, division, skill, and language names, and
  resolves the agents present in the loaded data in batches of 50 via
  `GET /api/v2/users?id=...&state=any` (only users who appear in the conversations are
  fetched; inactive and deleted users still resolve).
  This also runs automatically after a collection when you are authenticated. Any lookup
  that fails (for example, a missing permission) is logged in the Job Monitor and the raw
  ID is shown instead.
- **Detail panel** (drag the splitter to resize):
  - Overview: outcome, queue/agent path, wrap-up, who disconnected, ANI/DNIS, flow, timings, MOS, evaluations, surveys
  - Participants / Sessions: one row per session with media, queue, ANI/DNIS, and per-session talk/hold/ACW/handle
  - Segment Timeline: every segment across all participants in time order, with offset from conversation start
  - Metrics: every raw metric as emitted, with seconds for timers
  - Flows: Architect flow entry/exit, transfer target, languages, outcomes
  - Attributes: participant data from every participant
- **Export CSV**: every standard column plus the selected attribute columns, for all
  loaded conversations (not just the rows the grid shows).

## Report tab

Built from all loaded conversations:

- **Key metrics** - volume, service (answer/abandon rate, service level, speed of answer),
  handling (AHT with median and 90th percentile, talk/hold/ACW, transfer and hold rates),
  and quality/CX (MOS, recording coverage, evaluations, surveys).
- **Observations** - rule-based flags. Each one states its reference threshold (for example,
  a 5% abandon rate or 80% within service level); compare them against your own targets.
- **Breakdowns** - Queues, Agents, Hourly, Daily, Media & Direction, Wrap-up Codes,
  Disconnects, Error Codes, IVR Flows, Voice Quality, Divisions, Longest, Lowest MOS, and
  Error Conversations.
- **Error codes** - every segment `errorCode` is listed with who carried the segment, how
  that leg ended, and a plain-language meaning for known codes (the agent WebRTC drops
  `...webrtc.endpoint.disconnect.iceIdleDetection` and `...dtlsPeerDisconnect` are called
  out in the observations). Full list:
  [help.genesys.cloud/articles/error-codes](https://help.genesys.cloud/articles/error-codes/).
  The segment filter dimension `errorCode` lets you query only errored conversations.
- **Export Report (HTML)** - a self-contained HTML file (light/dark aware, printable) plus a
  `.json` file with the same numbers for downstream tools.

Queue metrics are attributed per session: offered, answered, and abandoned counts come from
ACD sessions; handle metrics come from agent sessions routed through that queue. Durations
are in seconds, and times are local.

## Required permissions

- Analytics conversation detail jobs (existing requirement).
- Optional, for name resolution: read access to routing queues (`routing:queue:view`),
  wrap-up codes (`routing:wrapupCode:view`), skills, languages, and divisions. Missing
  access only affects that lookup type; the Job Monitor log shows which call failed.

## Notes

- The grid shows at most the first 20,000 rows to avoid UI lockups. The report and exports
  always include every loaded conversation.
- Profiles and grid rows are cached per dataset, so switching columns or tabs does not
  re-analyze. Expect roughly 1-3 ms per conversation for the first analysis.
- The **Redact exports** checkbox is on by default. It masks customer name, ANI, DNIS,
  external contact ID, and wrap-up notes in the CSV, and the equivalent fields in the JSONL.
  Turn it off only when a raw export is explicitly needed in a trusted environment.
  The HTML report contains aggregates plus conversation IDs, queue names, and agent names;
  it holds no customer identifiers.

## Column reference

| Column | Default grid | Description |
| --- | --- | --- |
| `ConversationId` | Yes | Genesys Cloud conversation ID. |
| `Start` | Yes | Conversation start (local time). |
| `End` | No | Conversation end (local time); blank while still active. |
| `StartDate` | No | Local start date (yyyy-MM-dd), for pivoting. |
| `StartHour` | No | Local start hour (0-23), for pivoting. |
| `DayOfWeek` | No | Local start day of week. |
| `DurationSec` | Yes | Conversation start to end, seconds. |
| `Direction` | Yes | Originating direction (inbound / outbound). |
| `MediaType` | Yes | Primary media type (customer session, else first session). |
| `Initiator` | No | Who initiated the conversation (conversationInitiator). |
| `CustomerParticipation` | No | True when a customer took part. |
| `QueueId` | No | First queue ID the conversation entered. |
| `QueueName` | Yes | First queue (name when resolved, else ID). |
| `FinalQueue` | No | Last queue the conversation entered. |
| `QueuePath` | No | Every queue entered, in order. |
| `QueueCount` | No | Number of distinct queues entered. |
| `RequestedRouting` | No | Routing methods requested (Standard, Bullseye, Preferred, ...). |
| `UsedRouting` | No | Routing method that actually delivered the conversation. |
| `RequestedSkills` | No | Requested ACD skills (names when resolved). |
| `RequestedLanguage` | No | Requested ACD language (name when resolved). |
| `AgentName` | Yes | First agent who engaged (alert-only agents excluded). |
| `AgentUserId` | No | User ID of the first engaged agent. |
| `FinalAgent` | No | Last agent who engaged. |
| `AgentPath` | No | Every engaged agent, in order. |
| `AgentCount` | No | Number of distinct engaged agents. |
| `CustomerName` | No | Customer (or external) participant name. Redacted in exports. |
| `Ani` | No | Customer session ANI / from-address. Redacted in exports. |
| `Dnis` | No | Customer session DNIS / to-address. Redacted in exports. |
| `ExternalContactId` | No | External contact ID. Redacted in exports. |
| `ExternalTag` | No | Conversation external tag. |
| `Offered` | No | True when offered to a queue (nOffered). |
| `Answered` | Yes | True when answered from a queue (tAnswered emitted). |
| `Abandoned` | Yes | True when abandoned in queue (tAbandon emitted). |
| `ShortAbandon` | No | True when abandoned inside the short-abandon threshold. |
| `OverSla` | No | True when answered or abandoned outside the queue service level (nOverSla). |
| `AgentConnected` | No | True when an agent interacted with the customer. |
| `Transferred` | Yes | True when at least one transfer occurred. |
| `TransferCount` | No | Number of transfers (nTransferred). |
| `BlindTransfers` | No | Blind transfers (nBlindTransferred). |
| `ConsultTransfers` | No | Consult transfers (nConsultTransferred). |
| `Consults` | No | Consults started (nConsult). |
| `HoldCount` | Yes | Number of agent hold segments. |
| `Voicemail` | No | True when the conversation reached voicemail. |
| `SelfServed` | No | Genesys selfServed flag, or flow-only with no queue, agent, or voicemail. |
| `WrapUpCode` | Yes | Final wrap-up code (name when resolved). |
| `WrapUpNote` | No | Final wrap-up note. Redacted in exports. |
| `DisconnectedBy` | Yes | Participant purpose that ended the conversation (customer, agent, acd, ivr, ...). |
| `DisconnectType` | Yes | How it ended: endpoint (hung up), client (agent UI), system, error, timeout, ... |
| `FlowName` | No | First Architect flow the conversation entered. |
| `FlowPath` | No | Every flow entered, in order. |
| `FlowType` | No | Type of the first flow (INBOUNDCALL, INBOUNDCHAT, ...). |
| `FlowExitReason` | No | Why the conversation left the first flow. |
| `FlowOutcomes` | No | Flow outcomes recorded. |
| `FlowOutcomeFailures` | No | Flow outcomes recorded as FAILURE. |
| `tAnsweredSec` | Yes | Queue wait before answer (speed of answer), seconds. |
| `tAbandonSec` | No | Queue wait before abandon, seconds. |
| `tAcdSec` | No | Total time in queue (ACD), seconds. |
| `tWaitSec` | No | Total wait time (tWait), seconds. |
| `tIvrSec` | No | Time in IVR, seconds. |
| `tFlowSec` | No | Time in flows (tFlow), seconds. |
| `tAlertSec` | No | Agent alerting (ringing) time before answer, seconds. |
| `tNotRespondingSec` | No | Agent alerting time that went unanswered, seconds. |
| `tDialingSec` | No | Outbound dialing time, seconds. |
| `tContactingSec` | No | Outbound contacting time, seconds. |
| `tTalkSec` | Yes | Agent talk time, seconds. |
| `tHeldSec` | Yes | Agent hold time, seconds. |
| `tAcwSec` | Yes | After-call work (wrap-up) time, seconds. |
| `tHandleSec` | Yes | Handle time (talk + hold + ACW), seconds. |
| `tVoicemailSec` | No | Time in voicemail, seconds. |
| `AvgUserResponseSec` | No | Average customer response time per message turn, seconds (messaging). |
| `AvgAgentResponseSec` | No | Average agent response time per message turn, seconds (messaging). |
| `nConnected` | No | Agent connected count (nConnected). |
| `nOffered` | No | Queue offer count (nOffered). |
| `nOverSla` | No | Queue service-level breaches (nOverSla). |
| `nOutbound` | No | Outbound attempts by agents (nOutbound). |
| `nError` | No | Error count (nError). |
| `MinMos` | Yes | Lowest MOS (voice quality, 1-5) seen in the conversation. |
| `MinRFactor` | No | Lowest R-factor seen in the conversation. |
| `Codecs` | No | Audio codecs used. |
| `MaxLatencyMs` | No | Highest media latency, milliseconds. |
| `Recorded` | No | True when any session was recorded. |
| `Evaluations` | No | Quality evaluations linked to the conversation. |
| `EvalScore` | No | Average evaluation total score. |
| `Surveys` | No | Surveys linked to the conversation. |
| `SurveyScore` | No | Average survey total score. |
| `NpsScore` | No | Average survey promoter (NPS) score, 0-10. |
| `Resolutions` | No | Resolution records linked to the conversation. |
| `ErrorCodes` | Yes | Segment error codes (for example `error.ininedgecontrol.connection.webrtc.endpoint.disconnect.iceIdleDetection`). |
| `ErrorSegments` | No | Number of segments carrying an error code. |
| `ErrorDisconnect` | No | True when any segment ended with disconnectType error. |
| `SipCodes` | No | SIP response codes. |
| `Q850Codes` | No | Q.850 cause codes. |
| `ParticipantCount` | No | Number of participants. |
| `Participants` | No | Participant purposes with counts. |
| `Divisions` | No | Divisions (names when resolved). |
