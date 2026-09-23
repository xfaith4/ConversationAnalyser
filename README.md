# ConversationAnalyser

WPF tool for Genesys Cloud conversation detail analytics. It submits an async
`/api/v2/analytics/conversations/details/jobs` query, collects every page of results,
and presents them as a per-conversation grid, a drill-down detail panel, and a
collection-level report. The Campaign Analysis tab adds outbound campaign lookup, live
status, rules, and events, and feeds a campaign straight into the conversation query.

## Files

- `GenesysConvAnalyzer.ps1` - main WPF app
- `src/auth/PkceAuth.ps1` - OAuth 2.0 Authorization Code + PKCE (browser sign-in, local callback listener, token exchange/refresh; no UI code)
- `src/ui/UiApiRetry.ps1` - retry/backoff helper used by the app
- `src/analysis/ConversationAnalysis.ps1` - field extraction, flat rows, report aggregation, and HTML/JSON rendering (no UI or network code)
- `src/campaign/CampaignAnalysis.ps1` - campaign list, status snapshot, rules, and events for the Campaign Analysis tab (no UI or network code; takes a request callback)
- `GenesysConvAnalyzer.config.example.json` - template for the per-machine config file
- `tests/PkceAuth.Tests.ps1` - Pester tests for the PKCE helpers
- `tests/ConversationAnalysis.Tests.ps1` - Pester tests for the analysis helpers
- `tests/CampaignAnalysis.Tests.ps1` - Pester tests for the campaign module (fake API, no network)

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
Invoke-Pester -Path .\tests\ConversationAnalysis.Tests.ps1 -Output Detailed
Invoke-Pester -Path .\tests\CampaignAnalysis.Tests.ps1 -Output Detailed
```

Works in Windows PowerShell 5.1 and PowerShell 7+.

## Query Builder: interval and time zones

- **Dates and times are entered in US Eastern** (business HQ), whatever time zone the
  machine runs in. The app converts them to UTC, which is what Genesys Cloud stores for
  `conversationStart` and `conversationEnd`, and shows the exact UTC interval it will send
  under the pickers after **Preview** or **Submit**. Daylight saving is handled per date
  (EDT/EST); a start time that falls in the spring-forward gap is moved forward one hour.
  The presets (Today, Yesterday, Last 7 Days, ...) also use the Eastern calendar date.
- **startOfDayIntervalMatching** (checkbox, on by default) sends
  `"startOfDayIntervalMatching": true` with the job. The details job otherwise matches any
  conversation that has a segment inside the interval, so long-lived email, message, and
  callback conversations that started days or weeks earlier are returned too. With the flag
  on, only conversations whose `conversationStart` is on or after 00:00 UTC of the interval
  start date are included. Note that the cut-off is the start *date* in UTC, not the exact
  start time, so a few conversations from the hours just before the interval can still
  appear; the interval check in the Job Monitor makes that visible.
- **Interval check**: after every collection the Job Monitor logs how many conversations
  started inside, before, or after the requested interval, plus the earliest and latest
  `conversationStart` in UTC and Eastern. Nothing is dropped; the numbers are there so a
  result set that reaches back beyond the interval is obvious at once.
- The Report tab shows the query interval in UTC and its Eastern equivalent.

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

### HTML report layout

The exported page is ordered for investigation: awareness first, evidence underneath.

1. **Headline and observations** - the flags that crossed a threshold, with the thresholds in
   use stated beneath them.
2. **Dashboard** - inline SVG charts (no external dependencies): daily and hourly trends
   (volume bars over an abandon-rate line), disconnect reasons, segment error codes (agent
   WebRTC drops highlighted), queue abandon rate, and agents not responding. Dashed lines are
   the reference thresholds, flagged marks carry a marker glyph as well as colour, every mark
   has a hover tooltip, and each panel links to the table that holds its evidence.
3. **Key metrics** - the KPI tiles.
4. **Breakdowns** - every table in a collapsible section with a one-line summary. Short
   aggregate tables open by default; drill-down lists (Agents, Longest, Lowest MOS, Error
   Conversations) stay collapsed; tables over 25 rows show 25 until you press *Show all*. The
   sticky nav has *Expand all* / *Collapse all*, and a floating *Top* button returns to the
   headline.

**Printing** produces the executive brief: headline, observations, dashboard, and tiles. The
breakdown tables are left out of the PDF on purpose; investigators use the HTML.

**Thresholds** default to abandon 5%, within service level 80%, transfers 15%, poor MOS 2%,
unanswered alerts per agent 5, platform disconnects 5%. Override any of them with a
`reportThresholds` object in the config file (see `GenesysConvAnalyzer.config.example.json`);
the values in use are written into the observations, the chart reference lines, and the JSON
export.

Queue metrics are attributed per session: offered, answered, and abandoned counts come from
ACD sessions; handle metrics come from agent sessions routed through that queue. Durations
are in seconds, and times are shown in the machine's local time zone (the query interval
itself is entered in US Eastern; see the Query Builder section).

## Campaign Analysis tab

Investigate one outbound campaign without leaving the app. Sign in first: every call uses
the same token, retry policy, and Job Monitor logging as the analytics job.

1. **Load Campaigns** reads every voice, SMS, and email campaign in the org
   (`GET /api/v2/outbound/campaigns/all`). Type in the box next to it to filter by name, ID,
   status, media type, or division; several words must all match. The grid shows Name,
   Media, Status, Division, and Modified.
2. Select a campaign. The header shows its ID, division, and created/modified times, and
   the buttons become available:
   - **Refresh Status** fetches, independently, the campaign configuration
     (`/api/v2/outbound/campaigns/{id}`), progress (`/progress`), diagnostics
     (`/diagnostics`), live stats (`/stats`), and the diagnostics summary
     (`/api/v2/outbound/diagnostics/campaigns/{id}/summary`). SMS and email campaigns use
     `/api/v2/outbound/messagingcampaigns/{id}` and its `/progress`; they have no
     diagnostics or stats. A source that fails is listed in red on the Status tab and in
     the Job Monitor log, and the others still render. Known fields get friendly labels;
     anything else the API returns is shown under its raw field name, and the full payload
     is on the Raw JSON tab.
   - **Rules** reads every campaign rule (`/api/v2/outbound/campaignrules`; the API cannot
     filter by campaign) and keeps the ones that watch this campaign (`trigger`), act on it
     (`target`), or both. Conditions are joined with AND or OR according to the rule's
     match-any setting.
   - **Recent Events** reads the newest pages of the org-wide outbound event log
     (`/api/v2/outbound/events`, five pages of 100) and keeps the events that mention this
     campaign, newest first.
   - **Analyze Conversations** clears the Query Builder filters, adds one segment filter
     `outboundCampaignId = <id>`, sets the interval (from the campaign's creation date when
     it was created in the last 30 days, otherwise the last 30 days, through today), and
     shows the request preview. Review the interval, then **Submit Async Job** as usual;
     Results and Report then describe that campaign's conversations.
   - **Copy ID** puts the campaign ID on the clipboard.

The segment filter dimension list in the Query Builder also offers `outboundCampaignId`,
`outboundContactId`, and `outboundContactListId` for hand-built queries.

## Required permissions

- Analytics conversation detail jobs (existing requirement).
- Optional, for name resolution: read access to routing queues (`routing:queue:view`),
  wrap-up codes (`routing:wrapupCode:view`), skills, languages, and divisions. Missing
  access only affects that lookup type; the Job Monitor log shows which call failed.
- Campaign Analysis tab: campaign view (`outbound:campaign:view`) for the list,
  configuration, progress, diagnostics, and stats; messaging campaign view
  (`outbound:messagingCampaign:view`) for SMS and email campaigns; campaign rule view
  (`outbound:campaignRule:view`) for Rules; and outbound event log view
  (`outbound:eventLog:view`) for Recent Events. A missing permission fails only that call
  and is logged in the Job Monitor.

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
| `Start` | Yes | Conversation start (`conversationStart`, UTC on the platform, shown in machine-local time). |
| `End` | No | Conversation end (`conversationEnd`, shown in machine-local time); blank while still active. |
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
