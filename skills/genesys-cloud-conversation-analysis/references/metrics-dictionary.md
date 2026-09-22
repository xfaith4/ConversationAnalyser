# Metrics dictionary

Raw Genesys Cloud analytics metrics, then the derived per-conversation columns built from
them. Raw `t*` metrics arrive in milliseconds; every derived column is in seconds unless its
name says otherwise. Sums are per conversation; "emissions" is how many times the metric
was emitted, used as the denominator for averages.

## Contents

- Raw metrics by owner
- Derived columns: journey
- Derived columns: outcome booleans
- Derived columns: timings
- Derived columns: quality and errors
- Aggregation rules for batch tables

## Raw metrics by owner

| Metric | Emitted on | Unit | Meaning |
| --- | --- | --- | --- |
| `nOffered` | acd | count | Offered to the queue. |
| `tAnswered` | acd | ms | Wait before an agent answered (speed of answer). |
| `tAbandon` | acd | ms | Wait before the customer abandoned. |
| `tShortAbandon` | acd | ms | Abandon inside the queue's short-abandon threshold. |
| `tAcd` | acd | ms | Total time in queue. |
| `nOverSla` | acd | count | Answered or abandoned outside the service level target. |
| `tWait` | acd / agent | ms | Total wait time. |
| `tIvr` | ivr | ms | Time in IVR. |
| `tFlow` | ivr | ms | Time in Architect flows. |
| `tFlowOut` | ivr | ms | Time from flow exit to next leg. |
| `tAlert` | agent | ms | Ringing time before answer. |
| `tNotResponding` | agent | ms | Ringing time that went unanswered. |
| `tTalk`, `tTalkComplete` | agent | ms | Talk time (complete = leg ended in this interval). |
| `tHeld`, `tHeldComplete` | agent | ms | Hold time. |
| `tAcw` | agent | ms | After-call work. |
| `tHandle` | agent | ms | Talk + hold + ACW. Use this, do not re-add the parts. |
| `tDialing`, `tContacting` | agent / outbound | ms | Outbound attempt phases. |
| `tVoicemail` | voicemail | ms | Time in voicemail. |
| `tAgentResponseTime` | agent | ms per turn | Messaging: agent response time per turn. Average, do not sum. |
| `tUserResponseTime` | customer | ms per turn | Messaging: customer response time per turn. Average. |
| `nConnected` | agent | count | Agent connected to the customer. |
| `nTransferred` | agent | count | Transfers initiated. |
| `nBlindTransferred`, `nConsultTransferred` | agent | count | Transfer kinds. |
| `nConsult` | agent | count | Consults started. |
| `nOutbound` | agent | count | Outbound attempts. |
| `nError` | any | count | Errors. |
| `nStateTransitionError` | any | count | Routing state errors. |

## Derived columns: journey

| Column | Rule |
| --- | --- |
| QueueName / FinalQueue / QueuePath / QueueCount | Distinct `queueId` values from acd and agent segments, ordered by earliest segment start. First, last, all joined with ` > `, count. |
| AgentName / FinalAgent / AgentPath / AgentCount | Engaged agent keys (userId, else participantName) ordered by earliest engaged segment. Alert-only participants excluded. |
| AgentUserId | Raw key of the first engaged agent, kept alongside the name. |
| FlowName / FlowPath / FlowType / FlowEntryReason / FlowExitReason | From `session.flow` on ivr sessions, first-seen order. |
| FlowOutcomes / FlowOutcomeFailures | Count of `flow.outcomes[]`, and those with `flowOutcomeValue == FAILURE`. |
| RequestedRouting / UsedRouting | Distinct `requestedRoutings[]`; the `usedRouting` of the delivering session. |
| RequestedSkills / RequestedLanguage | `requestedRoutingSkillIds[]`, `requestedLanguageId`, resolved to names. |
| Direction / MediaType | `originatingDirection`; media type of the first session. |
| Participants / ParticipantCount | Purpose counts, for example `customer:1, ivr:1, acd:2, agent:2`. |
| Divisions | `divisionIds[]` resolved. |

## Derived columns: outcome booleans

| Column | Rule |
| --- | --- |
| Offered | `nOffered` sum > 0. |
| Answered | `tAnswered` emitted at least once. |
| Abandoned | `tAbandon` emitted at least once. |
| ShortAbandon | `tShortAbandon` emitted at least once. |
| OverSla | `nOverSla` sum > 0. |
| AgentConnected | An agent participant had an `interact` segment (or `nConnected` > 0). |
| Transferred / TransferCount / BlindTransfers / ConsultTransfers / Consults | From the `n*` transfer metrics on agent participants. |
| HoldCount | Number of `hold` segments on agent sessions. |
| Voicemail | Any `voicemail` segment or purpose. |
| SelfServed | `selfServed` flag; else flows present, not offered, no engaged agent, no voicemail. |
| Recorded | Any session with `recording == true`. |
| WrapUpCode / WrapUpNote | Latest `wrapup` segment across agent sessions; code resolved to name. |
| DisconnectedBy / DisconnectType | Latest non-peer, non-transfer `disconnectType` and the purpose that carried it. |

## Derived columns: timings

All rounded to whole seconds (away from zero) except the averages, which keep one decimal.

| Column | Rule |
| --- | --- |
| DurationSec | `conversationEnd - conversationStart`. |
| tAnsweredSec, tAbandonSec, tAcdSec, tWaitSec | Queue metric sums / 1000. |
| tIvrSec, tFlowSec | IVR metric sums / 1000. |
| tAlertSec, tNotRespondingSec | Agent alerting sums / 1000. |
| tTalkSec, tHeldSec, tAcwSec, tHandleSec | Agent work sums / 1000. Only from agent/user participants. |
| tDialingSec, tContactingSec, tVoicemailSec | As named. |
| AvgAgentResponseSec | `tAgentResponseTime` sum / emissions / 1000. |
| AvgUserResponseSec | `tUserResponseTime` sum / emissions / 1000. |

## Derived columns: quality and errors

| Column | Rule |
| --- | --- |
| MinMos / MinRFactor | Minimum across media endpoint stats, or the conversation-level minimum field. |
| Codecs | Distinct codec names across media endpoint stats. |
| MaxLatencyMs | Maximum `maxLatencyMs` across media endpoint stats. Stays in ms. |
| Evaluations / EvalScore | Count of `evaluations[]`; average `totalScore`. |
| Surveys / SurveyScore / NpsScore | Count of completed `surveys[]`; average `totalScore`; average `promoterScore`. |
| Resolutions | Count of `resolutions[]`. |
| ErrorCodes / SipCodes / Q850Codes | Distinct `errorCode`, `sipResponseCodes[]`, `q850ResponseCodes[]` across segments. |

## Aggregation rules for batch tables

- **Averages** divide by the number of conversations (or sessions) that actually emitted
  the metric, never by the whole population. A conversation with no hold does not lower
  average hold time.
- **Percentiles** use nearest-rank on the sorted values so a reported p90 is a real
  observation.
- **Abandon rate** = abandoned / offered, from acd sessions. Report short abandons
  separately; many orgs exclude them from the headline rate.
- **Service level** = (offered - overSla) / offered, from acd sessions.
- **Transfer rate** = conversations with `nTransferred` > 0 / conversations that reached
  an agent.
- **AHT** = mean `tHandle` per handled agent session (a conversation with two agents is
  two handles).
- **Per queue**: offered / answered / abandoned from acd sessions carrying that queueId;
  handle metrics from agent sessions carrying that queueId.
- **Per agent**: every agent session, including alert-only ones, so NotResponding is
  visible; Handled counts sessions with `tHandle`.
- **Per hour / per date**: by local `conversationStart`, and say which timezone.
- **Poor MOS share** = conversations with MinMos < 3.5 / conversations with MOS data.
