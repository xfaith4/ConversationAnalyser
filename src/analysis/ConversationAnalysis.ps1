<#
.SYNOPSIS
    Conversation analysis helpers for GenesysConvAnalyzer.

.DESCRIPTION
    Pure data functions (no WPF, no network) that turn Genesys Cloud analytics
    conversation detail records into:
      - per-conversation profiles           (Get-ConversationProfile)
      - flat rows for the grid and CSV      (ConvertTo-FlatRow)
      - detail-panel rows                   (Get-Conversation*Rows)
      - collection-level aggregates         (Get-ConversationReport)
      - a self-contained HTML report        (ConvertTo-ConversationReportHtml)

    Units: analytics metrics arrive in milliseconds. Everything surfaced here is in
    seconds unless the column name says otherwise (for example MaxLatencyMs).
    Name lookups (queues, wrap-up codes, divisions, skills, languages) are optional;
    when an ID is not in the lookup table the raw ID is shown instead.

    Compatible with Windows PowerShell 5.1 and PowerShell 7+. Keep this file ASCII-only
    so 5.1 reads it correctly without a BOM.
#>

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

$script:caAgentPurposes = @{ agent = $true; user = $true }

# Agent-work metrics are only summed from agent/user participants so a same-named
# emission on another participant can never double count handle time.
$script:caAgentMetricNames = @{}
foreach ($name in @('tHandle', 'tTalk', 'tTalkComplete', 'tHeld', 'tHeldComplete', 'tAcw', 'tAlert',
        'tNotResponding', 'tDialing', 'tContacting', 'tAgentResponseTime', 'nTransferred',
        'nBlindTransferred', 'nConsultTransferred', 'nConsult', 'nConnected', 'nOutbound')) { $script:caAgentMetricNames[$name] = $true }

# Session-level metrics retained on the profile for queue and agent breakdowns.
# Kept to a whitelist so cached profiles stay small on large result sets.
$script:caSessionMetricNames = @{}
foreach ($name in @('nOffered', 'tAnswered', 'tAbandon', 'tShortAbandon', 'nOverSla', 'tAcd',
        'tHandle', 'tTalk', 'tHeld', 'tAcw', 'tAlert', 'tNotResponding', 'nTransferred', 'nOutbound')) { $script:caSessionMetricNames[$name] = $true }

$script:caBarChar = [string][char]0x2588

# -----------------------------------------------------------------------------
# Small helpers
# -----------------------------------------------------------------------------

function New-ConversationLookupTable {
    return @{ queues = @{}; wrapupCodes = @{}; divisions = @{}; skills = @{}; languages = @{} }
}

function ConvertTo-UtcDateTime {
    # Accepts DateTime (PS7 ConvertFrom-Json), DateTimeOffset, or ISO-8601 text (PS 5.1).
    param([AllowNull()][object]$Value)
    if ($null -eq $Value) { return $null }
    if ($Value -is [DateTime]) {
        if ($Value.Kind -eq [DateTimeKind]::Utc) { return $Value }
        if ($Value.Kind -eq [DateTimeKind]::Local) { return $Value.ToUniversalTime() }
        return [DateTime]::SpecifyKind($Value, [DateTimeKind]::Utc)
    }
    if ($Value -is [DateTimeOffset]) { return $Value.UtcDateTime }
    $text = [string]$Value
    if ([string]::IsNullOrWhiteSpace($text)) { return $null }
    $parsed = [DateTimeOffset]::MinValue
    if ([DateTimeOffset]::TryParse($text, [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::AssumeUniversal, [ref]$parsed)) {
        return $parsed.UtcDateTime
    }
    return $null
}

function Format-LocalTimestamp {
    param([AllowNull()][object]$UtcValue, [string]$Format = 'yyyy-MM-dd HH:mm:ss')
    if ($null -eq $UtcValue) { return '' }
    return ([DateTime]$UtcValue).ToLocalTime().ToString($Format)
}

function ConvertTo-WholeSeconds {
    param([AllowNull()][object]$Milliseconds)
    if ($null -eq $Milliseconds) { return $null }
    return [int][Math]::Round([double]$Milliseconds / 1000.0, [MidpointRounding]::AwayFromZero)
}

function Get-RoundedAverage {
    param([double]$Sum, [int]$Count, [double]$Divisor = 1.0, [int]$Digits = 1)
    if ($Count -le 0) { return $null }
    return [Math]::Round($Sum / $Count / $Divisor, $Digits)
}

function Get-Percent {
    param([double]$Numerator, [double]$Denominator)
    if ($Denominator -le 0) { return $null }
    return [Math]::Round(100.0 * $Numerator / $Denominator, 1)
}

function Get-NearestRankPercentile {
    param([System.Collections.Generic.List[double]]$Values, [double]$Percentile)
    if ($null -eq $Values -or $Values.Count -eq 0) { return $null }
    $sorted = $Values.ToArray()
    [Array]::Sort($sorted)
    $rank = [int][Math]::Ceiling($Percentile * $sorted.Length) - 1
    if ($rank -lt 0) { $rank = 0 }
    return [Math]::Round($sorted[$rank], 1)
}

function Resolve-LookupName {
    param([hashtable]$Lookups, [string]$Kind, [string]$Id)
    if ([string]::IsNullOrEmpty($Id)) { return '' }
    if ($null -ne $Lookups) {
        $map = $Lookups[$Kind]
        if ($null -ne $map -and $map.ContainsKey($Id)) {
            $name = [string]$map[$Id]
            if (-not [string]::IsNullOrWhiteSpace($name)) { return $name }
        }
    }
    return $Id
}

function Resolve-LookupNameList {
    param([hashtable]$Lookups, [string]$Kind, [object[]]$Ids, [string]$Separator = ', ')
    $names = [System.Collections.Generic.List[string]]::new()
    foreach ($id in $Ids) {
        if ([string]::IsNullOrEmpty([string]$id)) { continue }
        $names.Add((Resolve-LookupName -Lookups $Lookups -Kind $Kind -Id ([string]$id))) | Out-Null
    }
    return ($names -join $Separator)
}

function Get-IdsOrderedByTime {
    # $FirstSeen maps id -> earliest UTC DateTime; returns the ids in time order.
    param([hashtable]$FirstSeen)
    if ($null -eq $FirstSeen -or $FirstSeen.Count -eq 0) { return @() }
    $ids = [string[]]::new($FirstSeen.Count)
    $times = [DateTime[]]::new($FirstSeen.Count)
    $i = 0
    foreach ($key in $FirstSeen.Keys) {
        $ids[$i] = [string]$key
        $times[$i] = [DateTime]$FirstSeen[$key]
        $i++
    }
    [Array]::Sort($times, $ids)
    return $ids
}

function Get-MetricTotal {
    # Returns the summed metric value (ms for t*, count for n*) or $null when never emitted.
    param([object]$ConversationProfile, [string]$Name)
    $sums = $ConversationProfile.MetricSums
    if ($null -ne $sums -and $sums.ContainsKey($Name)) { return $sums[$Name] }
    return $null
}

function Get-MetricEmissions {
    param([object]$ConversationProfile, [string]$Name)
    $counts = $ConversationProfile.MetricCounts
    if ($null -ne $counts -and $counts.ContainsKey($Name)) { return [int]$counts[$Name] }
    return 0
}

function Format-SecondsDisplay {
    param([AllowNull()][object]$Seconds)
    if ($null -eq $Seconds) { return 'n/a' }
    $value = [double]$Seconds
    $span = [TimeSpan]::FromSeconds([Math]::Round($value))
    $clock = if ($span.TotalHours -ge 1) { '{0}:{1:00}:{2:00}' -f [int][Math]::Floor($span.TotalHours), $span.Minutes, $span.Seconds } else { '{0}:{1:00}' -f $span.Minutes, $span.Seconds }
    return ('{0:0.#} s ({1})' -f $value, $clock)
}

function Format-OffsetDisplay {
    param([TimeSpan]$Offset)
    $sign = if ($Offset.Ticks -lt 0) { '-' } else { '+' }
    $abs = $Offset.Duration()
    if ($abs.TotalHours -ge 1) { return ('{0}{1}:{2:00}:{3:00}' -f $sign, [int][Math]::Floor($abs.TotalHours), $abs.Minutes, $abs.Seconds) }
    return ('{0}{1:00}:{2:00}.{3}' -f $sign, $abs.Minutes, $abs.Seconds, [int][Math]::Floor($abs.Milliseconds / 100))
}

# -----------------------------------------------------------------------------
# Per-conversation profile
# -----------------------------------------------------------------------------

function Get-ConversationProfile {
    # Single pass over participants/sessions/segments/metrics. Holds IDs, not names, so
    # the cached profile stays valid when name lookups are refreshed later.
    param([Parameter(Mandatory = $true)][object]$Conversation)

    $conv = $Conversation
    $startUtc = ConvertTo-UtcDateTime $conv.conversationStart
    $endUtc = ConvertTo-UtcDateTime $conv.conversationEnd
    $invariant = [System.Globalization.CultureInfo]::InvariantCulture
    $utcStyles = [System.Globalization.DateTimeStyles]::AssumeUniversal -bor [System.Globalization.DateTimeStyles]::AdjustToUniversal
    $parsedTime = [DateTime]::MinValue
    $agentPurposes = $script:caAgentPurposes
    $agentMetricNames = $script:caAgentMetricNames
    $sessionMetricNames = $script:caSessionMetricNames

    $metricSums = @{}
    $metricCounts = @{}
    $sessionFacts = [System.Collections.ArrayList]::new()
    $queueFirstSeen = @{}
    $agentFirstSeen = @{}
    $agentNames = @{}
    $purposeCounts = [ordered]@{}
    $flowNames = [ordered]@{}   # ordered set: key order = first seen
    $firstFlow = $null
    $flowOutcomes = 0
    $flowOutcomeFailures = 0
    $finalWrapUp = $null
    $finalDisconnect = $null
    $fallbackDisconnect = $null
    $errorCodes = [ordered]@{}   # ordered set: key order = first seen
    $sipCodes = [ordered]@{}   # ordered set: key order = first seen
    $q850Codes = [ordered]@{}   # ordered set: key order = first seen
    $skillIds = [ordered]@{}   # ordered set: key order = first seen
    $languageIds = [ordered]@{}   # ordered set: key order = first seen
    $requestedRoutings = [ordered]@{}   # ordered set: key order = first seen
    $usedRouting = ''
    $codecs = [ordered]@{}   # ordered set: key order = first seen
    $maxLatencyMs = $null
    $sessionMinMos = $null
    $recorded = $false
    $holdCount = 0
    $voicemail = $false
    $agentConnected = $false
    $customer = $null
    $external = $null
    $firstMediaType = ''
    $participantCount = 0

    foreach ($p in $conv.participants) {
        $participantCount++
        $purpose = [string]$p.purpose
        if ($purposeCounts.Contains($purpose)) { $purposeCounts[$purpose]++ } else { $purposeCounts[$purpose] = 1 }
        if ($null -eq $customer -and $purpose -eq 'customer') { $customer = $p }
        if ($null -eq $external -and $purpose -eq 'external') { $external = $p }

        $isAgent = [bool]$agentPurposes[$purpose]
        $isAcd = ($purpose -eq 'acd')
        $userId = [string]$p.userId
        $participantName = [string]$p.participantName
        $agentKey = ''
        if ($isAgent) {
            $agentKey = if (-not [string]::IsNullOrEmpty($userId)) { $userId } else { $participantName }
            if ($agentKey -and $participantName -and -not $agentNames.ContainsKey($agentKey)) { $agentNames[$agentKey] = $participantName }
        }

        foreach ($s in $p.sessions) {
            $sessionMedia = [string]$s.mediaType
            if (-not $firstMediaType -and $sessionMedia) { $firstMediaType = $sessionMedia }
            if ($s.recording -eq $true) { $recorded = $true }
            foreach ($routing in $s.requestedRoutings) {
                $routingText = [string]$routing
                if ($routingText) { $requestedRoutings[$routingText] = $true }
            }
            if (-not $usedRouting -and $s.usedRouting) { $usedRouting = [string]$s.usedRouting }

            if ($null -ne $s.flow) {
                $flowName = [string]$s.flow.flowName
                if ($flowName) { $flowNames[$flowName] = $true }
                if ($null -eq $firstFlow) { $firstFlow = $s.flow }
                foreach ($outcome in $s.flow.outcomes) {
                    $flowOutcomes++
                    if ([string]$outcome.flowOutcomeValue -eq 'FAILURE') { $flowOutcomeFailures++ }
                }
            }

            foreach ($stat in $s.mediaEndpointStats) {
                foreach ($codec in $stat.codecs) {
                    $codecText = [string]$codec
                    if ($codecText) { $codecs[$codecText] = $true }
                }
                if ($null -ne $stat.maxLatencyMs -and ($null -eq $maxLatencyMs -or [double]$stat.maxLatencyMs -gt $maxLatencyMs)) { $maxLatencyMs = [double]$stat.maxLatencyMs }
                if ($null -ne $stat.minMos -and ($null -eq $sessionMinMos -or [double]$stat.minMos -lt $sessionMinMos)) { $sessionMinMos = [double]$stat.minMos }
            }

            $keepSession = ($isAgent -or $isAcd)
            $sessionMetrics = if ($keepSession) { @{} } else { $null }
            foreach ($m in $s.metrics) {
                $metricName = [string]$m.name
                if (-not $metricName -or $null -eq $m.value) { continue }
                $metricValue = [double]$m.value
                if ($keepSession -and $sessionMetricNames[$metricName]) {
                    if ($sessionMetrics.ContainsKey($metricName)) { $sessionMetrics[$metricName] += $metricValue } else { $sessionMetrics[$metricName] = $metricValue }
                }
                if ($isAgent -or -not $agentMetricNames[$metricName]) {
                    if ($metricSums.ContainsKey($metricName)) {
                        $metricSums[$metricName] += $metricValue
                        $metricCounts[$metricName]++
                    }
                    else {
                        $metricSums[$metricName] = $metricValue
                        $metricCounts[$metricName] = 1
                    }
                }
            }

            $sessionQueueId = ''
            foreach ($seg in $s.segments) {
                $segType = [string]$seg.segmentType
                # Inline conversion (a function call per segment is the dominant cost at scale).
                # PS7 JSON already yields DateTime; PS 5.1 yields ISO-8601 text.
                $segStart = $seg.segmentStart
                if ($null -ne $segStart -and $segStart -isnot [DateTime]) {
                    $segStart = if ([DateTime]::TryParse([string]$segStart, $invariant, $utcStyles, [ref]$parsedTime)) { $parsedTime } else { $null }
                }
                $segEnd = $seg.segmentEnd
                if ($null -ne $segEnd -and $segEnd -isnot [DateTime]) {
                    $segEnd = if ([DateTime]::TryParse([string]$segEnd, $invariant, $utcStyles, [ref]$parsedTime)) { $parsedTime } else { $null }
                }
                $orderKey = if ($null -ne $segStart) { $segStart } else { [DateTime]::MaxValue }
                $eventTime = if ($null -ne $segEnd) { $segEnd } else { $orderKey }

                $queueId = [string]$seg.queueId
                if ($queueId) {
                    if (-not $sessionQueueId) { $sessionQueueId = $queueId }
                    if (-not $queueFirstSeen.ContainsKey($queueId) -or $orderKey -lt $queueFirstSeen[$queueId]) { $queueFirstSeen[$queueId] = $orderKey }
                }

                if ($isAgent) {
                    # Agents that were only alerted (never engaged) are not part of the agent path.
                    if ($agentKey -and $segType -ne 'alert') {
                        if (-not $agentFirstSeen.ContainsKey($agentKey) -or $orderKey -lt $agentFirstSeen[$agentKey]) { $agentFirstSeen[$agentKey] = $orderKey }
                    }
                    if ($segType -eq 'hold') { $holdCount++ }
                    elseif ($segType -eq 'interact') { $agentConnected = $true }
                }
                if ($segType -eq 'voicemail') { $voicemail = $true }

                $wrapCode = [string]$seg.wrapUpCode
                if ($wrapCode -and ($null -eq $finalWrapUp -or $eventTime -ge $finalWrapUp.Time)) {
                    $finalWrapUp = @{ Code = $wrapCode; Note = [string]$seg.wrapUpNote; Time = $eventTime }
                }

                # Conversation-ending disconnect: the latest non-peer, non-transfer disconnect
                # outside wrap-up. Early declined alerts or transfers therefore never win.
                $disconnectType = [string]$seg.disconnectType
                if ($disconnectType -and $segType -ne 'wrapup') {
                    $candidate = @{ Purpose = $purpose; Type = $disconnectType; Time = $eventTime }
                    if ($null -eq $fallbackDisconnect -or $eventTime -ge $fallbackDisconnect.Time) { $fallbackDisconnect = $candidate }
                    if ($disconnectType -ne 'peer' -and $disconnectType -notlike '*transfer*') {
                        if ($null -eq $finalDisconnect -or $eventTime -ge $finalDisconnect.Time) { $finalDisconnect = $candidate }
                    }
                }

                $errorCode = [string]$seg.errorCode
                if ($errorCode) { $errorCodes[$errorCode] = $true }
                foreach ($code in $seg.sipResponseCodes) {
                    $codeText = [string]$code
                    if ($codeText) { $sipCodes[$codeText] = $true }
                }
                foreach ($code in $seg.q850ResponseCodes) {
                    $codeText = [string]$code
                    if ($codeText) { $q850Codes[$codeText] = $true }
                }
                foreach ($skillId in $seg.requestedRoutingSkillIds) {
                    $skillText = [string]$skillId
                    if ($skillText) { $skillIds[$skillText] = $true }
                }
                $languageId = [string]$seg.requestedLanguageId
                if ($languageId) { $languageIds[$languageId] = $true }
            }

            if ($keepSession) {
                $sessionFacts.Add(@{
                        Purpose   = $purpose
                        AgentKey  = $agentKey
                        QueueId   = $sessionQueueId
                        MediaType = $sessionMedia
                        Metrics   = $sessionMetrics
                    }) | Out-Null
            }
        }
    }

    # Wrap the whole if in @(): assigning an if-statement's output unrolls a 1-item array.
    $queueIds = @(if ($queueFirstSeen.Count -gt 1) { Get-IdsOrderedByTime -FirstSeen $queueFirstSeen } else { $queueFirstSeen.Keys })
    $agentKeys = @(if ($agentFirstSeen.Count -gt 1) { Get-IdsOrderedByTime -FirstSeen $agentFirstSeen } else { $agentFirstSeen.Keys })

    # Customer identity (outbound agent calls may only have an 'external' party)
    $party = if ($null -ne $customer) { $customer } else { $external }
    $ani = ''; $dnis = ''; $partyMedia = ''
    if ($null -ne $party) {
        foreach ($s in $party.sessions) {
            if (-not $partyMedia -and $s.mediaType) { $partyMedia = [string]$s.mediaType }
            if (-not $ani) { $ani = if ($s.ani) { [string]$s.ani } else { [string]$s.addressFrom } }
            if (-not $dnis) { $dnis = if ($s.dnis) { [string]$s.dnis } else { [string]$s.addressTo } }
        }
    }

    $offered = ($metricSums.ContainsKey('nOffered') -and $metricSums['nOffered'] -gt 0)
    $transferTotal = 0
    foreach ($name in @('nTransferred', 'nBlindTransferred', 'nConsultTransferred')) {
        if ($metricSums.ContainsKey($name)) { $transferTotal = [Math]::Max($transferTotal, [int]$metricSums[$name]) }
    }

    $selfServed = $false
    if ($conv.PSObject.Properties.Name -contains 'selfServed' -and $null -ne $conv.selfServed) {
        $selfServed = [bool]$conv.selfServed
    }
    else {
        $selfServed = ($flowNames.Count -gt 0 -and -not $offered -and $agentKeys.Count -eq 0 -and -not $voicemail)
    }

    $minMos = if ($null -ne $conv.mediaStatsMinConversationMos) { [Math]::Round([double]$conv.mediaStatsMinConversationMos, 2) } elseif ($null -ne $sessionMinMos) { [Math]::Round($sessionMinMos, 2) } else { $null }
    $minRFactor = if ($null -ne $conv.mediaStatsMinConversationRFactor) { [Math]::Round([double]$conv.mediaStatsMinConversationRFactor, 1) } else { $null }

    $evalCount = 0; $evalScoreSum = 0.0; $evalScoreCount = 0
    foreach ($evaluation in $conv.evaluations) {
        $evalCount++
        if ($null -ne $evaluation.oTotalScore) { $evalScoreSum += [double]$evaluation.oTotalScore; $evalScoreCount++ }
    }
    $surveyCount = 0; $surveyScoreSum = 0.0; $surveyScoreCount = 0; $npsSum = 0.0; $npsCount = 0
    foreach ($survey in $conv.surveys) {
        $surveyCount++
        if ($null -ne $survey.oSurveyTotalScore) { $surveyScoreSum += [double]$survey.oSurveyTotalScore; $surveyScoreCount++ }
        if ($null -ne $survey.surveyPromoterScore) { $npsSum += [double]$survey.surveyPromoterScore; $npsCount++ }
    }
    $resolutionCount = 0
    foreach ($resolution in $conv.resolutions) { if ($null -ne $resolution) { $resolutionCount++ } }

    $divisionIds = [System.Collections.ArrayList]::new()
    foreach ($divisionId in $conv.divisionIds) { if ($divisionId) { $divisionIds.Add([string]$divisionId) | Out-Null } }

    $durationSec = $null
    if ($null -ne $startUtc -and $null -ne $endUtc) { $durationSec = [int][Math]::Round(($endUtc - $startUtc).TotalSeconds, [MidpointRounding]::AwayFromZero) }

    $purposeParts = [System.Collections.ArrayList]::new()
    foreach ($entry in $purposeCounts.GetEnumerator()) { $purposeParts.Add(('{0}:{1}' -f $entry.Key, $entry.Value)) | Out-Null }
    $purposeSummary = $purposeParts -join ', '

    return [pscustomobject]@{
        ConversationId        = [string]$conv.conversationId
        StartUtc              = $startUtc
        EndUtc                = $endUtc
        StartLocal            = if ($null -ne $startUtc) { $startUtc.ToLocalTime() } else { $null }
        DurationSec           = $durationSec
        Direction             = [string]$conv.originatingDirection
        MediaType             = if ($partyMedia) { $partyMedia } else { $firstMediaType }
        Initiator             = [string]$conv.conversationInitiator
        CustomerParticipation = if ($null -ne $conv.customerParticipation) { [bool]$conv.customerParticipation } else { $null }
        ExternalTag           = [string]$conv.externalTag
        DivisionIds           = @($divisionIds)
        CustomerName          = if ($null -ne $party) { [string]$party.participantName } else { '' }
        Ani                   = $ani
        Dnis                  = $dnis
        ExternalContactId     = if ($null -ne $party) { [string]$party.externalContactId } else { '' }
        ParticipantCount      = $participantCount
        PurposeSummary        = $purposeSummary
        QueueIds              = $queueIds
        AgentKeys             = $agentKeys
        AgentNames            = $agentNames
        RequestedRoutings     = @($requestedRoutings.Keys)
        UsedRouting           = $usedRouting
        SkillIds              = @($skillIds.Keys)
        LanguageIds           = @($languageIds.Keys)
        FlowNames             = @($flowNames.Keys)
        FlowType              = if ($null -ne $firstFlow) { [string]$firstFlow.flowType } else { '' }
        FlowEntryReason       = if ($null -ne $firstFlow) { [string]$firstFlow.entryReason } else { '' }
        FlowExitReason        = if ($null -ne $firstFlow) { [string]$firstFlow.exitReason } else { '' }
        FlowOutcomes          = $flowOutcomes
        FlowOutcomeFailures   = $flowOutcomeFailures
        WrapUpCodeId          = if ($null -ne $finalWrapUp) { $finalWrapUp.Code } else { '' }
        WrapUpNote            = if ($null -ne $finalWrapUp) { $finalWrapUp.Note } else { '' }
        DisconnectPurpose     = if ($null -ne $finalDisconnect) { $finalDisconnect.Purpose } elseif ($null -ne $fallbackDisconnect) { $fallbackDisconnect.Purpose } else { '' }
        DisconnectType        = if ($null -ne $finalDisconnect) { $finalDisconnect.Type } elseif ($null -ne $fallbackDisconnect) { $fallbackDisconnect.Type } else { '' }
        MetricSums            = $metricSums
        MetricCounts          = $metricCounts
        Offered               = $offered
        Answered              = $metricCounts.ContainsKey('tAnswered')
        Abandoned             = $metricCounts.ContainsKey('tAbandon')
        ShortAbandon          = $metricCounts.ContainsKey('tShortAbandon')
        OverSla               = ($metricSums.ContainsKey('nOverSla') -and $metricSums['nOverSla'] -gt 0)
        AgentConnected        = $agentConnected
        TransferCount         = $transferTotal
        Transferred           = ($transferTotal -gt 0)
        HoldCount             = $holdCount
        Voicemail             = ($voicemail -or $metricCounts.ContainsKey('tVoicemail'))
        SelfServed            = $selfServed
        MinMos                = $minMos
        MinRFactor            = $minRFactor
        Codecs                = @($codecs.Keys)
        MaxLatencyMs          = $maxLatencyMs
        Recorded              = $recorded
        EvaluationCount       = $evalCount
        EvalScore             = if ($evalScoreCount -gt 0) { [Math]::Round($evalScoreSum / $evalScoreCount, 1) } else { $null }
        SurveyCount           = $surveyCount
        SurveyScore           = if ($surveyScoreCount -gt 0) { [Math]::Round($surveyScoreSum / $surveyScoreCount, 1) } else { $null }
        NpsScore              = if ($npsCount -gt 0) { [Math]::Round($npsSum / $npsCount, 1) } else { $null }
        ResolutionCount       = $resolutionCount
        ErrorCodes            = @($errorCodes.Keys)
        SipCodes              = @($sipCodes.Keys)
        Q850Codes             = @($q850Codes.Keys)
        Sessions              = $sessionFacts
    }
}

# -----------------------------------------------------------------------------
# Flat row (grid + CSV)
# -----------------------------------------------------------------------------

# Column order and descriptions for ConvertTo-FlatRow. Descriptions drive grid header
# tooltips, the column selector, and the README column reference.
$script:caColumnDescriptions = [ordered]@{
    ConversationId        = 'Genesys Cloud conversation ID.'
    Start                 = 'Conversation start (local time).'
    End                   = 'Conversation end (local time); blank while still active.'
    StartDate             = 'Local start date (yyyy-MM-dd), for pivoting.'
    StartHour             = 'Local start hour (0-23), for pivoting.'
    DayOfWeek             = 'Local start day of week.'
    DurationSec           = 'Conversation start to end, seconds.'
    Direction             = 'Originating direction (inbound / outbound).'
    MediaType             = 'Primary media type (customer session, else first session).'
    Initiator             = 'Who initiated the conversation (conversationInitiator).'
    CustomerParticipation = 'True when a customer took part.'
    QueueId               = 'First queue ID the conversation entered.'
    QueueName             = 'First queue (name when resolved, else ID).'
    FinalQueue            = 'Last queue the conversation entered.'
    QueuePath             = 'Every queue entered, in order.'
    QueueCount            = 'Number of distinct queues entered.'
    RequestedRouting      = 'Routing methods requested (Standard, Bullseye, Preferred, ...).'
    UsedRouting           = 'Routing method that actually delivered the conversation.'
    RequestedSkills       = 'Requested ACD skills (names when resolved).'
    RequestedLanguage     = 'Requested ACD language (name when resolved).'
    AgentName             = 'First agent who engaged (alert-only agents excluded).'
    AgentUserId           = 'User ID of the first engaged agent.'
    FinalAgent            = 'Last agent who engaged.'
    AgentPath             = 'Every engaged agent, in order.'
    AgentCount            = 'Number of distinct engaged agents.'
    CustomerName          = 'Customer (or external) participant name. Redacted in exports.'
    Ani                   = 'Customer session ANI / from-address. Redacted in exports.'
    Dnis                  = 'Customer session DNIS / to-address. Redacted in exports.'
    ExternalContactId     = 'External contact ID. Redacted in exports.'
    ExternalTag           = 'Conversation external tag.'
    Offered               = 'True when offered to a queue (nOffered).'
    Answered              = 'True when answered from a queue (tAnswered emitted).'
    Abandoned             = 'True when abandoned in queue (tAbandon emitted).'
    ShortAbandon          = 'True when abandoned inside the short-abandon threshold.'
    OverSla               = 'True when answered or abandoned outside the queue service level (nOverSla).'
    AgentConnected        = 'True when an agent interacted with the customer.'
    Transferred           = 'True when at least one transfer occurred.'
    TransferCount         = 'Number of transfers (nTransferred).'
    BlindTransfers        = 'Blind transfers (nBlindTransferred).'
    ConsultTransfers      = 'Consult transfers (nConsultTransferred).'
    Consults              = 'Consults started (nConsult).'
    HoldCount             = 'Number of agent hold segments.'
    Voicemail             = 'True when the conversation reached voicemail.'
    SelfServed            = 'Genesys selfServed flag, or flow-only with no queue, agent, or voicemail.'
    WrapUpCode            = 'Final wrap-up code (name when resolved).'
    WrapUpNote            = 'Final wrap-up note. Redacted in exports.'
    DisconnectedBy        = 'Participant purpose that ended the conversation (customer, agent, acd, ivr, ...).'
    DisconnectType        = 'How it ended: endpoint (hung up), client (agent UI), system, error, timeout, ...'
    FlowName              = 'First Architect flow the conversation entered.'
    FlowPath              = 'Every flow entered, in order.'
    FlowType              = 'Type of the first flow (INBOUNDCALL, INBOUNDCHAT, ...).'
    FlowExitReason        = 'Why the conversation left the first flow.'
    FlowOutcomes          = 'Flow outcomes recorded.'
    FlowOutcomeFailures   = 'Flow outcomes recorded as FAILURE.'
    tAnsweredSec          = 'Queue wait before answer (speed of answer), seconds.'
    tAbandonSec           = 'Queue wait before abandon, seconds.'
    tAcdSec               = 'Total time in queue (ACD), seconds.'
    tWaitSec              = 'Total wait time (tWait), seconds.'
    tIvrSec               = 'Time in IVR, seconds.'
    tFlowSec              = 'Time in flows (tFlow), seconds.'
    tAlertSec             = 'Agent alerting (ringing) time before answer, seconds.'
    tNotRespondingSec     = 'Agent alerting time that went unanswered, seconds.'
    tDialingSec           = 'Outbound dialing time, seconds.'
    tContactingSec        = 'Outbound contacting time, seconds.'
    tTalkSec              = 'Agent talk time, seconds.'
    tHeldSec              = 'Agent hold time, seconds.'
    tAcwSec               = 'After-call work (wrap-up) time, seconds.'
    tHandleSec            = 'Handle time (talk + hold + ACW), seconds.'
    tVoicemailSec         = 'Time in voicemail, seconds.'
    AvgUserResponseSec    = 'Average customer response time per message turn, seconds (messaging).'
    AvgAgentResponseSec   = 'Average agent response time per message turn, seconds (messaging).'
    nConnected            = 'Agent connected count (nConnected).'
    nOffered              = 'Queue offer count (nOffered).'
    nOverSla              = 'Queue service-level breaches (nOverSla).'
    nOutbound             = 'Outbound attempts by agents (nOutbound).'
    nError                = 'Error count (nError).'
    MinMos                = 'Lowest MOS (voice quality, 1-5) seen in the conversation.'
    MinRFactor            = 'Lowest R-factor seen in the conversation.'
    Codecs                = 'Audio codecs used.'
    MaxLatencyMs          = 'Highest media latency, milliseconds.'
    Recorded              = 'True when any session was recorded.'
    Evaluations           = 'Quality evaluations linked to the conversation.'
    EvalScore             = 'Average evaluation total score.'
    Surveys               = 'Surveys linked to the conversation.'
    SurveyScore           = 'Average survey total score.'
    NpsScore              = 'Average survey promoter (NPS) score, 0-10.'
    Resolutions           = 'Resolution records linked to the conversation.'
    ErrorCodes            = 'Segment error codes.'
    SipCodes              = 'SIP response codes.'
    Q850Codes             = 'Q.850 cause codes.'
    ParticipantCount      = 'Number of participants.'
    Participants          = 'Participant purposes with counts.'
    Divisions             = 'Divisions (names when resolved).'
}

$script:caDefaultGridColumns = @(
    'ConversationId', 'Start', 'DurationSec', 'Direction', 'MediaType', 'QueueName', 'AgentName',
    'WrapUpCode', 'DisconnectedBy', 'Answered', 'Abandoned', 'Transferred', 'HoldCount',
    'tAnsweredSec', 'tTalkSec', 'tHeldSec', 'tAcwSec', 'tHandleSec', 'MinMos')

function Get-FlatRowColumnNames { return @($script:caColumnDescriptions.Keys) }

function Get-DefaultGridColumnNames { return @($script:caDefaultGridColumns) }

function Get-ColumnDescription {
    param([string]$Name)
    if ($script:caColumnDescriptions.Contains($Name)) { return [string]$script:caColumnDescriptions[$Name] }
    return ''
}

function Get-ConversationAttributeMap {
    # Participant attributes merged across participants; the customer's values win.
    param([object]$Conversation)
    $map = @{}
    $ordered = [System.Collections.Generic.List[object]]::new()
    foreach ($p in $Conversation.participants) { if ([string]$p.purpose -eq 'customer') { $ordered.Add($p) | Out-Null } }
    foreach ($p in $Conversation.participants) { if ([string]$p.purpose -ne 'customer') { $ordered.Add($p) | Out-Null } }
    foreach ($p in $ordered) {
        $attrs = $p.attributes
        if ($null -eq $attrs) { continue }
        if ($attrs -is [System.Collections.IDictionary]) {
            foreach ($key in $attrs.Keys) { if (-not $map.ContainsKey([string]$key)) { $map[[string]$key] = [string]$attrs[$key] } }
        }
        else {
            foreach ($prop in $attrs.PSObject.Properties) { if (-not $map.ContainsKey($prop.Name)) { $map[$prop.Name] = [string]$prop.Value } }
        }
    }
    return $map
}

function Get-ConversationAttributeKeys {
    param([object[]]$Conversations)
    $keys = [System.Collections.Generic.HashSet[string]]::new()
    foreach ($conv in $Conversations) {
        foreach ($p in $conv.participants) {
            $attrs = $p.attributes
            if ($null -eq $attrs) { continue }
            if ($attrs -is [System.Collections.IDictionary]) { foreach ($key in $attrs.Keys) { $keys.Add([string]$key) | Out-Null } }
            else { foreach ($prop in $attrs.PSObject.Properties) { $keys.Add($prop.Name) | Out-Null } }
        }
    }
    $sorted = [string[]]::new($keys.Count)
    $keys.CopyTo($sorted)
    [Array]::Sort($sorted, [System.StringComparer]::OrdinalIgnoreCase)
    return $sorted
}

function ConvertTo-FlatRow {
    # -ForGrid names attribute properties Attr0..AttrN (WPF binding paths cannot contain
    # dots or brackets); CSV rows keep the readable 'A:<key>' names.
    param(
        [Parameter(Mandatory = $true)][object]$ConversationProfile,
        [object]$Conversation,
        [string[]]$AttrCols,
        [hashtable]$Lookups,
        [switch]$ForGrid
    )

    $cp = $ConversationProfile
    $sums = $cp.MetricSums
    $counts = $cp.MetricCounts
    $firstQueueId = if ($cp.QueueIds.Count -gt 0) { $cp.QueueIds[0] } else { '' }
    $firstAgentKey = if ($cp.AgentKeys.Count -gt 0) { $cp.AgentKeys[0] } else { '' }
    $finalAgentKey = if ($cp.AgentKeys.Count -gt 0) { $cp.AgentKeys[$cp.AgentKeys.Count - 1] } else { '' }
    $agentLabels = @(foreach ($key in $cp.AgentKeys) { if ($cp.AgentNames.ContainsKey($key)) { [string]$cp.AgentNames[$key] } else { $key } })

    # Precomputed once per row; a missing key yields $null (metric never emitted).
    $secs = @{}
    $nums = @{}
    foreach ($entry in $sums.GetEnumerator()) {
        $secs[$entry.Key] = [int][Math]::Round([double]$entry.Value / 1000.0, [MidpointRounding]::AwayFromZero)
        $nums[$entry.Key] = [int]$entry.Value
    }
    $userResponse = if ($counts.ContainsKey('tUserResponseTime')) { [Math]::Round($sums['tUserResponseTime'] / $counts['tUserResponseTime'] / 1000.0, 1) } else { $null }
    $agentResponse = if ($counts.ContainsKey('tAgentResponseTime')) { [Math]::Round($sums['tAgentResponseTime'] / $counts['tAgentResponseTime'] / 1000.0, 1) } else { $null }
    $startLocal = $cp.StartLocal

    # Name resolution is inlined: this runs once per grid/CSV row and a function call
    # per lookup costs more than everything else in the row combined.
    $queueNames = $null; $wrapNames = $null; $skillNames = $null; $languageNames = $null; $divisionNames = $null
    if ($null -ne $Lookups) {
        $queueNames = $Lookups['queues']; $wrapNames = $Lookups['wrapupCodes']; $skillNames = $Lookups['skills']
        $languageNames = $Lookups['languages']; $divisionNames = $Lookups['divisions']
    }
    $queueLabels = @(foreach ($id in $cp.QueueIds) { if ($null -ne $queueNames -and $queueNames.ContainsKey($id)) { $queueNames[$id] } else { $id } })
    $skillLabels = @(foreach ($id in $cp.SkillIds) { if ($null -ne $skillNames -and $skillNames.ContainsKey($id)) { $skillNames[$id] } else { $id } })
    $languageLabels = @(foreach ($id in $cp.LanguageIds) { if ($null -ne $languageNames -and $languageNames.ContainsKey($id)) { $languageNames[$id] } else { $id } })
    $divisionLabels = @(foreach ($id in $cp.DivisionIds) { if ($null -ne $divisionNames -and $divisionNames.ContainsKey($id)) { $divisionNames[$id] } else { $id } })
    $wrapId = $cp.WrapUpCodeId
    $wrapLabel = if ($wrapId -and $null -ne $wrapNames -and $wrapNames.ContainsKey($wrapId)) { $wrapNames[$wrapId] } else { $wrapId }

    $row = [pscustomobject]@{
        ConversationId        = $cp.ConversationId
        Start                 = if ($null -ne $cp.StartUtc) { $cp.StartUtc.ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss') } else { '' }
        End                   = if ($null -ne $cp.EndUtc) { $cp.EndUtc.ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss') } else { '' }
        StartDate             = if ($null -ne $startLocal) { $startLocal.ToString('yyyy-MM-dd') } else { '' }
        StartHour             = if ($null -ne $startLocal) { [int]$startLocal.Hour } else { $null }
        DayOfWeek             = if ($null -ne $startLocal) { [string]$startLocal.DayOfWeek } else { '' }
        DurationSec           = $cp.DurationSec
        Direction             = $cp.Direction
        MediaType             = $cp.MediaType
        Initiator             = $cp.Initiator
        CustomerParticipation = $cp.CustomerParticipation
        QueueId               = $firstQueueId
        QueueName             = if ($queueLabels.Count -gt 0) { $queueLabels[0] } else { '' }
        FinalQueue            = if ($queueLabels.Count -gt 0) { $queueLabels[$queueLabels.Count - 1] } else { '' }
        QueuePath             = $queueLabels -join ' > '
        QueueCount            = $cp.QueueIds.Count
        RequestedRouting      = $cp.RequestedRoutings -join ', '
        UsedRouting           = $cp.UsedRouting
        RequestedSkills       = $skillLabels -join ', '
        RequestedLanguage     = $languageLabels -join ', '
        AgentName             = if ($firstAgentKey -and $cp.AgentNames.ContainsKey($firstAgentKey)) { [string]$cp.AgentNames[$firstAgentKey] } else { $firstAgentKey }
        AgentUserId           = $firstAgentKey
        FinalAgent            = if ($finalAgentKey -and $cp.AgentNames.ContainsKey($finalAgentKey)) { [string]$cp.AgentNames[$finalAgentKey] } else { $finalAgentKey }
        AgentPath             = $agentLabels -join ' > '
        AgentCount            = $cp.AgentKeys.Count
        CustomerName          = $cp.CustomerName
        Ani                   = $cp.Ani
        Dnis                  = $cp.Dnis
        ExternalContactId     = $cp.ExternalContactId
        ExternalTag           = $cp.ExternalTag
        Offered               = $cp.Offered
        Answered              = $cp.Answered
        Abandoned             = $cp.Abandoned
        ShortAbandon          = $cp.ShortAbandon
        OverSla               = $cp.OverSla
        AgentConnected        = $cp.AgentConnected
        Transferred           = $cp.Transferred
        TransferCount         = $cp.TransferCount
        BlindTransfers        = $nums['nBlindTransferred']
        ConsultTransfers      = $nums['nConsultTransferred']
        Consults              = $nums['nConsult']
        HoldCount             = $cp.HoldCount
        Voicemail             = $cp.Voicemail
        SelfServed            = $cp.SelfServed
        WrapUpCode            = $wrapLabel
        WrapUpNote            = $cp.WrapUpNote
        DisconnectedBy        = $cp.DisconnectPurpose
        DisconnectType        = $cp.DisconnectType
        FlowName              = if ($cp.FlowNames.Count -gt 0) { $cp.FlowNames[0] } else { '' }
        FlowPath              = $cp.FlowNames -join ' > '
        FlowType              = $cp.FlowType
        FlowExitReason        = $cp.FlowExitReason
        FlowOutcomes          = $cp.FlowOutcomes
        FlowOutcomeFailures   = $cp.FlowOutcomeFailures
        tAnsweredSec          = $secs['tAnswered']
        tAbandonSec           = $secs['tAbandon']
        tAcdSec               = $secs['tAcd']
        tWaitSec              = $secs['tWait']
        tIvrSec               = $secs['tIvr']
        tFlowSec              = $secs['tFlow']
        tAlertSec             = $secs['tAlert']
        tNotRespondingSec     = $secs['tNotResponding']
        tDialingSec           = $secs['tDialing']
        tContactingSec        = $secs['tContacting']
        tTalkSec              = $secs['tTalk']
        tHeldSec              = $secs['tHeld']
        tAcwSec               = $secs['tAcw']
        tHandleSec            = $secs['tHandle']
        tVoicemailSec         = $secs['tVoicemail']
        AvgUserResponseSec    = $userResponse
        AvgAgentResponseSec   = $agentResponse
        nConnected            = $nums['nConnected']
        nOffered              = $nums['nOffered']
        nOverSla              = $nums['nOverSla']
        nOutbound             = $nums['nOutbound']
        nError                = $nums['nError']
        MinMos                = $cp.MinMos
        MinRFactor            = $cp.MinRFactor
        Codecs                = $cp.Codecs -join ', '
        MaxLatencyMs          = $cp.MaxLatencyMs
        Recorded              = $cp.Recorded
        Evaluations           = $cp.EvaluationCount
        EvalScore             = $cp.EvalScore
        Surveys               = $cp.SurveyCount
        SurveyScore           = $cp.SurveyScore
        NpsScore              = $cp.NpsScore
        Resolutions           = $cp.ResolutionCount
        ErrorCodes            = $cp.ErrorCodes -join ', '
        SipCodes              = $cp.SipCodes -join ', '
        Q850Codes             = $cp.Q850Codes -join ', '
        ParticipantCount      = $cp.ParticipantCount
        Participants          = $cp.PurposeSummary
        Divisions             = $divisionLabels -join ', '
    }

    $attrList = @($AttrCols)
    if ($attrList.Count -gt 0) {
        # Look up only the requested keys (customer attributes win, as in
        # Get-ConversationAttributeMap) instead of materialising every attribute per row.
        $attrSources = [System.Collections.ArrayList]::new()
        if ($null -ne $Conversation) {
            foreach ($p in $Conversation.participants) { if ($null -ne $p.attributes -and [string]$p.purpose -eq 'customer') { [void]$attrSources.Add($p.attributes) } }
            foreach ($p in $Conversation.participants) { if ($null -ne $p.attributes -and [string]$p.purpose -ne 'customer') { [void]$attrSources.Add($p.attributes) } }
        }
        for ($i = 0; $i -lt $attrList.Count; $i++) {
            $key = [string]$attrList[$i]
            $value = ''
            foreach ($attrs in $attrSources) {
                if ($attrs -is [System.Collections.IDictionary]) {
                    if ($attrs.Contains($key)) { $value = [string]$attrs[$key]; break }
                }
                else {
                    $attrProp = $attrs.PSObject.Properties[$key]
                    if ($null -ne $attrProp) { $value = [string]$attrProp.Value; break }
                }
            }
            $propertyName = if ($ForGrid) { "Attr$i" } else { "A:$key" }
            $row.PSObject.Properties.Add([System.Management.Automation.PSNoteProperty]::new($propertyName, $value))
        }
    }

    return $row
}

# -----------------------------------------------------------------------------
# Detail-panel rows (single conversation)
# -----------------------------------------------------------------------------

function Get-ConversationOverviewFields {
    # Returns an ordered label -> display text map for the Overview tiles.
    param([object]$ConversationProfile, [hashtable]$Lookups)
    $cp = $ConversationProfile
    $row = ConvertTo-FlatRow -ConversationProfile $cp -Lookups $Lookups
    $yesNo = { param($v) if ($v) { 'Yes' } else { 'No' } }
    $orDash = { param($v) if ($null -eq $v -or [string]$v -eq '') { '-' } else { [string]$v } }

    $outcome = [System.Collections.Generic.List[string]]::new()
    if ($cp.Offered) { $outcome.Add('Offered') | Out-Null }
    if ($cp.Answered) { $outcome.Add('Answered') | Out-Null }
    if ($cp.Abandoned) { $outcome.Add($(if ($cp.ShortAbandon) { 'Short abandon' } else { 'Abandoned' })) | Out-Null }
    if ($cp.OverSla) { $outcome.Add('Over SLA') | Out-Null }
    if ($cp.Transferred) { $outcome.Add("Transferred x$($cp.TransferCount)") | Out-Null }
    if ($cp.HoldCount -gt 0) { $outcome.Add("Held x$($cp.HoldCount)") | Out-Null }
    if ($cp.Voicemail) { $outcome.Add('Voicemail') | Out-Null }
    if ($cp.SelfServed) { $outcome.Add('Self-served') | Out-Null }

    $disconnect = if ($cp.DisconnectPurpose) { "$($cp.DisconnectPurpose) ($($cp.DisconnectType))" } else { '-' }
    $evaluation = if ($cp.EvaluationCount -gt 0) { "$($cp.EvaluationCount) (avg score $(& $orDash $cp.EvalScore))" } else { 'None' }
    $survey = if ($cp.SurveyCount -gt 0) { "$($cp.SurveyCount) (score $(& $orDash $cp.SurveyScore), NPS $(& $orDash $cp.NpsScore))" } else { 'None' }
    $quality = if ($null -ne $cp.MinMos) { "MOS $($cp.MinMos) / R $(& $orDash $cp.MinRFactor)" } else { '-' }

    return [ordered]@{
        'Conversation ID'  = $cp.ConversationId
        'Start (local)'    = & $orDash $row.Start
        'End (local)'      = & $orDash $row.End
        'Duration'         = Format-SecondsDisplay $cp.DurationSec
        'Direction'        = & $orDash $cp.Direction
        'Media'            = & $orDash $cp.MediaType
        'Initiator'        = & $orDash $cp.Initiator
        'Outcome'          = if ($outcome.Count -gt 0) { $outcome -join ', ' } else { '-' }
        'Queue path'       = & $orDash $row.QueuePath
        'Agent path'       = & $orDash $row.AgentPath
        'Wrap-up'          = & $orDash $row.WrapUpCode
        'Wrap-up note'     = & $orDash $cp.WrapUpNote
        'Disconnected by'  = $disconnect
        'Customer'         = & $orDash $cp.CustomerName
        'ANI'              = & $orDash $cp.Ani
        'DNIS'             = & $orDash $cp.Dnis
        'Flow path'        = & $orDash $row.FlowPath
        'Flow exit'        = & $orDash $cp.FlowExitReason
        'Routing'          = & $orDash ((@($row.RequestedRouting, $row.UsedRouting) | Where-Object { $_ }) -join ' -> ')
        'Skills'           = & $orDash $row.RequestedSkills
        'Speed of answer'  = Format-SecondsDisplay $row.tAnsweredSec
        'Queue time'       = Format-SecondsDisplay $row.tAcdSec
        'IVR time'         = Format-SecondsDisplay $row.tIvrSec
        'Alert time'       = Format-SecondsDisplay $row.tAlertSec
        'Talk'             = Format-SecondsDisplay $row.tTalkSec
        'Hold'             = Format-SecondsDisplay $row.tHeldSec
        'ACW'              = Format-SecondsDisplay $row.tAcwSec
        'Handle'           = Format-SecondsDisplay $row.tHandleSec
        'Voice quality'    = $quality
        'Codecs'           = & $orDash $row.Codecs
        'Max latency (ms)' = & $orDash $cp.MaxLatencyMs
        'Recorded'         = & $yesNo $cp.Recorded
        'Evaluations'      = $evaluation
        'Surveys'          = $survey
        'Divisions'        = & $orDash $row.Divisions
        'External tag'     = & $orDash $cp.ExternalTag
        'Error / SIP'      = & $orDash ((@($row.ErrorCodes, $row.SipCodes, $row.Q850Codes) | Where-Object { $_ }) -join ' | ')
        'Participants'     = & $orDash $cp.PurposeSummary
    }
}

function Get-SessionMetricMap {
    param([object]$Session)
    $map = @{}
    foreach ($m in $Session.metrics) {
        $name = [string]$m.name
        if (-not $name -or $null -eq $m.value) { continue }
        if ($map.ContainsKey($name)) { $map[$name] += [double]$m.value } else { $map[$name] = [double]$m.value }
    }
    return $map
}

function Get-ConversationSessionRows {
    param([object]$Conversation, [hashtable]$Lookups)
    $rows = [System.Collections.Generic.List[object]]::new()
    $participantIndex = 0
    foreach ($p in $Conversation.participants) {
        $participantIndex++
        foreach ($s in $p.sessions) {
            $metrics = Get-SessionMetricMap -Session $s
            $queueId = ''
            $segmentCount = 0
            foreach ($seg in $s.segments) {
                $segmentCount++
                if (-not $queueId -and $seg.queueId) { $queueId = [string]$seg.queueId }
            }
            $sec = { param($n) if ($metrics.ContainsKey($n)) { [Math]::Round($metrics[$n] / 1000.0, 1) } else { $null } }
            $rows.Add([pscustomobject]@{
                    Participant = $participantIndex
                    Purpose     = [string]$p.purpose
                    Name        = [string]$p.participantName
                    Media       = [string]$s.mediaType
                    Direction   = [string]$s.direction
                    Queue       = Resolve-LookupName -Lookups $Lookups -Kind 'queues' -Id $queueId
                    Ani         = [string]$s.ani
                    Dnis        = [string]$s.dnis
                    Remote      = [string]$s.remote
                    Flow        = if ($null -ne $s.flow) { [string]$s.flow.flowName } else { '' }
                    Segments    = $segmentCount
                    AlertSec    = & $sec 'tAlert'
                    TalkSec     = & $sec 'tTalk'
                    HeldSec     = & $sec 'tHeld'
                    AcwSec      = & $sec 'tAcw'
                    HandleSec   = & $sec 'tHandle'
                    Recorded    = ($s.recording -eq $true)
                    Provider    = [string]$s.provider
                    UserId      = [string]$p.userId
                    SessionId   = [string]$s.sessionId
                }) | Out-Null
        }
    }
    return $rows
}

function Get-ConversationSegmentRows {
    # Chronological timeline across all participants, with offsets from conversation start.
    param([object]$Conversation, [hashtable]$Lookups)
    $convStart = ConvertTo-UtcDateTime $Conversation.conversationStart
    $items = [System.Collections.Generic.List[object]]::new()
    foreach ($p in $Conversation.participants) {
        foreach ($s in $p.sessions) {
            foreach ($seg in $s.segments) {
                $segStart = ConvertTo-UtcDateTime $seg.segmentStart
                $segEnd = ConvertTo-UtcDateTime $seg.segmentEnd
                $items.Add([pscustomobject]@{
                        SortKey    = if ($null -ne $segStart) { $segStart } else { [DateTime]::MaxValue }
                        Offset     = if ($null -ne $segStart -and $null -ne $convStart) { Format-OffsetDisplay ($segStart - $convStart) } else { '' }
                        Purpose    = [string]$p.purpose
                        Name       = [string]$p.participantName
                        Media      = [string]$s.mediaType
                        Type       = [string]$seg.segmentType
                        Start      = Format-LocalTimestamp $segStart 'HH:mm:ss.fff'
                        End        = Format-LocalTimestamp $segEnd 'HH:mm:ss.fff'
                        DurSec     = if ($null -ne $segStart -and $null -ne $segEnd) { [Math]::Round(($segEnd - $segStart).TotalSeconds, 1) } else { $null }
                        Queue      = Resolve-LookupName -Lookups $Lookups -Kind 'queues' -Id ([string]$seg.queueId)
                        WrapUp     = Resolve-LookupName -Lookups $Lookups -Kind 'wrapupCodes' -Id ([string]$seg.wrapUpCode)
                        Disconnect = [string]$seg.disconnectType
                        ErrorCode  = [string]$seg.errorCode
                        SipCodes   = (@($seg.sipResponseCodes) | Where-Object { $null -ne $_ }) -join ', '
                        Conference = ($seg.conference -eq $true)
                    }) | Out-Null
            }
        }
    }
    return @($items | Sort-Object SortKey | Select-Object -Property * -ExcludeProperty SortKey)
}

function Get-ConversationMetricRows {
    param([object]$Conversation)
    $rows = [System.Collections.Generic.List[object]]::new()
    foreach ($p in $Conversation.participants) {
        foreach ($s in $p.sessions) {
            foreach ($m in $s.metrics) {
                $name = [string]$m.name
                $isTimer = ($name -match '^t[A-Z]')
                $rows.Add([pscustomobject]@{
                        Purpose  = [string]$p.purpose
                        Name     = [string]$p.participantName
                        Media    = [string]$s.mediaType
                        Metric   = $name
                        Value    = if ($null -ne $m.value) { [double]$m.value } else { $null }
                        Seconds  = if ($isTimer -and $null -ne $m.value) { [Math]::Round([double]$m.value / 1000.0, 1) } else { $null }
                        Emitted  = Format-LocalTimestamp (ConvertTo-UtcDateTime $m.emitDate) 'yyyy-MM-dd HH:mm:ss'
                    }) | Out-Null
            }
        }
    }
    return $rows
}

function Get-ConversationFlowRows {
    param([object]$Conversation)
    $rows = [System.Collections.Generic.List[object]]::new()
    foreach ($p in $Conversation.participants) {
        foreach ($s in $p.sessions) {
            $f = $s.flow
            if ($null -eq $f) { continue }
            $outcomes = @(foreach ($o in $f.outcomes) { '{0}={1}' -f $o.flowOutcomeId, $o.flowOutcomeValue }) -join '; '
            $rows.Add([pscustomobject]@{
                    Purpose          = [string]$p.purpose
                    FlowName         = [string]$f.flowName
                    FlowType         = [string]$f.flowType
                    Version          = [string]$f.flowVersion
                    EntryType        = [string]$f.entryType
                    EntryReason      = [string]$f.entryReason
                    ExitReason       = [string]$f.exitReason
                    TransferType     = [string]$f.transferType
                    TransferTarget   = if ($f.transferTargetName) { [string]$f.transferTargetName } else { [string]$f.transferTargetAddress }
                    StartingLanguage = [string]$f.startingLanguage
                    EndingLanguage   = [string]$f.endingLanguage
                    IssuedCallback   = ($f.issuedCallback -eq $true)
                    Outcomes         = $outcomes
                    FlowId           = [string]$f.flowId
                }) | Out-Null
        }
    }
    return $rows
}

function Get-ConversationAttributeRows {
    param([object]$Conversation)
    $rows = [System.Collections.Generic.List[object]]::new()
    foreach ($p in $Conversation.participants) {
        $attrs = $p.attributes
        if ($null -eq $attrs) { continue }
        $label = if ($p.participantName) { "$($p.purpose): $($p.participantName)" } else { [string]$p.purpose }
        $pairs = if ($attrs -is [System.Collections.IDictionary]) {
            foreach ($key in $attrs.Keys) { [pscustomobject]@{ Key = [string]$key; Value = [string]$attrs[$key] } }
        }
        else {
            foreach ($prop in $attrs.PSObject.Properties) { [pscustomobject]@{ Key = $prop.Name; Value = [string]$prop.Value } }
        }
        foreach ($pair in @($pairs)) {
            $rows.Add([pscustomobject]@{ Participant = $label; Key = $pair.Key; Value = $pair.Value }) | Out-Null
        }
    }
    return @($rows | Sort-Object Key, Participant)
}

# -----------------------------------------------------------------------------
# Collection report (aggregates across all loaded conversations)
# -----------------------------------------------------------------------------

function New-ReportKpi {
    param([string]$Section, [string]$Metric, [AllowNull()][object]$Value, [string]$Unit, [string]$Detail = '')
    $display = if ($null -eq $Value) { 'n/a' }
    elseif ($Unit -eq 'sec') { Format-SecondsDisplay $Value }
    elseif ($Unit -eq '%') { '{0:0.0}%' -f [double]$Value }
    elseif ($Unit -eq 'count') { '{0:N0}' -f [double]$Value }
    else { [string]$Value }
    return [pscustomobject]@{ Section = $Section; Metric = $Metric; Value = $Value; Unit = $Unit; Display = $display; Detail = $Detail }
}

function New-VolumeBar {
    param([double]$Value, [double]$Max, [int]$Width = 30)
    if ($Max -le 0 -or $Value -le 0) { return '' }
    $length = [int][Math]::Max(1, [Math]::Round($Width * $Value / $Max))
    return ($script:caBarChar * $length)
}

function Get-StatAverage {
    # Average of a sum/count pair stored in a stat array, rounded to 0.1.
    param([double[]]$Stat, [int]$SumIndex, [double]$Divisor = 1.0)
    $count = $Stat[$SumIndex + 1]
    if ($count -le 0) { return $null }
    return [Math]::Round($Stat[$SumIndex] / $count / $Divisor, 1)
}

function Get-ConversationReport {
    # Aggregates cached profiles into KPIs, breakdown tables, and observations.
    # The hot loop uses inline double[] counters: PowerShell function and method calls
    # cost 10-75 microseconds each, which dominated earlier versions at 10k+ conversations.
    param(
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$Profiles,
        [hashtable]$Lookups,
        [string]$Source = '',
        [string]$QueryInterval = ''
    )

    # Conversation-level group layout (hour, date, media, wrap-up, disconnect, flow)
    $gConv = 0; $gOffered = 1; $gAnswered = 2; $gAsa = 3; $gAbandoned = 5; $gConnected = 6; $gTransferred = 7
    $gHandle = 8; $gTalk = 10; $gIvr = 12; $gDuration = 14; $gContained = 16; $gOutcomeFailures = 17
    $gSize = 18
    # Queue layout (ACD + agent sessions carrying the queue ID)
    $qConv = 0; $qOffered = 1; $qAsa = 2; $qAbandonWait = 4; $qShortAbandon = 6; $qOverSla = 7; $qAcd = 8
    $qHandle = 10; $qTalk = 12; $qHeld = 14; $qAcw = 16; $qTransfers = 18
    $qSize = 19
    # Agent layout
    $aConv = 0; $aHandle = 1; $aTalk = 3; $aHeld = 5; $aAcw = 7; $aAlert = 9; $aTransfers = 11; $aOutbound = 12; $aNotResponding = 13
    $aSize = 14

    $byQueue = @{}; $byAgent = @{}; $byHour = @{}; $byDate = @{}; $byMedia = @{}
    $byWrap = @{}; $byDisconnect = @{}; $byFlow = @{}; $byDivision = @{}; $byMos = @{}
    $agentLabels = @{}; $flowTypes = @{}
    $durations = [System.Collections.Generic.List[double]]::new()
    $handles = [System.Collections.Generic.List[double]]::new()
    $answerTimes = [System.Collections.Generic.List[double]]::new()
    $durationKeys = [System.Collections.Generic.List[double]]::new()
    $durationItems = [System.Collections.Generic.List[object]]::new()
    $mosKeys = [System.Collections.Generic.List[double]]::new()
    $mosItems = [System.Collections.Generic.List[object]]::new()
    $groups = [System.Collections.Generic.List[double[]]]::new()
    $windowStart = $null; $windowEnd = $null

    $total = 0; $inbound = 0; $outbound = 0; $offered = 0; $answered = 0; $abandoned = 0; $shortAbandons = 0
    $overSla = 0; $connected = 0; $transferredConnected = 0; $heldConnected = 0; $selfServed = 0; $voicemails = 0
    $recorded = 0; $voice = 0; $poorMos = 0; $evaluations = 0; $surveys = 0
    $asaSum = 0.0; $asaN = 0; $abandonWaitSum = 0.0; $abandonWaitN = 0; $handleSum = 0.0; $handleN = 0
    $talkSum = 0.0; $talkN = 0; $heldSum = 0.0; $heldN = 0; $acwSum = 0.0; $acwN = 0; $durationSum = 0.0; $durationN = 0
    $mosSum = 0.0; $mosN = 0; $evalScoreSum = 0.0; $evalScoreN = 0; $npsSum = 0.0; $npsN = 0

    foreach ($cp in $Profiles) {
        if ($null -eq $cp) { continue }
        $total++
        $sums = $cp.MetricSums
        $asaMs = $sums['tAnswered']; $handleMs = $sums['tHandle']; $talkMs = $sums['tTalk']; $ivrMs = $sums['tIvr']
        $duration = $cp.DurationSec

        if ($null -ne $cp.StartUtc) {
            if ($null -eq $windowStart -or $cp.StartUtc -lt $windowStart) { $windowStart = $cp.StartUtc }
            $lastSeen = if ($null -ne $cp.EndUtc) { $cp.EndUtc } else { $cp.StartUtc }
            if ($null -eq $windowEnd -or $lastSeen -gt $windowEnd) { $windowEnd = $lastSeen }
        }

        # -- Overall counters
        if ($cp.Direction -eq 'inbound') { $inbound++ } elseif ($cp.Direction -eq 'outbound') { $outbound++ }
        if ($cp.Offered) { $offered++; if ($cp.OverSla) { $overSla++ } }
        if ($cp.Answered) {
            $answered++
            if ($null -ne $asaMs) { $asaSum += $asaMs; $asaN++; $answerTimes.Add($asaMs / 1000.0) }
        }
        if ($cp.Abandoned) {
            $abandoned++
            $waitMs = $sums['tAbandon']
            if ($null -ne $waitMs) { $abandonWaitSum += $waitMs; $abandonWaitN++ }
        }
        if ($cp.ShortAbandon) { $shortAbandons++ }
        if ($cp.AgentConnected) {
            $connected++
            if ($cp.Transferred) { $transferredConnected++ }
            if ($cp.HoldCount -gt 0) { $heldConnected++ }
        }
        if ($null -ne $handleMs) { $handleSum += $handleMs; $handleN++; $handles.Add($handleMs / 1000.0) }
        if ($null -ne $talkMs) { $talkSum += $talkMs; $talkN++ }
        $value = $sums['tHeld']; if ($null -ne $value) { $heldSum += $value; $heldN++ }
        $value = $sums['tAcw']; if ($null -ne $value) { $acwSum += $value; $acwN++ }
        if ($null -ne $duration) {
            $durationSum += $duration; $durationN++
            $durations.Add([double]$duration)
            $durationKeys.Add([double]$duration); $durationItems.Add($cp)
        }
        if ($cp.SelfServed) { $selfServed++ }
        if ($cp.Voicemail) { $voicemails++ }
        if ($cp.Recorded) { $recorded++ }
        $evaluations += $cp.EvaluationCount
        if ($null -ne $cp.EvalScore) { $evalScoreSum += $cp.EvalScore; $evalScoreN++ }
        $surveys += $cp.SurveyCount
        if ($null -ne $cp.NpsScore) { $npsSum += $cp.NpsScore; $npsN++ }

        $isVoice = ($cp.MediaType -eq 'voice')
        if ($isVoice) { $voice++ }
        $band = $null
        if ($null -ne $cp.MinMos) {
            $mosSum += $cp.MinMos; $mosN++
            $mosKeys.Add([double]$cp.MinMos); $mosItems.Add($cp)
            if ($cp.MinMos -ge 4.0) { $band = 'Good (4.0 and above)' }
            elseif ($cp.MinMos -ge 3.5) { $band = 'Fair (3.5 - 3.99)' }
            else { $band = 'Poor (below 3.5)'; $poorMos++ }
        }
        elseif ($isVoice) { $band = 'No MOS data' }
        if ($null -ne $band) { if ($byMos.ContainsKey($band)) { $byMos[$band]++ } else { $byMos[$band] = 1 } }
        foreach ($divisionId in $cp.DivisionIds) { if ($byDivision.ContainsKey($divisionId)) { $byDivision[$divisionId]++ } else { $byDivision[$divisionId] = 1 } }

        # -- Conversation-level breakdowns: collect this conversation's group arrays, then update once
        $groups.Clear()
        if ($null -ne $cp.StartLocal) {
            $key = $cp.StartLocal.ToString('HH')
            $g = $byHour[$key]; if ($null -eq $g) { $g = [double[]]::new($gSize); $byHour[$key] = $g }; $groups.Add($g)
            $key = $cp.StartLocal.ToString('yyyy-MM-dd')
            $g = $byDate[$key]; if ($null -eq $g) { $g = [double[]]::new($gSize); $byDate[$key] = $g }; $groups.Add($g)
        }
        $key = $cp.MediaType + '|' + $cp.Direction
        $g = $byMedia[$key]; if ($null -eq $g) { $g = [double[]]::new($gSize); $byMedia[$key] = $g }; $groups.Add($g)
        if ($cp.WrapUpCodeId -or $cp.AgentConnected) {
            $key = [string]$cp.WrapUpCodeId
            $g = $byWrap[$key]; if ($null -eq $g) { $g = [double[]]::new($gSize); $byWrap[$key] = $g }; $groups.Add($g)
        }
        if ($cp.DisconnectPurpose -or $cp.DisconnectType) {
            $key = $cp.DisconnectPurpose + '|' + $cp.DisconnectType
            $g = $byDisconnect[$key]; if ($null -eq $g) { $g = [double[]]::new($gSize); $byDisconnect[$key] = $g }; $groups.Add($g)
        }
        if ($cp.FlowNames.Count -gt 0) {
            $key = $cp.FlowNames[0]
            $g = $byFlow[$key]; if ($null -eq $g) { $g = [double[]]::new($gSize); $byFlow[$key] = $g; $flowTypes[$key] = $cp.FlowType }
            if (-not $cp.Offered -and -not $cp.AgentConnected) { $g[$gContained]++ }
            $g[$gOutcomeFailures] += $cp.FlowOutcomeFailures
            $groups.Add($g)
        }
        foreach ($g in $groups) {
            $g[$gConv]++
            if ($cp.Offered) { $g[$gOffered]++ }
            if ($cp.Answered) { $g[$gAnswered]++; if ($null -ne $asaMs) { $g[$gAsa] += $asaMs; $g[$gAsa + 1]++ } }
            if ($cp.Abandoned) { $g[$gAbandoned]++ }
            if ($cp.AgentConnected) { $g[$gConnected]++ }
            if ($cp.Transferred) { $g[$gTransferred]++ }
            if ($null -ne $handleMs) { $g[$gHandle] += $handleMs; $g[$gHandle + 1]++ }
            if ($null -ne $talkMs) { $g[$gTalk] += $talkMs; $g[$gTalk + 1]++ }
            if ($null -ne $ivrMs) { $g[$gIvr] += $ivrMs; $g[$gIvr + 1]++ }
            if ($null -ne $duration) { $g[$gDuration] += $duration; $g[$gDuration + 1]++ }
        }

        # -- Session-level breakdowns (queue and agent attribution)
        $queuesSeen = $null; $agentsSeen = $null
        foreach ($sf in $cp.Sessions) {
            $m = $sf.Metrics
            $queueId = $sf.QueueId
            if ($queueId) {
                $q = $byQueue[$queueId]; if ($null -eq $q) { $q = [double[]]::new($qSize); $byQueue[$queueId] = $q }
                if ($null -eq $queuesSeen) { $queuesSeen = @{} }
                if (-not $queuesSeen.ContainsKey($queueId)) { $queuesSeen[$queueId] = $true; $q[$qConv]++ }
                if ($sf.Purpose -eq 'acd') {
                    $value = $m['nOffered']; if ($null -ne $value) { $q[$qOffered] += $value }
                    $value = $m['tAnswered']; if ($null -ne $value) { $q[$qAsa] += $value; $q[$qAsa + 1]++ }
                    $value = $m['tAbandon']; if ($null -ne $value) { $q[$qAbandonWait] += $value; $q[$qAbandonWait + 1]++ }
                    if ($m.ContainsKey('tShortAbandon')) { $q[$qShortAbandon]++ }
                    $value = $m['nOverSla']; if ($null -ne $value) { $q[$qOverSla] += $value }
                    $value = $m['tAcd']; if ($null -ne $value) { $q[$qAcd] += $value; $q[$qAcd + 1]++ }
                }
                else {
                    $value = $m['tHandle']; if ($null -ne $value) { $q[$qHandle] += $value; $q[$qHandle + 1]++ }
                    $value = $m['tTalk']; if ($null -ne $value) { $q[$qTalk] += $value; $q[$qTalk + 1]++ }
                    $value = $m['tHeld']; if ($null -ne $value) { $q[$qHeld] += $value; $q[$qHeld + 1]++ }
                    $value = $m['tAcw']; if ($null -ne $value) { $q[$qAcw] += $value; $q[$qAcw + 1]++ }
                    $value = $m['nTransferred']; if ($null -ne $value) { $q[$qTransfers] += $value }
                }
            }
            $agentKey = $sf.AgentKey
            if ($sf.Purpose -ne 'acd' -and $agentKey) {
                $a = $byAgent[$agentKey]; if ($null -eq $a) { $a = [double[]]::new($aSize); $byAgent[$agentKey] = $a }
                if ($null -eq $agentsSeen) { $agentsSeen = @{} }
                if (-not $agentsSeen.ContainsKey($agentKey)) { $agentsSeen[$agentKey] = $true; $a[$aConv]++ }
                if (-not $agentLabels.ContainsKey($agentKey) -and $cp.AgentNames.ContainsKey($agentKey)) { $agentLabels[$agentKey] = $cp.AgentNames[$agentKey] }
                $value = $m['tHandle']; if ($null -ne $value) { $a[$aHandle] += $value; $a[$aHandle + 1]++ }
                $value = $m['tTalk']; if ($null -ne $value) { $a[$aTalk] += $value; $a[$aTalk + 1]++ }
                $value = $m['tHeld']; if ($null -ne $value) { $a[$aHeld] += $value; $a[$aHeld + 1]++ }
                $value = $m['tAcw']; if ($null -ne $value) { $a[$aAcw] += $value; $a[$aAcw + 1]++ }
                $value = $m['tAlert']; if ($null -ne $value) { $a[$aAlert] += $value; $a[$aAlert + 1]++ }
                $value = $m['nTransferred']; if ($null -ne $value) { $a[$aTransfers] += $value }
                $value = $m['nOutbound']; if ($null -ne $value) { $a[$aOutbound] += $value }
                if ($m.ContainsKey('tNotResponding')) { $a[$aNotResponding]++ }
            }
        }
    }

    # -- Headline numbers
    $avg = { param([double]$sum, [int]$n, [double]$div) if ($n -gt 0) { [Math]::Round($sum / $n / $div, 1) } else { $null } }
    $abandonPct = Get-Percent $abandoned $offered
    $slaPct = Get-Percent ($offered - $overSla) $offered
    $transferPct = Get-Percent $transferredConnected $connected
    $holdPct = Get-Percent $heldConnected $connected
    $poorMosPct = Get-Percent $poorMos $mosN
    $aht = & $avg $handleSum $handleN 1000

    $kpis = [System.Collections.Generic.List[object]]::new()
    $kpis.Add((New-ReportKpi 'Volume' 'Conversations' $total 'count')) | Out-Null
    $kpis.Add((New-ReportKpi 'Volume' 'Inbound' $inbound 'count' ('{0}% of total' -f (Get-Percent $inbound $total)))) | Out-Null
    $kpis.Add((New-ReportKpi 'Volume' 'Outbound' $outbound 'count' ('{0}% of total' -f (Get-Percent $outbound $total)))) | Out-Null
    $kpis.Add((New-ReportKpi 'Volume' 'Offered to a queue' $offered 'count')) | Out-Null
    $kpis.Add((New-ReportKpi 'Volume' 'Reached an agent' $connected 'count' ('{0}% of total' -f (Get-Percent $connected $total)))) | Out-Null
    $kpis.Add((New-ReportKpi 'Volume' 'Self-served (flow only)' $selfServed 'count')) | Out-Null
    $kpis.Add((New-ReportKpi 'Volume' 'Voicemail' $voicemails 'count')) | Out-Null

    $kpis.Add((New-ReportKpi 'Service' 'Answered' (Get-Percent $answered $offered) '%' ('{0:N0} of {1:N0} offered' -f $answered, $offered))) | Out-Null
    $kpis.Add((New-ReportKpi 'Service' 'Abandon rate' $abandonPct '%' ('{0:N0} abandoned, {1:N0} short' -f $abandoned, $shortAbandons))) | Out-Null
    $kpis.Add((New-ReportKpi 'Service' 'Within service level' $slaPct '%' ('(offered - over SLA) / offered; {0:N0} over SLA' -f $overSla))) | Out-Null
    $kpis.Add((New-ReportKpi 'Service' 'Avg speed of answer' (& $avg $asaSum $asaN 1000) 'sec' 'tAnswered, answered conversations')) | Out-Null
    $kpis.Add((New-ReportKpi 'Service' '90th pct speed of answer' (Get-NearestRankPercentile $answerTimes 0.9) 'sec')) | Out-Null
    $kpis.Add((New-ReportKpi 'Service' 'Avg wait before abandon' (& $avg $abandonWaitSum $abandonWaitN 1000) 'sec' 'tAbandon')) | Out-Null

    $kpis.Add((New-ReportKpi 'Handling' 'Avg handle time' $aht 'sec' ('{0:N0} handled conversations' -f $handleN))) | Out-Null
    $kpis.Add((New-ReportKpi 'Handling' 'Median handle time' (Get-NearestRankPercentile $handles 0.5) 'sec')) | Out-Null
    $kpis.Add((New-ReportKpi 'Handling' '90th pct handle time' (Get-NearestRankPercentile $handles 0.9) 'sec')) | Out-Null
    $kpis.Add((New-ReportKpi 'Handling' 'Avg talk' (& $avg $talkSum $talkN 1000) 'sec')) | Out-Null
    $kpis.Add((New-ReportKpi 'Handling' 'Avg hold (when held)' (& $avg $heldSum $heldN 1000) 'sec')) | Out-Null
    $kpis.Add((New-ReportKpi 'Handling' 'Avg after-call work' (& $avg $acwSum $acwN 1000) 'sec')) | Out-Null
    $kpis.Add((New-ReportKpi 'Handling' 'Transfer rate' $transferPct '%' 'of conversations that reached an agent')) | Out-Null
    $kpis.Add((New-ReportKpi 'Handling' 'Hold rate' $holdPct '%' 'of conversations that reached an agent')) | Out-Null
    $kpis.Add((New-ReportKpi 'Handling' 'Avg conversation duration' (& $avg $durationSum $durationN 1) 'sec')) | Out-Null
    $kpis.Add((New-ReportKpi 'Handling' 'Median conversation duration' (Get-NearestRankPercentile $durations 0.5) 'sec')) | Out-Null

    $kpis.Add((New-ReportKpi 'Quality & CX' 'Avg minimum MOS' (& $avg $mosSum $mosN 1) 'score' ('{0:N0} conversations with MOS data' -f $mosN))) | Out-Null
    $kpis.Add((New-ReportKpi 'Quality & CX' 'Poor MOS (below 3.5)' $poorMosPct '%' ('{0:N0} conversations' -f $poorMos))) | Out-Null
    $kpis.Add((New-ReportKpi 'Quality & CX' 'Recorded' (Get-Percent $recorded $total) '%')) | Out-Null
    $kpis.Add((New-ReportKpi 'Quality & CX' 'Evaluations' $evaluations 'count' ('avg score {0}' -f $(if ($evalScoreN -gt 0) { & $avg $evalScoreSum $evalScoreN 1 } else { 'n/a' })))) | Out-Null
    $kpis.Add((New-ReportKpi 'Quality & CX' 'Surveys' $surveys 'count' ('avg NPS score {0}' -f $(if ($npsN -gt 0) { & $avg $npsSum $npsN 1 } else { 'n/a' })))) | Out-Null

    # -- Tables
    $tables = [ordered]@{}

    $rows = foreach ($key in $byQueue.Keys) {
        $q = $byQueue[$key]
        $qOfferedCount = [int]$q[$qOffered]; $qAbandoned = [int]$q[$qAbandonWait + 1]; $qOverSlaCount = [int]$q[$qOverSla]
        [pscustomobject]@{
            Queue             = Resolve-LookupName -Lookups $Lookups -Kind 'queues' -Id $key
            Conversations     = [int]$q[$qConv]
            Offered           = $qOfferedCount
            Answered          = [int]$q[$qAsa + 1]
            Abandoned         = $qAbandoned
            AbandonPct        = Get-Percent $qAbandoned $qOfferedCount
            ShortAbandons     = [int]$q[$qShortAbandon]
            AvgAsaSec         = Get-StatAverage $q $qAsa 1000
            AvgAbandonWaitSec = Get-StatAverage $q $qAbandonWait 1000
            AvgQueueSec       = Get-StatAverage $q $qAcd 1000
            OverSla           = $qOverSlaCount
            WithinSlaPct      = Get-Percent ($qOfferedCount - $qOverSlaCount) $qOfferedCount
            Handled           = [int]$q[$qHandle + 1]
            AhtSec            = Get-StatAverage $q $qHandle 1000
            AvgTalkSec        = Get-StatAverage $q $qTalk 1000
            AvgHoldSec        = Get-StatAverage $q $qHeld 1000
            AvgAcwSec         = Get-StatAverage $q $qAcw 1000
            Transfers         = [int]$q[$qTransfers]
            QueueId           = $key
        }
    }
    $tables['Queues'] = [pscustomobject]@{
        Description = 'Per-queue performance. Offered/answered/abandoned come from ACD sessions; handle metrics from agent sessions routed through the queue. Times in seconds.'
        Rows        = @($rows | Sort-Object -Property @{ Expression = 'Offered'; Descending = $true }, @{ Expression = 'Conversations'; Descending = $true })
    }

    $rows = foreach ($key in $byAgent.Keys) {
        $a = $byAgent[$key]
        [pscustomobject]@{
            Agent          = if ($agentLabels.ContainsKey($key)) { [string]$agentLabels[$key] } else { $key }
            Conversations  = [int]$a[$aConv]
            Handled        = [int]$a[$aHandle + 1]
            AhtSec         = Get-StatAverage $a $aHandle 1000
            AvgTalkSec     = Get-StatAverage $a $aTalk 1000
            AvgHoldSec     = Get-StatAverage $a $aHeld 1000
            Holds          = [int]$a[$aHeld + 1]
            AvgAcwSec      = Get-StatAverage $a $aAcw 1000
            TotalHandleHrs = [Math]::Round($a[$aHandle] / 3600000.0, 2)
            AvgAlertSec    = Get-StatAverage $a $aAlert 1000
            NotResponding  = [int]$a[$aNotResponding]
            Transfers      = [int]$a[$aTransfers]
            Outbound       = [int]$a[$aOutbound]
            UserId         = $key
        }
    }
    $tables['Agents'] = [pscustomobject]@{
        Description = 'Per-agent workload from agent sessions. Conversations includes alerts the agent did not answer (see NotResponding). Times in seconds unless noted.'
        Rows        = @($rows | Sort-Object -Property @{ Expression = 'Handled'; Descending = $true }, @{ Expression = 'Conversations'; Descending = $true })
    }

    $maxHour = 0.0
    foreach ($g in $byHour.Values) { $maxHour = [Math]::Max($maxHour, $g[$gConv]) }
    $rows = foreach ($key in ($byHour.Keys | Sort-Object)) {
        $g = $byHour[$key]; $count = [int]$g[$gConv]; $hOffered = [int]$g[$gOffered]; $hAbandoned = [int]$g[$gAbandoned]
        [pscustomobject]@{
            Hour          = '{0}:00' -f $key
            Conversations = $count
            SharePct      = Get-Percent $count $total
            Volume        = New-VolumeBar $count $maxHour
            Offered       = $hOffered
            Answered      = [int]$g[$gAnswered]
            Abandoned     = $hAbandoned
            AbandonPct    = Get-Percent $hAbandoned $hOffered
            AvgAsaSec     = Get-StatAverage $g $gAsa 1000
            AhtSec        = Get-StatAverage $g $gHandle 1000
        }
    }
    $tables['Hourly'] = [pscustomobject]@{ Description = 'Conversations by local start hour. Times in seconds.'; Rows = @($rows) }

    $maxDay = 0.0
    foreach ($g in $byDate.Values) { $maxDay = [Math]::Max($maxDay, $g[$gConv]) }
    $rows = foreach ($key in ($byDate.Keys | Sort-Object)) {
        $g = $byDate[$key]; $count = [int]$g[$gConv]; $dOffered = [int]$g[$gOffered]; $dAbandoned = [int]$g[$gAbandoned]
        [pscustomobject]@{
            Date           = $key
            Day            = ([DateTime]::ParseExact($key, 'yyyy-MM-dd', [System.Globalization.CultureInfo]::InvariantCulture)).DayOfWeek.ToString().Substring(0, 3)
            Conversations  = $count
            SharePct       = Get-Percent $count $total
            Volume         = New-VolumeBar $count $maxDay
            Answered       = [int]$g[$gAnswered]
            Abandoned      = $dAbandoned
            AbandonPct     = Get-Percent $dAbandoned $dOffered
            AvgAsaSec      = Get-StatAverage $g $gAsa 1000
            AhtSec         = Get-StatAverage $g $gHandle 1000
            AvgDurationSec = Get-StatAverage $g $gDuration 1
        }
    }
    $tables['Daily'] = [pscustomobject]@{ Description = 'Conversations by local start date. Times in seconds.'; Rows = @($rows) }

    $rows = foreach ($key in $byMedia.Keys) {
        $g = $byMedia[$key]; $parts = $key.Split('|'); $count = [int]$g[$gConv]; $mOffered = [int]$g[$gOffered]; $mAbandoned = [int]$g[$gAbandoned]
        [pscustomobject]@{
            MediaType      = if ($parts[0]) { $parts[0] } else { '(unknown)' }
            Direction      = if ($parts[1]) { $parts[1] } else { '(unknown)' }
            Conversations  = $count
            SharePct       = Get-Percent $count $total
            Answered       = [int]$g[$gAnswered]
            Abandoned      = $mAbandoned
            AbandonPct     = Get-Percent $mAbandoned $mOffered
            ReachedAgent   = [int]$g[$gConnected]
            Transferred    = [int]$g[$gTransferred]
            AvgDurationSec = Get-StatAverage $g $gDuration 1
            AhtSec         = Get-StatAverage $g $gHandle 1000
        }
    }
    $tables['Media & Direction'] = [pscustomobject]@{ Description = 'Conversations by primary media type and originating direction. Times in seconds.'; Rows = @($rows | Sort-Object -Property @{ Expression = 'Conversations'; Descending = $true }) }

    $wrapTotal = 0.0
    foreach ($g in $byWrap.Values) { $wrapTotal += $g[$gConv] }
    $rows = foreach ($key in $byWrap.Keys) {
        $g = $byWrap[$key]; $count = [int]$g[$gConv]
        [pscustomobject]@{
            WrapUpCode    = if ($key) { Resolve-LookupName -Lookups $Lookups -Kind 'wrapupCodes' -Id $key } else { '(no wrap-up)' }
            Conversations = $count
            SharePct      = Get-Percent $count $wrapTotal
            AhtSec        = Get-StatAverage $g $gHandle 1000
            AvgTalkSec    = Get-StatAverage $g $gTalk 1000
            Transferred   = [int]$g[$gTransferred]
            WrapUpCodeId  = $key
        }
    }
    $tables['Wrap-up Codes'] = [pscustomobject]@{ Description = 'Final wrap-up code of conversations that reached an agent (share is of those conversations). Times in seconds.'; Rows = @($rows | Sort-Object -Property @{ Expression = 'Conversations'; Descending = $true }) }

    $rows = foreach ($key in $byDisconnect.Keys) {
        $g = $byDisconnect[$key]; $parts = $key.Split('|'); $count = [int]$g[$gConv]
        [pscustomobject]@{
            DisconnectedBy = if ($parts[0]) { $parts[0] } else { '(unknown)' }
            DisconnectType = $parts[1]
            Conversations  = $count
            SharePct       = Get-Percent $count $total
            Answered       = [int]$g[$gAnswered]
            Abandoned      = [int]$g[$gAbandoned]
        }
    }
    $tables['Disconnects'] = [pscustomobject]@{ Description = 'Who ended each conversation: the latest non-peer, non-transfer disconnect (endpoint = hung up, client = agent UI, system/error/timeout = platform).'; Rows = @($rows | Sort-Object -Property @{ Expression = 'Conversations'; Descending = $true }) }

    $rows = foreach ($key in $byFlow.Keys) {
        $g = $byFlow[$key]; $count = [int]$g[$gConv]; $contained = [int]$g[$gContained]
        [pscustomobject]@{
            Flow            = $key
            FlowType        = [string]$flowTypes[$key]
            Conversations   = $count
            SharePct        = Get-Percent $count $total
            ReachedQueue    = [int]$g[$gOffered]
            ReachedAgent    = [int]$g[$gConnected]
            Contained       = $contained
            ContainedPct    = Get-Percent $contained $count
            OutcomeFailures = [int]$g[$gOutcomeFailures]
            AvgIvrSec       = Get-StatAverage $g $gIvr 1000
        }
    }
    $tables['IVR Flows'] = [pscustomobject]@{ Description = 'First Architect flow per conversation. Contained = never offered to a queue and never reached an agent. Times in seconds.'; Rows = @($rows | Sort-Object -Property @{ Expression = 'Conversations'; Descending = $true }) }

    $mosTotal = 0.0
    foreach ($count in $byMos.Values) { $mosTotal += $count }
    $rows = foreach ($band in @('Good (4.0 and above)', 'Fair (3.5 - 3.99)', 'Poor (below 3.5)', 'No MOS data')) {
        if (-not $byMos.ContainsKey($band)) { continue }
        $count = [int]$byMos[$band]
        [pscustomobject]@{ Band = $band; Conversations = $count; SharePct = Get-Percent $count $mosTotal }
    }
    $tables['Voice Quality'] = [pscustomobject]@{ Description = 'Minimum MOS per conversation (1-5 scale; 4.0+ is good, below 3.5 is noticeably degraded). Share is of voice conversations.'; Rows = @($rows) }

    $rows = foreach ($key in $byDivision.Keys) {
        $count = [int]$byDivision[$key]
        [pscustomobject]@{ Division = Resolve-LookupName -Lookups $Lookups -Kind 'divisions' -Id $key; Conversations = $count; SharePct = Get-Percent $count $total; DivisionId = $key }
    }
    $tables['Divisions'] = [pscustomobject]@{ Description = 'Conversations per division (a conversation can belong to several).'; Rows = @($rows | Sort-Object -Property @{ Expression = 'Conversations'; Descending = $true }) }

    $outlierRow = {
        param($cp)
        [pscustomobject]@{
            ConversationId = $cp.ConversationId
            Start          = Format-LocalTimestamp $cp.StartUtc
            DurationSec    = $cp.DurationSec
            MinMos         = $cp.MinMos
            MediaType      = $cp.MediaType
            Direction      = $cp.Direction
            Queue          = if ($cp.QueueIds.Count -gt 0) { Resolve-LookupName -Lookups $Lookups -Kind 'queues' -Id $cp.QueueIds[0] } else { '' }
            Agent          = if ($cp.AgentKeys.Count -gt 0 -and $cp.AgentNames.ContainsKey($cp.AgentKeys[0])) { [string]$cp.AgentNames[$cp.AgentKeys[0]] } elseif ($cp.AgentKeys.Count -gt 0) { $cp.AgentKeys[0] } else { '' }
            DisconnectedBy = $cp.DisconnectPurpose
            Codecs         = $cp.Codecs -join ', '
        }
    }
    $longest = [System.Collections.Generic.List[object]]::new()
    if ($durationItems.Count -gt 0) {
        $keys = $durationKeys.ToArray(); $items = $durationItems.ToArray()
        [Array]::Sort($keys, $items)
        for ($i = $items.Length - 1; $i -ge [Math]::Max(0, $items.Length - 25); $i--) { $longest.Add((& $outlierRow $items[$i])) | Out-Null }
    }
    $tables['Longest'] = [pscustomobject]@{ Description = 'The 25 longest conversations (seconds), for drill-down in the Results tab.'; Rows = @($longest) }

    $lowestMos = [System.Collections.Generic.List[object]]::new()
    if ($mosItems.Count -gt 0) {
        $keys = $mosKeys.ToArray(); $items = $mosItems.ToArray()
        [Array]::Sort($keys, $items)
        for ($i = 0; $i -lt [Math]::Min(25, $items.Length); $i++) { $lowestMos.Add((& $outlierRow $items[$i])) | Out-Null }
    }
    $tables['Lowest MOS'] = [pscustomobject]@{ Description = 'The 25 conversations with the lowest minimum MOS, for voice-quality follow-up.'; Rows = @($lowestMos) }

    # -- Observations (rule-based, with the reference threshold stated in each line)
    $observations = [System.Collections.Generic.List[string]]::new()
    $queueRows = @($tables['Queues'].Rows)
    if ($null -ne $abandonPct -and $abandonPct -ge 5) {
        $worst = @($queueRows | Where-Object { $_.Offered -ge 10 -and $null -ne $_.AbandonPct } | Sort-Object AbandonPct -Descending | Select-Object -First 1)
        $suffix = if ($worst.Count -gt 0) { " Highest: $($worst[0].Queue) at $($worst[0].AbandonPct)% of $($worst[0].Offered) offered." } else { '' }
        $observations.Add("Abandon rate is $abandonPct% ($abandoned of $offered offered), above the 5% reference threshold.$suffix") | Out-Null
    }
    if ($null -ne $slaPct -and $slaPct -lt 80) {
        $observations.Add("Only $slaPct% of queue offers were handled within service level (80% reference).") | Out-Null
    }
    if ($null -ne $transferPct -and $transferPct -ge 15) {
        $observations.Add("$transferPct% of agent-handled conversations were transferred (15% reference). Review the Queues and Wrap-up Codes tables for routing fit.") | Out-Null
    }
    if ($null -ne $poorMosPct -and $poorMosPct -ge 2) {
        $observations.Add("$poorMosPct% of conversations with MOS data scored below 3.5 (2% reference). See the Lowest MOS table for the affected conversations.") | Out-Null
    }
    $slowQueue = @($queueRows | Where-Object { $_.Handled -ge 10 -and $null -ne $_.AhtSec } | Sort-Object AhtSec -Descending | Select-Object -First 1)
    if ($slowQueue.Count -gt 0 -and $queueRows.Count -gt 1) {
        $observations.Add("Longest average handle time: $($slowQueue[0].Queue) at $(Format-SecondsDisplay $slowQueue[0].AhtSec) across $($slowQueue[0].Handled) handled conversations.") | Out-Null
    }
    $missedAgent = @($tables['Agents'].Rows | Where-Object { $_.NotResponding -ge 5 } | Sort-Object NotResponding -Descending | Select-Object -First 1)
    if ($missedAgent.Count -gt 0) {
        $observations.Add("Most unanswered alerts: $($missedAgent[0].Agent) with $($missedAgent[0].NotResponding) (tNotResponding; 5 reference). Check presence and alerting-timeout settings.") | Out-Null
    }
    $systemEnds = 0
    foreach ($row in $tables['Disconnects'].Rows) { if ($row.DisconnectType -in @('system', 'error', 'timeout', 'transport.failure')) { $systemEnds += $row.Conversations } }
    $systemPct = Get-Percent $systemEnds $total
    if ($null -ne $systemPct -and $systemPct -ge 5) {
        $observations.Add("$systemPct% of conversations ended with a system, error, or timeout disconnect (5% reference).") | Out-Null
    }
    $peak = @($tables['Hourly'].Rows | Sort-Object Conversations -Descending | Select-Object -First 1)
    if ($peak.Count -gt 0) {
        $observations.Add("Peak hour: $($peak[0].Hour) with $($peak[0].Conversations) conversations ($($peak[0].SharePct)% of volume).") | Out-Null
    }

    # -- Headline
    $windowText = if ($null -ne $windowStart) { '{0} to {1}' -f (Format-LocalTimestamp $windowStart), (Format-LocalTimestamp $windowEnd) } else { 'an unknown window' }
    $headline = '{0:N0} conversations from {1}.' -f $total, $windowText
    if ($offered -gt 0) {
        $headline += ' {0:N0} of {1:N0} queue offers answered ({2}%), {3:N0} abandoned ({4}%).' -f $answered, $offered, (Get-Percent $answered $offered), $abandoned, $abandonPct
    }
    if ($null -ne $aht) { $headline += ' Average handle time {0}.' -f (Format-SecondsDisplay $aht) }

    $offset = [TimeZoneInfo]::Local.GetUtcOffset([DateTime]::Now)
    $offsetText = 'UTC{0}{1:hh\:mm}' -f $(if ($offset.Ticks -lt 0) { '-' } else { '+' }), $offset

    return [pscustomobject]@{
        Title             = 'Genesys Cloud Conversation Report'
        GeneratedLocal    = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
        TimeZone          = $offsetText
        Source            = $Source
        QueryInterval     = $QueryInterval
        WindowStartLocal  = Format-LocalTimestamp $windowStart
        WindowEndLocal    = Format-LocalTimestamp $windowEnd
        ConversationCount = $total
        Headline          = $headline
        Units             = 'Durations in seconds (display adds m:ss); percentages 0-100; MOS on a 1-5 scale; times in local time.'
        Kpis              = @($kpis)
        Observations      = @($observations)
        Tables            = $tables
    }
}

# -----------------------------------------------------------------------------
# Report rendering (HTML + JSON)
# -----------------------------------------------------------------------------

function ConvertTo-ReportHeaderText {
    # 'AvgAsaSec' -> 'Avg ASA (s)'; 'AbandonPct' -> 'Abandon %'
    param([string]$Name)
    $suffix = ''
    if ($Name -cmatch 'Sec$') { $Name = $Name.Substring(0, $Name.Length - 3); $suffix = ' (s)' }
    elseif ($Name -cmatch 'Pct$') { $Name = $Name.Substring(0, $Name.Length - 3); $suffix = ' %' }
    elseif ($Name -cmatch 'Hrs$') { $Name = $Name.Substring(0, $Name.Length - 3); $suffix = ' (h)' }
    $words = [regex]::Replace($Name, '(?<=[a-z])(?=[A-Z])', ' ')
    foreach ($pair in @(@('Asa', 'ASA'), @('Aht', 'AHT'), @('Mos', 'MOS'), @('Sla', 'SLA'), @('Ivr', 'IVR'), @('Acw', 'ACW'), @('Id', 'ID'))) {
        $words = [regex]::Replace($words, "\b$($pair[0])\b", $pair[1])
    }
    return $words + $suffix
}

function ConvertTo-ConversationReportHtml {
    param([Parameter(Mandatory = $true)][object]$Report)

    $enc = { param($v) [System.Net.WebUtility]::HtmlEncode([string]$v) }
    $sb = [System.Text.StringBuilder]::new()
    $null = $sb.AppendLine('<!DOCTYPE html>')
    $null = $sb.AppendLine('<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">')
    $null = $sb.AppendLine("<title>$(& $enc $Report.Title)</title>")
    $null = $sb.AppendLine(@"
<style>
:root { --bg:#ffffff; --fg:#1f2328; --muted:#636c76; --line:#d0d7de; --tile:#f6f8fa; --accent:#0b5cad; --bar:#9cc3eb; --warn:#fff8e1; --warnline:#e0b000; }
@media (prefers-color-scheme: dark) { :root { --bg:#0d1117; --fg:#e6edf3; --muted:#9198a1; --line:#30363d; --tile:#161b22; --accent:#4493f8; --bar:#1f4f86; --warn:#2a2410; --warnline:#9e7a00; } }
* { box-sizing:border-box; }
body { margin:0; background:var(--bg); color:var(--fg); font:14px/1.45 "Segoe UI", system-ui, sans-serif; }
main { max-width:1200px; margin:0 auto; padding:24px 16px 48px; }
h1 { font-size:24px; margin:0 0 4px; }
h2 { font-size:18px; margin:32px 0 4px; padding-top:8px; border-top:1px solid var(--line); }
h3 { font-size:13px; text-transform:uppercase; letter-spacing:.04em; color:var(--muted); margin:18px 0 6px; }
.meta, .desc, .units { color:var(--muted); font-size:13px; }
.headline { margin:16px 0; padding:12px 14px; border-left:4px solid var(--accent); background:var(--tile); font-size:15px; }
.tiles { display:grid; grid-template-columns:repeat(auto-fill, minmax(170px, 1fr)); gap:8px; }
.tile { background:var(--tile); border:1px solid var(--line); border-radius:6px; padding:8px 10px; }
.tile .k { color:var(--muted); font-size:12px; }
.tile .v { font-size:18px; font-weight:600; }
.tile .d { color:var(--muted); font-size:11px; }
.obs { background:var(--warn); border:1px solid var(--warnline); border-radius:6px; padding:8px 12px 8px 28px; }
nav a { color:var(--accent); margin-right:12px; white-space:nowrap; }
.scroll { overflow-x:auto; }
table { border-collapse:collapse; width:100%; font-size:13px; }
th, td { border-bottom:1px solid var(--line); padding:4px 8px; text-align:left; white-space:nowrap; }
th { position:sticky; top:0; background:var(--tile); }
td.n { text-align:right; font-variant-numeric:tabular-nums; }
tbody tr:nth-child(even) { background:color-mix(in srgb, var(--tile) 60%, transparent); }
@media print { h2 { break-before:auto; } .scroll { overflow:visible; } }
</style></head><body><main>
"@)
    $null = $sb.AppendLine("<h1>$(& $enc $Report.Title)</h1>")
    $metaParts = [System.Collections.Generic.List[string]]::new()
    $metaParts.Add("Data window (conversation times): $(& $enc $Report.WindowStartLocal) to $(& $enc $Report.WindowEndLocal) ($(& $enc $Report.TimeZone))") | Out-Null
    if ($Report.QueryInterval) { $metaParts.Add("Query interval (UTC): $(& $enc $Report.QueryInterval)") | Out-Null }
    if ($Report.Source) { $metaParts.Add("Source: $(& $enc $Report.Source)") | Out-Null }
    $metaParts.Add(('Conversations: {0:N0}' -f $Report.ConversationCount)) | Out-Null
    $metaParts.Add("Generated: $(& $enc $Report.GeneratedLocal)") | Out-Null
    $null = $sb.AppendLine("<p class=""meta"">$($metaParts -join ' &middot; ')</p>")
    $null = $sb.AppendLine("<p class=""units"">$(& $enc $Report.Units)</p>")
    $null = $sb.AppendLine("<div class=""headline"">$(& $enc $Report.Headline)</div>")

    $null = $sb.Append('<nav>')
    foreach ($title in $Report.Tables.Keys) { $null = $sb.Append("<a href=""#t-$([Array]::IndexOf(@($Report.Tables.Keys), $title))"">$(& $enc $title)</a>") }
    $null = $sb.AppendLine('</nav>')

    $null = $sb.AppendLine('<h2>Key metrics</h2>')
    foreach ($section in @($Report.Kpis | ForEach-Object Section | Select-Object -Unique)) {
        $null = $sb.AppendLine("<h3>$(& $enc $section)</h3><div class=""tiles"">")
        foreach ($kpi in @($Report.Kpis | Where-Object { $_.Section -eq $section })) {
            $detail = if ($kpi.Detail) { "<div class=""d"">$(& $enc $kpi.Detail)</div>" } else { '' }
            $null = $sb.AppendLine("<div class=""tile""><div class=""k"">$(& $enc $kpi.Metric)</div><div class=""v"">$(& $enc $kpi.Display)</div>$detail</div>")
        }
        $null = $sb.AppendLine('</div>')
    }

    $null = $sb.AppendLine('<h2>Observations</h2>')
    if (@($Report.Observations).Count -gt 0) {
        $null = $sb.AppendLine('<ul class="obs">')
        foreach ($line in $Report.Observations) { $null = $sb.AppendLine("<li>$(& $enc $line)</li>") }
        $null = $sb.AppendLine('</ul>')
    }
    else { $null = $sb.AppendLine('<p class="desc">No reference thresholds were exceeded.</p>') }
    $null = $sb.AppendLine('<p class="desc">Reference thresholds are generic starting points; compare against your own service targets.</p>')

    $tableIndex = 0
    foreach ($title in $Report.Tables.Keys) {
        $table = $Report.Tables[$title]
        $rows = @($table.Rows)
        $null = $sb.AppendLine("<h2 id=""t-$tableIndex"">$(& $enc $title)</h2><p class=""desc"">$(& $enc $table.Description)</p>")
        $tableIndex++
        if ($rows.Count -eq 0) { $null = $sb.AppendLine('<p class="desc">No data.</p>'); continue }
        $columns = @($rows[0].PSObject.Properties.Name | Where-Object { $_ -ne 'Volume' })
        # Share bars are scaled to the table's largest share so the peak row fills the cell.
        $maxShare = 0.0
        foreach ($row in $rows) { if ($null -ne $row.PSObject.Properties['SharePct'] -and $null -ne $row.SharePct) { $maxShare = [Math]::Max($maxShare, [double]$row.SharePct) } }
        $null = $sb.Append('<div class="scroll"><table><thead><tr>')
        foreach ($column in $columns) { $null = $sb.Append("<th>$(& $enc (ConvertTo-ReportHeaderText $column))</th>") }
        $null = $sb.AppendLine('</tr></thead><tbody>')
        foreach ($row in $rows) {
            $null = $sb.Append('<tr>')
            foreach ($column in $columns) {
                $value = $row.$column
                if ($null -eq $value -or [string]$value -eq '') { $null = $sb.Append('<td></td>'); continue }
                $isNumber = ($value -is [int] -or $value -is [long] -or $value -is [double] -or $value -is [decimal])
                if ($column -eq 'SharePct' -and $isNumber) {
                    $width = if ($maxShare -gt 0) { [Math]::Round(100.0 * [double]$value / $maxShare, 1) } else { 0 }
                    $null = $sb.Append(('<td class="n" style="background:linear-gradient(90deg,var(--bar) {0}%,transparent {0}%)">{1}</td>' -f $width.ToString([System.Globalization.CultureInfo]::InvariantCulture), (& $enc $value)))
                }
                elseif ($isNumber) { $null = $sb.Append("<td class=""n"">$(& $enc $value)</td>") }
                else { $null = $sb.Append("<td>$(& $enc $value)</td>") }
            }
            $null = $sb.AppendLine('</tr>')
        }
        $null = $sb.AppendLine('</tbody></table></div>')
    }

    $null = $sb.AppendLine('</main></body></html>')
    return $sb.ToString()
}

function ConvertTo-ConversationReportJson {
    # Same content as the HTML, minus the text-only Volume bars.
    param([Parameter(Mandatory = $true)][object]$Report)
    $tables = [ordered]@{}
    foreach ($title in $Report.Tables.Keys) {
        $table = $Report.Tables[$title]
        $tables[$title] = [ordered]@{
            description = $table.Description
            rows        = @($table.Rows | Select-Object -Property * -ExcludeProperty Volume)
        }
    }
    $payload = [ordered]@{
        title             = $Report.Title
        generatedLocal    = $Report.GeneratedLocal
        timeZone          = $Report.TimeZone
        source            = $Report.Source
        queryInterval     = $Report.QueryInterval
        windowStartLocal  = $Report.WindowStartLocal
        windowEndLocal    = $Report.WindowEndLocal
        conversationCount = $Report.ConversationCount
        units             = $Report.Units
        headline          = $Report.Headline
        kpis              = @($Report.Kpis | Select-Object Section, Metric, Value, Unit, Detail)
        observations      = @($Report.Observations)
        tables            = $tables
    }
    return ($payload | ConvertTo-Json -Depth 8)
}
