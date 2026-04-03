function ConvertTo-NullableUtcDateTime {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$Value
    )

    if ($null -eq $Value) {
        return $null
    }

    if ($Value -is [DateTimeOffset]) {
        return $Value.UtcDateTime
    }

    if ($Value -is [DateTime]) {
        if ($Value.Kind -eq [DateTimeKind]::Utc) {
            return $Value
        }

        return $Value.ToUniversalTime()
    }

    $text = [string]$Value
    if ([string]::IsNullOrWhiteSpace($text)) {
        return $null
    }

    try {
        return ([DateTimeOffset]::Parse($text, [System.Globalization.CultureInfo]::InvariantCulture)).UtcDateTime
    }
    catch {
        return $null
    }
}

function Get-ConversationRecordId {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$Conversation
    )

    if ($Conversation.PSObject.Properties.Name -contains 'conversationId') {
        return [string]$Conversation.conversationId
    }

    return ''
}

function ConvertTo-ConversationRecordList {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$InputObject
    )

    if ($null -eq $InputObject) {
        return @()
    }

    if ($InputObject -is [System.Collections.IEnumerable] -and $InputObject -isnot [string] -and $InputObject -isnot [pscustomobject]) {
        return @($InputObject)
    }

    if ($InputObject.PSObject.Properties.Name -contains 'conversations') {
        return @($InputObject.conversations)
    }

    return @($InputObject)
}

function Get-ConversationParticipants {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$Conversation
    )

    if ($Conversation.PSObject.Properties.Name -contains 'participants' -and $null -ne $Conversation.participants) {
        return @($Conversation.participants)
    }

    return @()
}

function Get-ConversationSessions {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$Participant
    )

    if ($Participant.PSObject.Properties.Name -contains 'sessions' -and $null -ne $Participant.sessions) {
        return @($Participant.sessions)
    }

    return @()
}

function Get-ConversationSegments {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$Session
    )

    if ($Session.PSObject.Properties.Name -contains 'segments' -and $null -ne $Session.segments) {
        return @($Session.segments)
    }

    return @()
}

function Get-ConversationMetrics {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$Session
    )

    if ($Session.PSObject.Properties.Name -contains 'metrics' -and $null -ne $Session.metrics) {
        return @($Session.metrics)
    }

    return @()
}

function ConvertTo-DelimitedText {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object[]]$Values
    )

    $items = @($Values | Where-Object { $null -ne $_ -and [string]::IsNullOrWhiteSpace([string]$_) -eq $false } | ForEach-Object { [string]$_ } | Select-Object -Unique)
    if ($items.Count -eq 0) {
        return ''
    }

    return [string]::Join('|', $items)
}

function Get-ConversationMetricAggregate {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$Sessions,

        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [string]$MetricName,

        [switch]$ConvertMillisecondsToSeconds
    )

    $total = 0.0
    $hasValue = $false

    foreach ($session in @($Sessions)) {
        foreach ($metric in @(Get-ConversationMetrics -Session $session)) {
            if ([string]$metric.name -ne $MetricName) {
                continue
            }

            $rawValue = $metric.value
            $parsedValue = 0.0
            if ([double]::TryParse([string]$rawValue, [ref]$parsedValue)) {
                if ($ConvertMillisecondsToSeconds) {
                    $parsedValue = $parsedValue / 1000.0
                }

                $total += $parsedValue
                $hasValue = $true
            }
        }
    }

    if (-not $hasValue) {
        return $null
    }

    return [Math]::Round($total, 3)
}

function Get-ConversationPrimaryQueueId {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$Participants
    )

    foreach ($participant in @($Participants)) {
        foreach ($session in @(Get-ConversationSessions -Participant $participant)) {
            foreach ($segment in @(Get-ConversationSegments -Session $session)) {
                if ([string]$segment.segmentType -eq 'interact' -and [string]::IsNullOrWhiteSpace([string]$segment.queueId) -eq $false) {
                    return [string]$segment.queueId
                }
            }
        }
    }

    return ''
}

function ConvertTo-ConversationFact {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$Conversation
    )

    $conversationId = Get-ConversationRecordId -Conversation $Conversation
    $startUtc = ConvertTo-NullableUtcDateTime -Value $Conversation.conversationStart
    $endUtc = ConvertTo-NullableUtcDateTime -Value $Conversation.conversationEnd
    $participants = @(Get-ConversationParticipants -Conversation $Conversation)
    $agentParticipants = @($participants | Where-Object { [string]$_.purpose -eq 'agent' })
    $customerParticipants = @($participants | Where-Object { [string]$_.purpose -eq 'customer' })
    $allSessions = @()
    $allSegments = @()
    $allMetrics = @()
    $attributeCount = 0

    foreach ($participant in $participants) {
        $sessions = @(Get-ConversationSessions -Participant $participant)
        $allSessions += $sessions

        if ($participant.PSObject.Properties.Name -contains 'attributes' -and $null -ne $participant.attributes) {
            if ($participant.attributes -is [System.Collections.IDictionary]) {
                $attributeCount += @($participant.attributes.Keys).Count
            }
            else {
                $attributeCount += @($participant.attributes.PSObject.Properties).Count
            }
        }

        foreach ($session in $sessions) {
            $allSegments += @(Get-ConversationSegments -Session $session)
            $allMetrics += @(Get-ConversationMetrics -Session $session)
        }
    }

    $durationSec = $null
    if ($null -ne $startUtc -and $null -ne $endUtc) {
        $durationSec = [Math]::Round(($endUtc - $startUtc).TotalSeconds, 3)
    }

    $mediaTypes = @($allSessions | ForEach-Object { [string]$_.mediaType } | Where-Object { $_ } | Select-Object -Unique)
    $queueIds = @($allSegments | ForEach-Object { [string]$_.queueId } | Where-Object { $_ } | Select-Object -Unique)
    $wrapUpCodes = @($allSegments | ForEach-Object { [string]$_.wrapUpCode } | Where-Object { $_ } | Select-Object -Unique)
    $agentSessions = @($agentParticipants | ForEach-Object { @(Get-ConversationSessions -Participant $_) })

    return [pscustomobject][ordered]@{
        ConversationId = $conversationId
        ConversationStartUtc = if ($null -ne $startUtc) { $startUtc.ToString('o') } else { $null }
        ConversationEndUtc = if ($null -ne $endUtc) { $endUtc.ToString('o') } else { $null }
        ConversationStartDate = if ($null -ne $startUtc) { $startUtc.ToString('yyyy-MM-dd') } else { $null }
        ConversationStartHourUtc = if ($null -ne $startUtc) { $startUtc.Hour } else { $null }
        DurationSec = $durationSec
        OriginatingDirection = [string]$Conversation.originatingDirection
        PrimaryMediaType = if ($mediaTypes.Count -gt 0) { $mediaTypes[0] } else { '' }
        MediaTypes = ConvertTo-DelimitedText -Values $mediaTypes
        QueueId = Get-ConversationPrimaryQueueId -Participants $participants
        QueueIds = ConvertTo-DelimitedText -Values $queueIds
        WrapUpCodes = ConvertTo-DelimitedText -Values $wrapUpCodes
        ParticipantCount = $participants.Count
        AgentCount = $agentParticipants.Count
        CustomerCount = $customerParticipants.Count
        SessionCount = $allSessions.Count
        SegmentCount = $allSegments.Count
        MetricCount = $allMetrics.Count
        AttributeCount = $attributeCount
        AgentNames = ConvertTo-DelimitedText -Values @($agentParticipants | ForEach-Object { [string]$_.participantName })
        AgentUserIds = ConvertTo-DelimitedText -Values @($agentParticipants | ForEach-Object { [string]$_.userId })
        HandleSec = Get-ConversationMetricAggregate -Sessions $agentSessions -MetricName 'tHandle' -ConvertMillisecondsToSeconds
        TalkSec = Get-ConversationMetricAggregate -Sessions $agentSessions -MetricName 'tTalk' -ConvertMillisecondsToSeconds
        AcwSec = Get-ConversationMetricAggregate -Sessions $agentSessions -MetricName 'tAcw' -ConvertMillisecondsToSeconds
        HeldSec = Get-ConversationMetricAggregate -Sessions $agentSessions -MetricName 'tHeld' -ConvertMillisecondsToSeconds
        ConnectedCount = Get-ConversationMetricAggregate -Sessions $agentSessions -MetricName 'nConnected'
    }
}

function ConvertTo-ConversationParticipantFacts {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$Conversation
    )

    $conversationId = Get-ConversationRecordId -Conversation $Conversation
    $results = [System.Collections.Generic.List[object]]::new()
    $participants = @(Get-ConversationParticipants -Conversation $Conversation)

    for ($participantIndex = 0; $participantIndex -lt $participants.Count; $participantIndex++) {
        $participant = $participants[$participantIndex]
        $sessions = @(Get-ConversationSessions -Participant $participant)
        $attributes = @()
        if ($participant.PSObject.Properties.Name -contains 'attributes' -and $null -ne $participant.attributes) {
            if ($participant.attributes -is [System.Collections.IDictionary]) {
                $attributes = @($participant.attributes.Keys)
            }
            else {
                $attributes = @($participant.attributes.PSObject.Properties | ForEach-Object { $_.Name })
            }
        }

        $results.Add([pscustomobject][ordered]@{
            ConversationId = $conversationId
            ParticipantIndex = $participantIndex
            ParticipantId = [string]$participant.participantId
            ParticipantPurpose = [string]$participant.purpose
            ParticipantName = [string]$participant.participantName
            UserId = [string]$participant.userId
            ExternalContactId = [string]$participant.externalContactId
            SessionCount = $sessions.Count
            AttributeCount = $attributes.Count
        }) | Out-Null
    }

    return @($results)
}

function ConvertTo-ConversationSessionFacts {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$Conversation
    )

    $conversationId = Get-ConversationRecordId -Conversation $Conversation
    $results = [System.Collections.Generic.List[object]]::new()
    $participants = @(Get-ConversationParticipants -Conversation $Conversation)

    for ($participantIndex = 0; $participantIndex -lt $participants.Count; $participantIndex++) {
        $participant = $participants[$participantIndex]
        $sessions = @(Get-ConversationSessions -Participant $participant)

        for ($sessionIndex = 0; $sessionIndex -lt $sessions.Count; $sessionIndex++) {
            $session = $sessions[$sessionIndex]
            $segments = @(Get-ConversationSegments -Session $session)
            $metrics = @(Get-ConversationMetrics -Session $session)
            $segmentQueueIds = @($segments | ForEach-Object { [string]$_.queueId } | Where-Object { $_ } | Select-Object -Unique)

            $results.Add([pscustomobject][ordered]@{
                ConversationId = $conversationId
                ParticipantIndex = $participantIndex
                ParticipantPurpose = [string]$participant.purpose
                SessionIndex = $sessionIndex
                SessionId = [string]$session.sessionId
                MediaType = [string]$session.mediaType
                Direction = [string]$session.direction
                Provider = [string]$session.provider
                Ani = [string]$session.ani
                Dnis = [string]$session.dnis
                EdgeId = [string]$session.edgeId
                QueueIds = ConvertTo-DelimitedText -Values $segmentQueueIds
                SegmentCount = $segments.Count
                MetricCount = $metrics.Count
            }) | Out-Null
        }
    }

    return @($results)
}

function ConvertTo-ConversationSegmentFacts {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$Conversation
    )

    $conversationId = Get-ConversationRecordId -Conversation $Conversation
    $results = [System.Collections.Generic.List[object]]::new()
    $participants = @(Get-ConversationParticipants -Conversation $Conversation)

    for ($participantIndex = 0; $participantIndex -lt $participants.Count; $participantIndex++) {
        $participant = $participants[$participantIndex]
        $sessions = @(Get-ConversationSessions -Participant $participant)

        for ($sessionIndex = 0; $sessionIndex -lt $sessions.Count; $sessionIndex++) {
            $session = $sessions[$sessionIndex]
            $segments = @(Get-ConversationSegments -Session $session)

            for ($segmentIndex = 0; $segmentIndex -lt $segments.Count; $segmentIndex++) {
                $segment = $segments[$segmentIndex]
                $startUtc = ConvertTo-NullableUtcDateTime -Value $segment.segmentStart
                $endUtc = ConvertTo-NullableUtcDateTime -Value $segment.segmentEnd
                $durationSec = $null
                if ($null -ne $startUtc -and $null -ne $endUtc) {
                    $durationSec = [Math]::Round(($endUtc - $startUtc).TotalSeconds, 3)
                }

                $results.Add([pscustomobject][ordered]@{
                    ConversationId = $conversationId
                    ParticipantIndex = $participantIndex
                    ParticipantPurpose = [string]$participant.purpose
                    SessionIndex = $sessionIndex
                    SegmentIndex = $segmentIndex
                    SegmentType = [string]$segment.segmentType
                    QueueId = [string]$segment.queueId
                    DisconnectType = [string]$segment.disconnectType
                    WrapUpCode = [string]$segment.wrapUpCode
                    WrapUpNote = [string]$segment.wrapUpNote
                    Conference = [string]$segment.conference
                    SegmentStartUtc = if ($null -ne $startUtc) { $startUtc.ToString('o') } else { $null }
                    SegmentEndUtc = if ($null -ne $endUtc) { $endUtc.ToString('o') } else { $null }
                    DurationSec = $durationSec
                }) | Out-Null
            }
        }
    }

    return @($results)
}

function ConvertTo-ConversationMetricFacts {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$Conversation
    )

    $conversationId = Get-ConversationRecordId -Conversation $Conversation
    $results = [System.Collections.Generic.List[object]]::new()
    $participants = @(Get-ConversationParticipants -Conversation $Conversation)
    $msMetrics = @('thandle', 'ttalk', 'tacw', 'theld', 'tivr', 'twait', 'tanswered', 'tabandon', 'toffer', 'tdialing', 'tcontacting', 'tmonitoring')

    for ($participantIndex = 0; $participantIndex -lt $participants.Count; $participantIndex++) {
        $participant = $participants[$participantIndex]
        $sessions = @(Get-ConversationSessions -Participant $participant)

        for ($sessionIndex = 0; $sessionIndex -lt $sessions.Count; $sessionIndex++) {
            $session = $sessions[$sessionIndex]

            foreach ($metric in @(Get-ConversationMetrics -Session $session)) {
                $metricName = [string]$metric.name
                $parsedValue = $null
                $numericValue = 0.0
                if ([double]::TryParse([string]$metric.value, [ref]$numericValue)) {
                    $parsedValue = [Math]::Round($numericValue, 3)
                }

                $secondsValue = $null
                if ($null -ne $parsedValue -and $msMetrics -contains $metricName.ToLowerInvariant()) {
                    $secondsValue = [Math]::Round(($parsedValue / 1000.0), 3)
                }

                $emitDateUtc = ConvertTo-NullableUtcDateTime -Value $metric.emitDate
                $results.Add([pscustomobject][ordered]@{
                    ConversationId = $conversationId
                    ParticipantIndex = $participantIndex
                    ParticipantPurpose = [string]$participant.purpose
                    SessionIndex = $sessionIndex
                    MetricName = $metricName
                    MetricValue = $parsedValue
                    MetricValueSeconds = $secondsValue
                    EmitDateUtc = if ($null -ne $emitDateUtc) { $emitDateUtc.ToString('o') } else { $null }
                }) | Out-Null
            }
        }
    }

    return @($results)
}

function ConvertTo-ConversationAttributeFacts {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$Conversation
    )

    $conversationId = Get-ConversationRecordId -Conversation $Conversation
    $results = [System.Collections.Generic.List[object]]::new()
    $participants = @(Get-ConversationParticipants -Conversation $Conversation)

    for ($participantIndex = 0; $participantIndex -lt $participants.Count; $participantIndex++) {
        $participant = $participants[$participantIndex]
        if ($participant.PSObject.Properties.Name -notcontains 'attributes' -or $null -eq $participant.attributes) {
            continue
        }

        if ($participant.attributes -is [System.Collections.IDictionary]) {
            foreach ($key in $participant.attributes.Keys) {
                $results.Add([pscustomobject][ordered]@{
                    ConversationId = $conversationId
                    ParticipantIndex = $participantIndex
                    ParticipantPurpose = [string]$participant.purpose
                    AttributeName = [string]$key
                    AttributeValue = [string]$participant.attributes[$key]
                }) | Out-Null
            }

            continue
        }

        foreach ($property in $participant.attributes.PSObject.Properties) {
            $results.Add([pscustomobject][ordered]@{
                ConversationId = $conversationId
                ParticipantIndex = $participantIndex
                ParticipantPurpose = [string]$participant.purpose
                AttributeName = [string]$property.Name
                AttributeValue = [string]$property.Value
            }) | Out-Null
        }
    }

    return @($results)
}

function Resolve-ConversationDataPath {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [string]$SourcePath
    )

    $resolvedSource = if (Test-Path -Path $SourcePath) { (Resolve-Path -Path $SourcePath).Path } else { $null }
    if ($null -eq $resolvedSource) {
        throw "Source path '$($SourcePath)' was not found."
    }

    if (Test-Path -Path $resolvedSource -PathType Leaf) {
        return $resolvedSource
    }

    $candidates = @(
        (Join-Path -Path $resolvedSource -ChildPath 'data/analytics-conversation-details.jsonl'),
        (Join-Path -Path $resolvedSource -ChildPath 'analytics-conversation-details.jsonl')
    )

    foreach ($candidate in $candidates) {
        if (Test-Path -Path $candidate -PathType Leaf) {
            return (Resolve-Path -Path $candidate).Path
        }
    }

    $jsonlCandidate = Get-ChildItem -Path $resolvedSource -Filter '*.jsonl' -File -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -ne $jsonlCandidate) {
        return $jsonlCandidate.FullName
    }

    throw "No analytics conversation details JSONL file was found under '$($resolvedSource)'."
}
