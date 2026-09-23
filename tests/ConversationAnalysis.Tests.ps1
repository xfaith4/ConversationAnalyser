# Pester 5 tests for the user-name resolution in src/analysis/ConversationAnalysis.ps1
# (pure logic only; no network, no WPF).
# Run: Invoke-Pester -Path .\tests\ConversationAnalysis.Tests.ps1 -Output Detailed

BeforeAll {
    . (Join-Path $PSScriptRoot '..\src\analysis\ConversationAnalysis.ps1')

    $script:agentA = '11111111-1111-1111-1111-111111111111'
    $script:agentB = '22222222-2222-2222-2222-222222222222'

    function New-TestConversation {
        # Minimal analytics detail record: customer -> ACD queue -> agent A -> agent B, all voice.
        param([string]$Id = 'conv-1', [string]$AgentAName = '', [string]$AgentBName = 'Bea Record')
        $mk = { param($purpose, $userId, $name, $startOffset)
            $start = [DateTime]::new(2026, 9, 1, 10, 0, $startOffset, [DateTimeKind]::Utc)
            [pscustomobject]@{
                purpose         = $purpose
                userId          = $userId
                participantName = $name
                sessions        = @([pscustomobject]@{
                        mediaType = 'voice'; direction = 'inbound'; sessionId = "s-$purpose-$startOffset"
                        segments  = @([pscustomobject]@{ segmentType = 'interact'; segmentStart = $start.ToString('o'); segmentEnd = $start.AddSeconds(30).ToString('o'); queueId = 'q-1' })
                        metrics   = @([pscustomobject]@{ name = 'tHandle'; value = 30000; emitDate = $start.ToString('o') },
                                      [pscustomobject]@{ name = 'nConnected'; value = 1; emitDate = $start.ToString('o') })
                    })
            }
        }
        [pscustomobject]@{
            conversationId       = $Id
            conversationStart    = '2026-09-01T10:00:00.000Z'
            conversationEnd      = '2026-09-01T10:02:00.000Z'
            originatingDirection = 'inbound'
            divisionIds          = @('d-1')
            participants         = @(
                (& $mk 'customer' $null 'Cust Omer' 0),
                (& $mk 'acd' $null 'Sales' 1),
                (& $mk 'agent' $script:agentA $AgentAName 2),
                (& $mk 'agent' $script:agentB $AgentBName 3)
            )
        }
    }
}

Describe 'New-ConversationLookupTable' {
    It 'includes an empty users map' {
        $t = New-ConversationLookupTable
        $t.ContainsKey('users') | Should -BeTrue
        $t['users'].Count | Should -Be 0
    }
}

Describe 'Get-ConversationUserIds' {
    It 'returns distinct agent/user IDs in first-seen order and ignores other participants' {
        $c1 = New-TestConversation -Id 'c1'
        $c2 = New-TestConversation -Id 'c2'
        $c2.participants[3].userId = '33333333-3333-3333-3333-333333333333'
        $c2.participants += [pscustomobject]@{ purpose = 'user'; userId = '44444444-4444-4444-4444-444444444444'; participantName = ''; sessions = @() }
        $c2.participants += [pscustomobject]@{ purpose = 'customer'; userId = '55555555-5555-5555-5555-555555555555'; participantName = ''; sessions = @() }
        $ids = @(Get-ConversationUserIds -Conversations @($c1, $c2, $null))
        $ids | Should -Be @($script:agentA, $script:agentB, '33333333-3333-3333-3333-333333333333', '44444444-4444-4444-4444-444444444444')
    }

    It 'skips agents without a user ID' {
        $c = New-TestConversation
        $c.participants[2].userId = $null
        $c.participants[3].userId = ''
        @(Get-ConversationUserIds -Conversations @($c)).Count | Should -Be 0
    }
}

Describe 'Resolve-AgentLabel' {
    It 'prefers the users lookup, then participantName, then the raw key' {
        $lookups = New-ConversationLookupTable
        $lookups['users']['u1'] = 'Directory Name'
        $names = @{ u1 = 'Record Name'; u2 = 'Record Only' }
        Resolve-AgentLabel -Lookups $lookups -AgentNames $names -Key 'u1' | Should -Be 'Directory Name'
        Resolve-AgentLabel -Lookups $lookups -AgentNames $names -Key 'u2' | Should -Be 'Record Only'
        Resolve-AgentLabel -Lookups $lookups -AgentNames $names -Key 'u3' | Should -Be 'u3'
        Resolve-AgentLabel -Lookups $null -AgentNames $null -Key 'u3' | Should -Be 'u3'
        Resolve-AgentLabel -Lookups $lookups -AgentNames $names -Key '' | Should -Be ''
    }

    It 'ignores blank lookup values' {
        $lookups = New-ConversationLookupTable
        $lookups['users']['u1'] = '   '
        Resolve-AgentLabel -Lookups $lookups -AgentNames @{ u1 = 'Record Name' } -Key 'u1' | Should -Be 'Record Name'
    }
}

Describe 'Agent names across grid, detail and report' {
    BeforeAll {
        $script:conv = New-TestConversation
        $script:profile = Get-ConversationProfile -Conversation $script:conv
        $script:resolved = New-ConversationLookupTable
        $script:resolved['users'][$script:agentA] = 'Ada Directory'
        $script:resolved['users'][$script:agentB] = 'Bea Directory'
    }

    It 'shows IDs (or the record name) in the grid row when users are not resolved' {
        $row = ConvertTo-FlatRow -ConversationProfile $script:profile -Conversation $script:conv -AttrCols @() -Lookups (New-ConversationLookupTable)
        $row.AgentName | Should -Be $script:agentA
        $row.AgentUserId | Should -Be $script:agentA
        $row.FinalAgent | Should -Be 'Bea Record'
        $row.AgentPath | Should -Be "$($script:agentA) > Bea Record"
    }

    It 'shows directory names in the grid row once users are resolved' {
        $row = ConvertTo-FlatRow -ConversationProfile $script:profile -Conversation $script:conv -AttrCols @() -Lookups $script:resolved
        $row.AgentName | Should -Be 'Ada Directory'
        $row.AgentUserId | Should -Be $script:agentA
        $row.FinalAgent | Should -Be 'Bea Directory'
        $row.AgentPath | Should -Be 'Ada Directory > Bea Directory'
        $row.AgentCount | Should -Be 2
    }

    It 'names agent participants in the session rows (ID when unresolved) and leaves other purposes untouched' {
        $rows = @(Get-ConversationSessionRows -Conversation $script:conv -Lookups $script:resolved)
        ($rows | Where-Object Purpose -eq 'agent').Name | Should -Be @('Ada Directory', 'Bea Directory')
        ($rows | Where-Object Purpose -eq 'customer').Name | Should -Be 'Cust Omer'
        $unresolved = @(Get-ConversationSessionRows -Conversation $script:conv -Lookups (New-ConversationLookupTable))
        ($unresolved | Where-Object Purpose -eq 'agent').Name | Should -Be @($script:agentA, 'Bea Record')
    }

    It 'includes DisconnectType and ErrorCodes in the default grid columns' {
        $defaults = @(Get-DefaultGridColumnNames)
        $defaults | Should -Contain 'DisconnectType'
        $defaults | Should -Contain 'ErrorCodes'
        $all = @(Get-FlatRowColumnNames)
        $all | Should -Contain 'ErrorSegments'
        $all | Should -Contain 'ErrorDisconnect'
    }

    It 'labels the Agents report table and keeps the UserId column' {
        $report = Get-ConversationReport -Profiles @($script:profile) -Lookups $script:resolved -Source 'test' -QueryInterval ''
        $agents = @($report.Tables['Agents'].Rows | Sort-Object UserId)
        $agents.Agent | Should -Be @('Ada Directory', 'Bea Directory')
        $agents.UserId | Should -Be @($script:agentA, $script:agentB)
    }
}
Describe 'Segment error codes' {
    BeforeAll {
        $script:iceCode = 'error.ininedgecontrol.connection.webrtc.endpoint.disconnect.iceIdleDetection'
        $script:dtlsCode = 'error.ininedgecontrol.connection.webrtc.endpoint.disconnect.dtlsPeerDisconnect'
        $script:errConv = New-TestConversation -Id 'err-1'
        $seg = $script:errConv.participants[3].sessions[0].segments[0]
        $seg | Add-Member -NotePropertyName disconnectType -NotePropertyValue 'ERROR'
        $seg | Add-Member -NotePropertyName errorCode -NotePropertyValue $script:iceCode
        $script:errProfile = Get-ConversationProfile -Conversation $script:errConv
        $script:cleanProfile = Get-ConversationProfile -Conversation (New-TestConversation -Id 'ok-1')
    }

    It 'captures the code, the carrying purpose and the error disconnect on the profile' {
        $script:errProfile.ErrorCodes | Should -Be @($script:iceCode)
        $script:errProfile.ErrorSegmentCount | Should -Be 1
        $script:errProfile.ErrorDisconnect | Should -BeTrue
        $script:errProfile.ErrorEvents[0].Purpose | Should -Be 'agent'
        $script:errProfile.ErrorEvents[0].DisconnectType | Should -Be 'ERROR'
        $script:errProfile.DisconnectType | Should -Be 'ERROR'
        $script:cleanProfile.ErrorCodes.Count | Should -Be 0
        $script:cleanProfile.ErrorDisconnect | Should -BeFalse
    }

    It 'surfaces the code in the flat row' {
        $row = ConvertTo-FlatRow -ConversationProfile $script:errProfile -Conversation $script:errConv -AttrCols @() -Lookups (New-ConversationLookupTable)
        $row.ErrorCodes | Should -Be $script:iceCode
        $row.ErrorSegments | Should -Be 1
        $row.ErrorDisconnect | Should -BeTrue
        $row.DisconnectType | Should -Be 'ERROR'
    }

    It 'shows the code on the segment rows of the detail panel' {
        $rows = @(Get-ConversationSegmentRows -Conversation $script:errConv -Lookups (New-ConversationLookupTable))
        @($rows | Where-Object { $_.ErrorCode -eq $script:iceCode }).Count | Should -Be 1
    }

    It 'builds the Error Codes table, the drill-down table and the observations' {
        $report = Get-ConversationReport -Profiles @($script:errProfile, $script:cleanProfile) -Lookups (New-ConversationLookupTable) -Source 'test' -QueryInterval ''
        $codes = @($report.Tables['Error Codes'].Rows)
        $codes.Count | Should -Be 1
        $codes[0].ErrorCode | Should -Be $script:iceCode
        $codes[0].Conversations | Should -Be 1
        $codes[0].SharePct | Should -Be 50
        $codes[0].Purposes | Should -Be 'agent'
        $codes[0].DisconnectTypes | Should -Be 'ERROR'
        $codes[0].Meaning | Should -Match 'WebRTC'
        $drill = @($report.Tables['Error Conversations'].Rows)
        $drill.Count | Should -Be 1
        $drill[0].ConversationId | Should -Be 'err-1'
        $drill[0].ErrorCodes | Should -Be $script:iceCode
        @($report.Observations | Where-Object { $_ -match 'carry a segment error code' }).Count | Should -Be 1
        @($report.Observations | Where-Object { $_ -match 'WebRTC phone drops in 1 conversations \(iceIdleDetection 1\)' }).Count | Should -Be 1
    }

    It 'maps known codes and families to a meaning and leaves unknown codes blank' {
        Get-ErrorCodeMeaning -Code $script:dtlsCode | Should -Match 'DTLS'
        Get-ErrorCodeMeaning -Code 'error.ininedgecontrol.connection.webrtc.something.else' | Should -Match 'WebRTC'
        Get-ErrorCodeMeaning -Code 'error.ininedgecontrol.other' | Should -Match 'Edge'
        Get-ErrorCodeMeaning -Code 'error.unknown.thing' | Should -Be ''
        Get-ErrorCodeMeaning -Code '' | Should -Be ''
    }

    It 'reports no error tables rows or observations when nothing errored' {
        $report = Get-ConversationReport -Profiles @($script:cleanProfile) -Lookups (New-ConversationLookupTable) -Source 'test' -QueryInterval ''
        @($report.Tables['Error Codes'].Rows).Count | Should -Be 0
        @($report.Tables['Error Conversations'].Rows).Count | Should -Be 0
        @($report.Observations | Where-Object { $_ -match 'error code|WebRTC' }).Count | Should -Be 0
    }
}

Describe 'Business time zone (US Eastern) interval handling' {
    It 'resolves the Eastern time zone' {
        (Get-BusinessTimeZone).BaseUtcOffset | Should -Be ([TimeSpan]::FromHours(-5))
    }

    It 'converts Eastern wall-clock to UTC honouring daylight saving' {
        # EDT (UTC-4): 2026-07-01 00:00 Eastern = 04:00Z
        $summer = ConvertFrom-BusinessTime ([DateTime]::new(2026, 7, 1, 0, 0, 0))
        $summer.Kind | Should -Be ([DateTimeKind]::Utc)
        $summer | Should -Be ([DateTime]::new(2026, 7, 1, 4, 0, 0, [DateTimeKind]::Utc))
        # EST (UTC-5): 2026-01-15 23:59:59 Eastern = 2026-01-16 04:59:59Z
        $winter = ConvertFrom-BusinessTime ([DateTime]::new(2026, 1, 15, 23, 59, 59))
        $winter | Should -Be ([DateTime]::new(2026, 1, 16, 4, 59, 59, [DateTimeKind]::Utc))
    }

    It 'ignores the Kind of the input and always reads it as Eastern' {
        $asLocal = [DateTime]::SpecifyKind([DateTime]::new(2026, 7, 1, 0, 0, 0), [DateTimeKind]::Local)
        $asUtc = [DateTime]::SpecifyKind([DateTime]::new(2026, 7, 1, 0, 0, 0), [DateTimeKind]::Utc)
        (ConvertFrom-BusinessTime $asLocal) | Should -Be ([DateTime]::new(2026, 7, 1, 4, 0, 0, [DateTimeKind]::Utc))
        (ConvertFrom-BusinessTime $asUtc) | Should -Be ([DateTime]::new(2026, 7, 1, 4, 0, 0, [DateTimeKind]::Utc))
    }

    It 'moves a spring-forward gap time forward one hour instead of throwing' {
        # 2026-03-08 02:30 does not exist in Eastern; it becomes 03:30 EDT = 07:30Z
        $gap = ConvertFrom-BusinessTime ([DateTime]::new(2026, 3, 8, 2, 30, 0))
        $gap | Should -Be ([DateTime]::new(2026, 3, 8, 7, 30, 0, [DateTimeKind]::Utc))
    }

    It 'round-trips UTC back to Eastern' {
        $utc = [DateTime]::new(2026, 9, 1, 4, 0, 0, [DateTimeKind]::Utc)
        (ConvertTo-BusinessTime $utc) | Should -Be ([DateTime]::new(2026, 9, 1, 0, 0, 0))
        (ConvertTo-BusinessTime ([DateTime]::new(2026, 9, 1, 4, 0, 0, [DateTimeKind]::Unspecified))) | Should -Be ([DateTime]::new(2026, 9, 1, 0, 0, 0))
    }

    It 'labels the zone with the abbreviation and offset in force' {
        Get-BusinessTimeZoneLabel -UtcValue ([DateTime]::new(2026, 7, 1, 12, 0, 0, [DateTimeKind]::Utc)) | Should -Be 'US Eastern (EDT, UTC-04:00)'
        Get-BusinessTimeZoneLabel -UtcValue ([DateTime]::new(2026, 1, 1, 12, 0, 0, [DateTimeKind]::Utc)) | Should -Be 'US Eastern (EST, UTC-05:00)'
    }

    It 'parses a start/end interval string into UTC' {
        $w = ConvertFrom-IntervalString '2026-09-01T04:00:00.000Z/2026-09-02T03:59:59.000Z'
        $w.StartUtc | Should -Be ([DateTime]::new(2026, 9, 1, 4, 0, 0, [DateTimeKind]::Utc))
        $w.EndUtc | Should -Be ([DateTime]::new(2026, 9, 2, 3, 59, 59, [DateTimeKind]::Utc))
        ConvertFrom-IntervalString '' | Should -BeNullOrEmpty
        ConvertFrom-IntervalString 'not-an-interval' | Should -BeNullOrEmpty
    }
}

Describe 'Get-ConversationIntervalCoverage' {
    BeforeAll {
        $script:interval = '2026-09-01T04:00:00.000Z/2026-09-02T03:59:59.000Z'
        function New-StartOnly { param([string]$Id, [object]$Start) [pscustomobject]@{ conversationId = $Id; conversationStart = $Start } }
    }

    It 'classifies conversationStart against the requested interval' {
        $convs = @(
            (New-StartOnly 'inside-1' '2026-09-01T04:00:00.000Z'),
            (New-StartOnly 'inside-2' '2026-09-02T03:59:59.000Z'),
            (New-StartOnly 'before-month' '2026-08-02T10:00:00.000Z'),
            (New-StartOnly 'before-second' '2026-09-01T03:59:59.999Z'),
            (New-StartOnly 'after' '2026-09-02T04:00:00.000Z'),
            (New-StartOnly 'missing' $null)
        )
        $cov = Get-ConversationIntervalCoverage -Conversations $convs -Interval $script:interval
        $cov.Total | Should -Be 6
        $cov.Inside | Should -Be 2
        $cov.StartedBefore | Should -Be 2
        $cov.StartedAfter | Should -Be 1
        $cov.MissingStart | Should -Be 1
        $cov.EarliestStartUtc | Should -Be ([DateTime]::new(2026, 8, 2, 10, 0, 0, [DateTimeKind]::Utc))
        $cov.LatestStartUtc | Should -Be ([DateTime]::new(2026, 9, 2, 4, 0, 0, [DateTimeKind]::Utc))
    }

    It 'accepts DateTime values as produced by ConvertFrom-Json' {
        $json = '[{"conversationId":"a","conversationStart":"2026-09-01T12:00:00.000Z"},{"conversationId":"b","conversationStart":"2026-07-01T12:00:00.000Z"}]'
        $cov = Get-ConversationIntervalCoverage -Conversations @($json | ConvertFrom-Json) -Interval $script:interval
        $cov.Inside | Should -Be 1
        $cov.StartedBefore | Should -Be 1
    }

    It 'handles an empty collection and rejects a malformed interval' {
        $cov = Get-ConversationIntervalCoverage -Conversations @() -Interval $script:interval
        $cov.Total | Should -Be 0
        $cov.EarliestStartUtc | Should -BeNullOrEmpty
        { Get-ConversationIntervalCoverage -Conversations @() -Interval 'bad' } | Should -Throw
    }
}
