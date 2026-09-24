# Pester 5 tests for src/query/ConversationDetailQuery.ps1 (pure logic only; no network, no WPF).
# Invoke-ConversationDetailQuery takes a -Request scriptblock; these tests inject a fake that
# serves canned pages per interval window and records the bodies it receives.
# Run: Invoke-Pester -Path .\tests\ConversationDetailQuery.Tests.ps1 -Output Detailed

BeforeAll {
    Set-StrictMode -Version Latest
    . (Join-Path $PSScriptRoot '..\src\query\ConversationDetailQuery.ps1')

    function New-Conv {
        param([string]$Id, [string]$Start = '2026-09-02T10:00:00.000Z')
        [pscustomobject]@{ conversationId = $Id; conversationStart = $Start; participants = @() }
    }

    function New-FakeQueryRequest {
        # Pages: hashtable interval -> array of pages (each page an array of conversations).
        # totalHits per interval is the sum of that interval's rows unless -Hits overrides it.
        # Calls: list of [pscustomobject] { Method, Path, Body (parsed) }.
        param([hashtable]$Pages, [System.Collections.Generic.List[object]]$Calls, [hashtable]$Hits = @{})
        return {
            param([string]$Method, [string]$Path, [string]$Body)
            $parsed = $Body | ConvertFrom-Json
            $Calls.Add([pscustomobject]@{ Method = $Method; Path = $Path; Body = $parsed; Raw = $Body }) | Out-Null
            $interval = [string]$parsed.interval
            if (-not $Pages.ContainsKey($interval)) { throw "HTTP 400 unexpected interval $interval" }
            # Each window's value is an array of pages; each page is an array of conversations.
            $windowPages = @($Pages[$interval])
            $n = [int]$parsed.paging.pageNumber
            $rows = if ($n -le $windowPages.Count) { @($windowPages[$n - 1]) } else { @() }
            $total = 0
            foreach ($p in $windowPages) { $total += @($p).Count }
            if ($Hits.ContainsKey($interval)) { $total = [int]$Hits[$interval] }
            [pscustomobject]@{ conversations = $rows; totalHits = $total }
        }.GetNewClosure()
    }

    $script:oneDay = '2026-09-01T04:00:00.000Z/2026-09-02T03:59:59.000Z'
    $script:baseBody = [ordered]@{
        interval                   = $script:oneDay
        startOfDayIntervalMatching = $true
        order                      = 'asc'
        orderBy                    = 'conversationStart'
        conversationFilters        = @(@{ type = 'and'; predicates = @(@{ dimension = 'originatingDirection'; value = 'inbound' }) })
        segmentFilters             = @(@{ type = 'and'; predicates = @(@{ dimension = 'mediaType'; value = 'voice' }) })
    }
}

Describe 'Split-ConversationQueryInterval' {
    It 'keeps an interval of exactly seven days as one window' {
        $w = @(Split-ConversationQueryInterval -Interval '2026-09-01T00:00:00.000Z/2026-09-08T00:00:00.000Z')
        $w.Count | Should -Be 1
        $w[0].Interval | Should -Be '2026-09-01T00:00:00.000Z/2026-09-08T00:00:00.000Z'
    }

    It 'splits a longer interval into consecutive windows that share boundaries and end exactly at the requested end' {
        $w = @(Split-ConversationQueryInterval -Interval '2026-09-01T04:00:00.000Z/2026-09-23T03:59:59.000Z')
        $w.Count | Should -Be 4
        $w[0].Interval | Should -Be '2026-09-01T04:00:00.000Z/2026-09-08T04:00:00.000Z'
        $w[1].StartUtc | Should -Be $w[0].EndUtc
        $w[3].Interval | Should -Be '2026-09-22T04:00:00.000Z/2026-09-23T03:59:59.000Z'
        ($w | ForEach-Object { $_.Index }) | Should -Be @(1, 2, 3, 4)
    }

    It 'honours a custom window size' {
        $w = @(Split-ConversationQueryInterval -Interval '2026-09-01T00:00:00.000Z/2026-09-04T00:00:00.000Z' -MaxWindowDays 1)
        $w.Count | Should -Be 3
    }

    It 'rejects inverted and malformed intervals' {
        { Split-ConversationQueryInterval -Interval '2026-09-08T00:00:00.000Z/2026-09-01T00:00:00.000Z' } | Should -Throw '*ends before it starts*'
        { Split-ConversationQueryInterval -Interval 'not-an-interval' } | Should -Throw '*not a start/end*'
    }
}

Describe 'Get-ConversationDetailQueryPlan' {
    It 'walks windows newest-first for descending queries' {
        $body = [ordered]@{ interval = '2026-09-01T00:00:00.000Z/2026-09-15T00:00:00.000Z'; order = 'desc' }
        $plan = Get-ConversationDetailQueryPlan -Body $body
        $plan.WindowCount | Should -Be 2
        $plan.Order | Should -Be 'desc'
        $plan.Windows[0].Index | Should -Be 2
        $plan.Windows[1].Index | Should -Be 1
    }

    It 'rejects a page size above the API cap' {
        { Get-ConversationDetailQueryPlan -Body $script:baseBody -PageSize 1000 } | Should -Throw '*between 1 and 100*'
    }

    It 'describes the plan in one sentence' {
        $plan = Get-ConversationDetailQueryPlan -Body ([ordered]@{ interval = '2026-09-01T00:00:00.000Z/2026-09-15T00:00:00.000Z'; order = 'desc' })
        $text = Get-ConversationDetailQueryPlanText -Plan $plan -StartOfDayMatching $true
        $text | Should -BeLike 'Interval spans 14 days: 2 consecutive query windows of up to 7 days*newest window first, 100 conversations per page.*dropped client-side.'
        (Get-ConversationDetailQueryPlanText -Plan (Get-ConversationDetailQueryPlan -Body $script:baseBody) -StartOfDayMatching $false) | Should -BeLike 'Interval spans * days: 1 query window, 100 conversations per page.'
    }
}

Describe 'New-ConversationDetailQueryPageBody' {
    It 'keeps every filter, replaces the interval, drops async-only keys, and sets paging' {
        $page = New-ConversationDetailQueryPageBody -Body $script:baseBody -Interval 'X/Y' -PageSize 100 -PageNumber 3
        $page.Contains('startOfDayIntervalMatching') | Should -BeFalse
        $page['interval'] | Should -Be 'X/Y'
        $page['order'] | Should -Be 'asc'
        $page['orderBy'] | Should -Be 'conversationStart'
        $page['conversationFilters'] -is [array] | Should -BeTrue
        @($page['conversationFilters']).Count | Should -Be 1
        @($page['segmentFilters'])[0].predicates[0].value | Should -Be 'voice'
        $page['paging']['pageSize'] | Should -Be 100
        $page['paging']['pageNumber'] | Should -Be 3
    }

    It 'accepts a PSCustomObject body too' {
        $obj = [pscustomobject]@{ interval = 'A/B'; order = 'desc'; limit = 5 }
        $page = New-ConversationDetailQueryPageBody -Body $obj -Interval 'C/D' -PageSize 50 -PageNumber 1
        $page['interval'] | Should -Be 'C/D'
        $page['order'] | Should -Be 'desc'
        $page.Contains('limit') | Should -BeFalse
    }
}

Describe 'Invoke-ConversationDetailQuery' {
    It 'pages one window until a short page and posts the filters with every page' {
        $calls = [System.Collections.Generic.List[object]]::new()
        $pages = @{ $script:oneDay = @( @((New-Conv 'a'), (New-Conv 'b')), @((New-Conv 'c')) ) }
        $req = New-FakeQueryRequest -Pages $pages -Calls $calls
        $r = Invoke-ConversationDetailQuery -Request $req -Body $script:baseBody -PageSize 2
        @($r.Conversations).conversationId | Should -Be @('a', 'b', 'c')
        $r.PagesRead | Should -Be 2
        $r.TotalHits | Should -Be 3
        $r.Completed | Should -BeTrue
        $calls.Count | Should -Be 2
        $calls[0].Method | Should -Be 'POST'
        $calls[0].Path | Should -Be '/api/v2/analytics/conversations/details/query'
        $calls[0].Body.paging.pageNumber | Should -Be 1
        $calls[1].Body.paging.pageNumber | Should -Be 2
        $calls[1].Body.paging.pageSize | Should -Be 2
        $calls[1].Body.conversationFilters[0].predicates[0].dimension | Should -Be 'originatingDirection'
        $calls[1].Body.segmentFilters[0].predicates[0].value | Should -Be 'voice'
        $calls[1].Body.PSObject.Properties.Name | Should -Not -Contain 'startOfDayIntervalMatching'
        # One-element filter lists must still serialize as JSON arrays or the API rejects the body.
        $calls[1].Raw | Should -Match '"conversationFilters":\s*\['
        $calls[1].Raw | Should -Match '"segmentFilters":\s*\['
        $calls[1].Raw | Should -Match '"predicates":\s*\['
    }

    It 'stops when totalHits is reached even if the last page is full' {
        $calls = [System.Collections.Generic.List[object]]::new()
        $pages = @{ $script:oneDay = @( ,@((New-Conv 'a'), (New-Conv 'b')) ) }
        $req = New-FakeQueryRequest -Pages $pages -Calls $calls
        $r = Invoke-ConversationDetailQuery -Request $req -Body $script:baseBody -PageSize 2
        $r.Conversations.Count | Should -Be 2
        $calls.Count | Should -Be 1
    }

    It 'returns nothing and one request for an empty window' {
        $calls = [System.Collections.Generic.List[object]]::new()
        $req = New-FakeQueryRequest -Pages @{ $script:oneDay = @() } -Calls $calls
        $r = Invoke-ConversationDetailQuery -Request $req -Body $script:baseBody
        $r.Conversations.Count | Should -Be 0
        $r.TotalHits | Should -Be 0
        $calls.Count | Should -Be 1
    }

    It 'queries every window of a long interval and de-duplicates conversations that straddle a boundary' {
        $calls = [System.Collections.Generic.List[object]]::new()
        $body = [ordered]@{ interval = '2026-09-01T00:00:00.000Z/2026-09-15T00:00:00.000Z'; order = 'asc' }
        $w1 = '2026-09-01T00:00:00.000Z/2026-09-08T00:00:00.000Z'
        $w2 = '2026-09-08T00:00:00.000Z/2026-09-15T00:00:00.000Z'
        $pages = @{
            $w1 = @( ,@((New-Conv 'a'), (New-Conv 'straddle')) )
            $w2 = @( ,@((New-Conv 'straddle'), (New-Conv 'z')) )
        }
        $req = New-FakeQueryRequest -Pages $pages -Calls $calls
        $seenPages = [System.Collections.Generic.List[object]]::new()
        $r = Invoke-ConversationDetailQuery -Request $req -Body $body -OnPage { param($p) $seenPages.Add($p) | Out-Null }
        @($r.Conversations).conversationId | Should -Be @('a', 'straddle', 'z')
        $r.Duplicates | Should -Be 1
        $r.WindowsCompleted | Should -Be 2
        $r.TotalHits | Should -Be 4
        $calls[0].Body.interval | Should -Be $w1
        $calls[1].Body.interval | Should -Be $w2
        $seenPages.Count | Should -Be 2
        $seenPages[1].WindowIndex | Should -Be 2
        $seenPages[1].WindowCount | Should -Be 2
        $seenPages[1].NewRows | Should -Be 1
        $seenPages[1].PageRows | Should -Be 2
        @($seenPages[1].Conversations).conversationId | Should -Be @('z')
        $seenPages[1].Collected | Should -Be 3
    }

    It 'walks windows newest-first when the order is desc' {
        $calls = [System.Collections.Generic.List[object]]::new()
        $body = [ordered]@{ interval = '2026-09-01T00:00:00.000Z/2026-09-15T00:00:00.000Z'; order = 'desc' }
        $w1 = '2026-09-01T00:00:00.000Z/2026-09-08T00:00:00.000Z'
        $w2 = '2026-09-08T00:00:00.000Z/2026-09-15T00:00:00.000Z'
        $req = New-FakeQueryRequest -Pages @{ $w1 = @( ,@((New-Conv 'old')) ); $w2 = @( ,@((New-Conv 'new')) ) } -Calls $calls
        $r = Invoke-ConversationDetailQuery -Request $req -Body $body
        $calls[0].Body.interval | Should -Be $w2
        @($r.Conversations).conversationId | Should -Be @('new', 'old')
    }

    It 'honours MaxPages and reports truncation with the rows read so far' {
        $calls = [System.Collections.Generic.List[object]]::new()
        $pages = @{ $script:oneDay = @( @((New-Conv 'a')), @((New-Conv 'b')), @((New-Conv 'c')) ) }
        $req = New-FakeQueryRequest -Pages $pages -Calls $calls -Hits @{ $script:oneDay = 3 }
        $r = Invoke-ConversationDetailQuery -Request $req -Body $script:baseBody -PageSize 1 -MaxPages 2
        $r.Conversations.Count | Should -Be 2
        $r.Truncated | Should -BeTrue
        $r.Completed | Should -BeFalse
        $r.TotalHits | Should -BeNullOrEmpty
        $calls.Count | Should -Be 2
    }

    It 'stops before the next request when ShouldStop returns true' {
        $calls = [System.Collections.Generic.List[object]]::new()
        $pages = @{ $script:oneDay = @( @((New-Conv 'a')), @((New-Conv 'b')) ) }
        $req = New-FakeQueryRequest -Pages $pages -Calls $calls -Hits @{ $script:oneDay = 2 }
        $stopAfterFirst = { $calls.Count -ge 1 }.GetNewClosure()
        $r = Invoke-ConversationDetailQuery -Request $req -Body $script:baseBody -PageSize 1 -ShouldStop $stopAfterFirst
        $r.Stopped | Should -BeTrue
        $r.Conversations.Count | Should -Be 1
        $calls.Count | Should -Be 1
    }

    It 'lets request failures propagate to the caller' {
        $req = { param($m, $p, $b) throw 'HTTP 429 too many requests' }
        { Invoke-ConversationDetailQuery -Request $req -Body $script:baseBody } | Should -Throw '*429*'
    }
}

Describe 'Select-ConversationsStartedOnOrAfter' {
    It 'drops conversations that started before 00:00 UTC of the interval start date and keeps the rest' {
        $convs = @(
            (New-Conv 'before' '2026-08-31T23:59:59.000Z'),
            (New-Conv 'early-same-day' '2026-09-01T01:00:00.000Z'),
            (New-Conv 'inside' '2026-09-01T12:00:00.000Z'),
            (New-Conv 'no-start' $null)
        )
        $r = Select-ConversationsStartedOnOrAfter -Conversations $convs -Interval $script:oneDay
        @($r.Conversations).conversationId | Should -Be @('early-same-day', 'inside', 'no-start')
        $r.Dropped | Should -Be 1
        $r.CutoffUtc | Should -Be ([DateTime]::new(2026, 9, 1, 0, 0, 0, [DateTimeKind]::Utc))
    }

    It 'handles an empty input' {
        $r = Select-ConversationsStartedOnOrAfter -Conversations @() -Interval $script:oneDay
        $r.Conversations.Count | Should -Be 0
        $r.Dropped | Should -Be 0
    }
}
