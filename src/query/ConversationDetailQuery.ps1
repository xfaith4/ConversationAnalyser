# ConversationDetailQuery.ps1 - Paged synchronous conversation detail queries for the
# Query Builder. Pure logic: no UI and no direct network calls. Invoke-ConversationDetailQuery
# takes a -Request scriptblock with the signature
#     param([string]$Method, [string]$Path, [string]$Body)
# that returns the parsed JSON response. The app passes a wrapper around Invoke-GcApiRequest;
# the Pester tests pass a fake. Endpoint used:
#   POST /api/v2/analytics/conversations/details/query
#
# Platform constraints this module works around:
#   - One query covers at most 7 days, so a longer interval is split into consecutive
#     windows that are queried one after another (see Split-ConversationQueryInterval).
#   - Results are paged with paging.pageSize (max 100) / paging.pageNumber; the response
#     carries totalHits for the whole window. Paging stops on a short page, when totalHits
#     is reached, at MaxPages, or when the caller's ShouldStop callback returns $true.
#   - A conversation with activity in two adjacent windows is returned by both; results are
#     de-duplicated by conversationId (first occurrence wins).
#   - The sync query has no startOfDayIntervalMatching flag. Select-ConversationsStartedOnOrAfter
#     applies the same rule client-side (conversationStart on/after 00:00 UTC of the interval
#     start date).

$script:CdqPath = '/api/v2/analytics/conversations/details/query'
$script:CdqDefaultPageSize = 100
$script:CdqMaxPageSize = 100
$script:CdqDefaultMaxWindowDays = 7
$script:CdqDefaultMaxPages = 1000
$script:CdqBodyKeysDroppedForSyncQuery = @('startOfDayIntervalMatching', 'limit', 'paging')

function Get-CdqProperty {
    # Safe property read for PSCustomObject / hashtable / $null. Returns $null when absent.
    param([AllowNull()][object]$Object, [Parameter(Mandatory)][string]$Name)
    if ($null -eq $Object) { return $null }
    if ($Object -is [System.Collections.IDictionary]) {
        if ($Object.Contains($Name)) { return $Object[$Name] }
        return $null
    }
    if ($Object -is [string] -or $Object -is [ValueType]) { return $null }
    $prop = $Object.PSObject.Properties[$Name]
    if ($null -eq $prop) { return $null }
    return $prop.Value
}

function Get-CdqPropertyName {
    # Key names of a dictionary or the property names of an object, in their original order.
    param([AllowNull()][object]$Object)
    if ($null -eq $Object) { return @() }
    if ($Object -is [System.Collections.IDictionary]) { return @($Object.Keys | ForEach-Object { [string]$_ }) }
    return @($Object.PSObject.Properties | ForEach-Object { $_.Name })
}

function ConvertTo-CdqUtcDateTime {
    # Accepts DateTime, DateTimeOffset, or ISO-8601 text. Returns a UTC DateTime or $null.
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

function Format-CdqInterval {
    param([Parameter(Mandatory)][DateTime]$StartUtc, [Parameter(Mandatory)][DateTime]$EndUtc)
    return "$($StartUtc.ToString('yyyy-MM-ddTHH:mm:ss.fffZ'))/$($EndUtc.ToString('yyyy-MM-ddTHH:mm:ss.fffZ'))"
}

function ConvertFrom-CdqInterval {
    # 'start/end' ISO-8601 interval -> StartUtc / EndUtc. Throws on malformed or inverted input.
    param([AllowNull()][string]$Interval)
    if ([string]::IsNullOrWhiteSpace($Interval)) { throw 'The query interval is empty.' }
    $parts = $Interval.Split('/')
    if ($parts.Count -ne 2) { throw "Interval '$Interval' is not a start/end ISO-8601 interval." }
    $start = ConvertTo-CdqUtcDateTime $parts[0]
    $end = ConvertTo-CdqUtcDateTime $parts[1]
    if ($null -eq $start -or $null -eq $end) { throw "Interval '$Interval' has an unparseable start or end." }
    if ($end -le $start) { throw "Interval '$Interval' ends before it starts." }
    return [pscustomobject]@{ StartUtc = $start; EndUtc = $end }
}

function Split-ConversationQueryInterval {
    # Splits a UTC interval into consecutive windows of at most MaxWindowDays each. Adjacent
    # windows share their boundary instant; the platform treats the interval end as exclusive,
    # so nothing is skipped and only conversations that straddle a boundary repeat.
    param(
        [Parameter(Mandatory)][string]$Interval,
        [int]$MaxWindowDays = $script:CdqDefaultMaxWindowDays
    )
    if ($MaxWindowDays -lt 1) { throw 'MaxWindowDays must be at least 1.' }
    $range = ConvertFrom-CdqInterval $Interval
    $windows = [System.Collections.Generic.List[object]]::new()
    $cursor = $range.StartUtc
    $index = 0
    while ($cursor -lt $range.EndUtc) {
        $windowEnd = $cursor.AddDays($MaxWindowDays)
        if ($windowEnd -gt $range.EndUtc) { $windowEnd = $range.EndUtc }
        $index++
        $windows.Add([pscustomobject]@{
                Index    = $index
                StartUtc = $cursor
                EndUtc   = $windowEnd
                Interval = Format-CdqInterval -StartUtc $cursor -EndUtc $windowEnd
            }) | Out-Null
        $cursor = $windowEnd
    }
    return @($windows)
}

function Get-ConversationDetailQueryPlan {
    # Describes how a Query Builder body will be executed: the windows it is split into, the
    # page size, and the window order (descending queries walk the windows newest-first so the
    # merged result keeps the requested order).
    param(
        [Parameter(Mandatory)][object]$Body,
        [int]$PageSize = $script:CdqDefaultPageSize,
        [int]$MaxWindowDays = $script:CdqDefaultMaxWindowDays
    )
    if ($PageSize -lt 1 -or $PageSize -gt $script:CdqMaxPageSize) {
        throw "PageSize must be between 1 and $($script:CdqMaxPageSize)."
    }
    $interval = [string](Get-CdqProperty -Object $Body -Name 'interval')
    $windows = @(Split-ConversationQueryInterval -Interval $interval -MaxWindowDays $MaxWindowDays)
    $order = [string](Get-CdqProperty -Object $Body -Name 'order')
    if ($order -eq 'desc') { [array]::Reverse($windows) }
    $range = ConvertFrom-CdqInterval $interval
    return [pscustomobject]@{
        Interval      = $interval
        StartUtc      = $range.StartUtc
        EndUtc        = $range.EndUtc
        TotalDays     = [Math]::Round(($range.EndUtc - $range.StartUtc).TotalDays, 2)
        Windows       = $windows
        WindowCount   = $windows.Count
        MaxWindowDays = $MaxWindowDays
        PageSize      = $PageSize
        Order         = $(if ($order -eq 'desc') { 'desc' } else { 'asc' })
    }
}

function Get-ConversationDetailQueryPlanText {
    # One human-readable sentence for the UI and the activity log.
    param([Parameter(Mandatory)][pscustomobject]$Plan, [bool]$StartOfDayMatching)
    $windowText = if ($Plan.WindowCount -eq 1) {
        '1 query window'
    }
    else {
        "$($Plan.WindowCount) consecutive query windows of up to $($Plan.MaxWindowDays) days (the API caps one query at $($Plan.MaxWindowDays) days)"
    }
    $orderText = if ($Plan.WindowCount -gt 1 -and $Plan.Order -eq 'desc') { ', newest window first' } else { '' }
    $filterText = if ($StartOfDayMatching) {
        ' Conversations that started before 00:00 UTC of the interval start date are dropped client-side.'
    }
    else {
        ''
    }
    return "Interval spans $($Plan.TotalDays) days: $windowText$orderText, $($Plan.PageSize) conversations per page.$filterText"
}

function New-ConversationDetailQueryPageBody {
    # Copies the Query Builder body for one window and page. Keys that only the async job
    # understands (startOfDayIntervalMatching, limit) are dropped; paging is set explicitly.
    param(
        [Parameter(Mandatory)][object]$Body,
        [Parameter(Mandatory)][string]$Interval,
        [Parameter(Mandatory)][int]$PageSize,
        [Parameter(Mandatory)][int]$PageNumber
    )
    $page = [ordered]@{}
    $isMap = $Body -is [System.Collections.IDictionary]
    foreach ($key in (Get-CdqPropertyName -Object $Body)) {
        if ($script:CdqBodyKeysDroppedForSyncQuery -contains $key) { continue }
        if ($key -eq 'interval') { $page['interval'] = $Interval; continue }
        # Assign the value in place (not through a function or an if-expression, both of which
        # unroll a one-element array) so filter lists still serialize as JSON [ ... ].
        if ($isMap) { $page[$key] = $Body[$key] } else { $page[$key] = $Body.PSObject.Properties[$key].Value }
    }
    if (-not $page.Contains('interval')) { $page['interval'] = $Interval }
    $page['paging'] = [ordered]@{ pageSize = $PageSize; pageNumber = $PageNumber }
    return $page
}

function Invoke-ConversationDetailQuery {
    # Runs the paged query for every window of the plan and returns every distinct conversation.
    # OnPage (optional) is called after each page with a progress object; ShouldStop (optional)
    # is consulted before every request and ends the run early when it returns $true.
    param(
        [Parameter(Mandatory)][scriptblock]$Request,
        [Parameter(Mandatory)][object]$Body,
        [int]$PageSize = $script:CdqDefaultPageSize,
        [int]$MaxWindowDays = $script:CdqDefaultMaxWindowDays,
        [int]$MaxPages = $script:CdqDefaultMaxPages,
        [scriptblock]$OnPage,
        [scriptblock]$ShouldStop
    )
    if ($MaxPages -lt 1) { throw 'MaxPages must be at least 1.' }
    $plan = Get-ConversationDetailQueryPlan -Body $Body -PageSize $PageSize -MaxWindowDays $MaxWindowDays

    $seen = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    $all = [System.Collections.Generic.List[object]]::new()
    $pagesRead = 0
    $duplicates = 0
    $windowsCompleted = 0
    $totalHits = 0
    $totalHitsKnown = $true
    $truncated = $false
    $stopped = $false
    $windowCount = $plan.WindowCount

    :windows foreach ($window in $plan.Windows) {
        $pageNumber = 1
        $windowCollected = 0
        $windowHits = $null
        while ($true) {
            if ($ShouldStop -and (& $ShouldStop)) { $stopped = $true; break windows }
            if ($pagesRead -ge $MaxPages) { $truncated = $true; break windows }

            $pageBody = New-ConversationDetailQueryPageBody -Body $Body -Interval $window.Interval -PageSize $PageSize -PageNumber $pageNumber
            $json = $pageBody | ConvertTo-Json -Depth 12
            $response = & $Request 'POST' $script:CdqPath $json
            $pagesRead++

            $batch = @(Get-CdqProperty -Object $response -Name 'conversations')
            $batch = @($batch | Where-Object { $null -ne $_ })
            $hits = Get-CdqProperty -Object $response -Name 'totalHits'
            if ($null -ne $hits -and $null -eq $windowHits) { $windowHits = [int]$hits }

            $fresh = [System.Collections.Generic.List[object]]::new()
            foreach ($conv in $batch) {
                $id = [string](Get-CdqProperty -Object $conv -Name 'conversationId')
                if ([string]::IsNullOrWhiteSpace($id) -or $seen.Add($id)) { $fresh.Add($conv) | Out-Null }
                else { $duplicates++ }
            }
            foreach ($conv in $fresh) { $all.Add($conv) | Out-Null }
            $windowCollected += $batch.Count

            if ($OnPage) {
                & $OnPage ([pscustomobject]@{
                        WindowIndex    = $window.Index
                        WindowCount    = $windowCount
                        WindowInterval = $window.Interval
                        PageNumber     = $pageNumber
                        PageRows       = $batch.Count
                        NewRows        = $fresh.Count
                        Conversations  = @($fresh)
                        WindowHits     = $windowHits
                        Collected      = $all.Count
                        PagesRead      = $pagesRead
                    })
            }

            $hasMore = ($batch.Count -ge $PageSize)
            if ($hasMore -and $null -ne $windowHits -and $windowCollected -ge $windowHits) { $hasMore = $false }
            if (-not $hasMore) { break }
            $pageNumber++
        }
        $windowsCompleted++
        if ($null -ne $windowHits) { $totalHits += $windowHits } else { $totalHitsKnown = $false }
    }

    return [pscustomobject]@{
        Conversations    = @($all)
        Plan             = $plan
        WindowsCompleted = $windowsCompleted
        PagesRead        = $pagesRead
        TotalHits        = $(if ($totalHitsKnown -and $windowsCompleted -eq $windowCount) { $totalHits } else { $null })
        Duplicates       = $duplicates
        Truncated        = $truncated
        Stopped          = $stopped
        Completed        = [bool](-not $truncated -and -not $stopped)
    }
}

function Select-ConversationsStartedOnOrAfter {
    # Client-side equivalent of the async job's startOfDayIntervalMatching: keeps conversations
    # whose conversationStart is on/after 00:00 UTC of the interval start date. Conversations
    # without a parseable start are kept so nothing disappears silently.
    param(
        [AllowNull()][object[]]$Conversations,
        [Parameter(Mandatory)][string]$Interval
    )
    $range = ConvertFrom-CdqInterval $Interval
    $cutoff = [DateTime]::SpecifyKind($range.StartUtc.Date, [DateTimeKind]::Utc)
    $kept = [System.Collections.Generic.List[object]]::new()
    $dropped = 0
    foreach ($conv in @($Conversations)) {
        if ($null -eq $conv) { continue }
        $start = ConvertTo-CdqUtcDateTime (Get-CdqProperty -Object $conv -Name 'conversationStart')
        if ($null -ne $start -and $start -lt $cutoff) { $dropped++; continue }
        $kept.Add($conv) | Out-Null
    }
    return [pscustomobject]@{
        Conversations = @($kept)
        Dropped       = $dropped
        CutoffUtc     = $cutoff
    }
}
