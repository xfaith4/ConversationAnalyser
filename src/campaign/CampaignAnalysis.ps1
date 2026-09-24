# CampaignAnalysis.ps1 - Outbound campaign discovery, status, rules, and events for the
# Campaign Analysis tab. Pure logic: no UI and no direct network calls. Every function that
# talks to Genesys Cloud takes a -Request scriptblock with the signature
#     param([string]$Method, [string]$Path, [hashtable]$QueryParams)
# that returns the parsed JSON response. The app passes a wrapper around Invoke-GcApiRequest;
# the Pester tests pass a fake. Endpoints used:
#   GET /api/v2/outbound/campaigns/all                         every campaign (voice, sms, email)
#   GET /api/v2/outbound/campaigns/{id}                        voice campaign configuration
#   GET /api/v2/outbound/campaigns/{id}/progress               contacts called / total
#   GET /api/v2/outbound/campaigns/{id}/diagnostics            outstanding, scheduled, errors
#   GET /api/v2/outbound/campaigns/{id}/stats                  live dialer stats (voice only)
#   GET /api/v2/outbound/diagnostics/campaigns/{id}/summary    diagnostics summary
#   GET /api/v2/outbound/messagingcampaigns/{id}[/progress]    sms / email campaigns
#   GET /api/v2/outbound/campaignrules                         all rules, matched client-side
#   GET /api/v2/outbound/events                                org-wide events, matched client-side


$script:CampaignDefaultPageSize = 100
$script:CampaignMaxListPages = 100
$script:CampaignMaxEventPages = 5

function Get-CampaignProperty {
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

function Get-CampaignRefName {
    # DomainEntityRef -> name, falling back to the id, then blank.
    param([AllowNull()][object]$Ref)
    if ($null -eq $Ref) { return '' }
    if ($Ref -is [string]) { return $Ref }
    $name = Get-CampaignProperty -Object $Ref -Name 'name'
    if (-not [string]::IsNullOrWhiteSpace([string]$name)) { return [string]$name }
    $id = Get-CampaignProperty -Object $Ref -Name 'id'
    if ($null -ne $id) { return [string]$id }
    return ''
}

function Get-CampaignRefId {
    param([AllowNull()][object]$Ref)
    if ($null -eq $Ref) { return '' }
    if ($Ref -is [string]) { return $Ref }
    $id = Get-CampaignProperty -Object $Ref -Name 'id'
    if ($null -ne $id) { return [string]$id }
    return ''
}

function ConvertTo-CampaignLocalTime {
    # ISO-8601 timestamp -> 'yyyy-MM-dd HH:mm:ss' local time, or blank when missing. Unparseable
    # input is returned unchanged so nothing is silently dropped.
    param([AllowNull()][object]$Value)
    if ($null -eq $Value) { return '' }
    $text = [string]$Value
    if ([string]::IsNullOrWhiteSpace($text)) { return '' }
    $parsed = [DateTime]::MinValue
    $styles = [System.Globalization.DateTimeStyles]::RoundtripKind
    if ([DateTime]::TryParse($text, [System.Globalization.CultureInfo]::InvariantCulture, $styles, [ref]$parsed)) {
        return $parsed.ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss')
    }
    return $text
}

function Invoke-CampaignRequest {
    param(
        [Parameter(Mandatory)][scriptblock]$Request,
        [Parameter(Mandatory)][string]$Path,
        [string]$Method = 'GET',
        [hashtable]$QueryParams
    )
    if ($null -eq $QueryParams) { $QueryParams = @{} }
    return & $Request $Method $Path $QueryParams
}

function Get-CampaignPagedEntities {
    # Pages a pageNumber/pageSize list endpoint and returns every entity. Stops on pageCount,
    # on a short page, or at MaxPages. OnProgress (optional) receives the page number.
    param(
        [Parameter(Mandatory)][scriptblock]$Request,
        [Parameter(Mandatory)][string]$Path,
        [hashtable]$QueryParams,
        [int]$PageSize = $script:CampaignDefaultPageSize,
        [int]$MaxPages = $script:CampaignMaxListPages,
        [scriptblock]$OnProgress
    )
    $all = [System.Collections.Generic.List[object]]::new()
    $pageNumber = 1
    $hasMore = $true
    while ($hasMore -and $pageNumber -le $MaxPages) {
        if ($OnProgress) { & $OnProgress $pageNumber }
        $q = @{ pageSize = [string]$PageSize; pageNumber = [string]$pageNumber }
        if ($QueryParams) { foreach ($k in $QueryParams.Keys) { $q[$k] = [string]$QueryParams[$k] } }
        $response = Invoke-CampaignRequest -Request $Request -Path $Path -QueryParams $q
        $entities = @(Get-CampaignProperty -Object $response -Name 'entities')
        foreach ($entity in $entities) { if ($null -ne $entity) { $all.Add($entity) | Out-Null } }
        $pageCount = Get-CampaignProperty -Object $response -Name 'pageCount'
        $hasMore = if ($null -ne $pageCount) { $pageNumber -lt [int]$pageCount } else { $entities.Count -ge $PageSize }
        $pageNumber++
    }
    return [pscustomobject]@{
        Entities  = @($all)
        PagesRead = $pageNumber - 1
        Truncated = [bool]($hasMore -and $pageNumber -gt $MaxPages)
    }
}

# -----------------------------------------------------------------------------
# Campaign list
# -----------------------------------------------------------------------------

function Get-CampaignMediaKind {
    # 'voice' campaigns live under /outbound/campaigns; sms and email under /outbound/messagingcampaigns.
    param([AllowNull()][string]$MediaType)
    $m = ([string]$MediaType).Trim().ToLowerInvariant()
    if ([string]::IsNullOrWhiteSpace($m) -or $m -eq 'voice') { return 'voice' }
    return 'messaging'
}

function Get-CampaignApiPaths {
    param([Parameter(Mandatory)][string]$CampaignId, [AllowNull()][string]$MediaType)
    $id = [Uri]::EscapeDataString($CampaignId)
    if ((Get-CampaignMediaKind -MediaType $MediaType) -eq 'voice') {
        return [pscustomobject]@{
            Kind        = 'voice'
            Detail      = "/api/v2/outbound/campaigns/$id"
            Progress    = "/api/v2/outbound/campaigns/$id/progress"
            Diagnostics = "/api/v2/outbound/campaigns/$id/diagnostics"
            Stats       = "/api/v2/outbound/campaigns/$id/stats"
            Summary     = "/api/v2/outbound/diagnostics/campaigns/$id/summary"
        }
    }
    return [pscustomobject]@{
        Kind        = 'messaging'
        Detail      = "/api/v2/outbound/messagingcampaigns/$id"
        Progress    = "/api/v2/outbound/messagingcampaigns/$id/progress"
        Diagnostics = $null
        Stats       = $null
        Summary     = "/api/v2/outbound/diagnostics/campaigns/$id/summary"
    }
}

function ConvertTo-CampaignRow {
    # Flattens a CommonCampaign (from /outbound/campaigns/all) or a full Campaign into a grid row.
    param([Parameter(Mandatory)][object]$Campaign)
    $mediaType = [string](Get-CampaignProperty -Object $Campaign -Name 'mediaType')
    if ([string]::IsNullOrWhiteSpace($mediaType)) { $mediaType = 'voice' }
    return [pscustomobject]@{
        Name       = [string](Get-CampaignProperty -Object $Campaign -Name 'name')
        MediaType  = $mediaType.ToLowerInvariant()
        Status     = [string](Get-CampaignProperty -Object $Campaign -Name 'campaignStatus')
        Division   = Get-CampaignRefName -Ref (Get-CampaignProperty -Object $Campaign -Name 'division')
        Modified   = ConvertTo-CampaignLocalTime -Value (Get-CampaignProperty -Object $Campaign -Name 'dateModified')
        Created    = ConvertTo-CampaignLocalTime -Value (Get-CampaignProperty -Object $Campaign -Name 'dateCreated')
        Version    = [string](Get-CampaignProperty -Object $Campaign -Name 'version')
        CampaignId = [string](Get-CampaignProperty -Object $Campaign -Name 'id')
        Raw        = $Campaign
    }
}

function Get-OutboundCampaignList {
    param(
        [Parameter(Mandatory)][scriptblock]$Request,
        [int]$MaxPages = $script:CampaignMaxListPages,
        [scriptblock]$OnProgress
    )
    $page = Get-CampaignPagedEntities -Request $Request -Path '/api/v2/outbound/campaigns/all' -MaxPages $MaxPages -OnProgress $OnProgress
    $rows = foreach ($c in $page.Entities) { ConvertTo-CampaignRow -Campaign $c }
    $sorted = @($rows | Sort-Object -Property @{ Expression = { $_.Name.ToLowerInvariant() } }, CampaignId)
    return [pscustomobject]@{ Rows = $sorted; PagesRead = $page.PagesRead; Truncated = $page.Truncated }
}

function Select-CampaignRows {
    # Case-insensitive substring search across name, id, status, media type, and division.
    # Multiple words must all match (each in any field).
    param([AllowNull()][object[]]$Rows, [AllowNull()][string]$Text)
    if ($null -eq $Rows) { return @() }
    $terms = @(([string]$Text) -split '\s+' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($terms.Count -eq 0) { return @($Rows) }
    return @($Rows | Where-Object {
            $row = $_
            $hay = (@($row.Name, $row.CampaignId, $row.Status, $row.MediaType, $row.Division) -join "`n")
            $ok = $true
            foreach ($t in $terms) { if ($hay.IndexOf($t, [StringComparison]::OrdinalIgnoreCase) -lt 0) { $ok = $false; break } }
            $ok
        })
}

# -----------------------------------------------------------------------------
# Status snapshot: configuration + progress + diagnostics + stats + summary
# -----------------------------------------------------------------------------

function Get-CampaignStatusSnapshot {
    # Fetches every status source independently. A failure in one source is recorded in
    # Errors and does not stop the others. Each section is $null when unavailable.
    param(
        [Parameter(Mandatory)][scriptblock]$Request,
        [Parameter(Mandatory)][string]$CampaignId,
        [AllowNull()][string]$MediaType,
        [scriptblock]$OnProgress
    )
    $paths = Get-CampaignApiPaths -CampaignId $CampaignId -MediaType $MediaType
    $sections = [ordered]@{
        Detail      = $paths.Detail
        Progress    = $paths.Progress
        Diagnostics = $paths.Diagnostics
        Stats       = $paths.Stats
        Summary     = $paths.Summary
    }
    $result = [ordered]@{
        CampaignId  = $CampaignId
        MediaKind   = $paths.Kind
        FetchedAt   = [DateTime]::Now
        Detail      = $null
        Progress    = $null
        Diagnostics = $null
        Stats       = $null
        Summary     = $null
        Errors      = @()
        Sources     = @()
    }
    $errors = [System.Collections.Generic.List[string]]::new()
    $sources = [System.Collections.Generic.List[string]]::new()
    foreach ($name in $sections.Keys) {
        $path = $sections[$name]
        if ([string]::IsNullOrWhiteSpace($path)) { continue }
        if ($OnProgress) { & $OnProgress $name }
        try {
            $result[$name] = Invoke-CampaignRequest -Request $Request -Path $path
            $sources.Add($name) | Out-Null
        }
        catch {
            $errors.Add(('{0} ({1}): {2}' -f $name, $path, $_.Exception.Message)) | Out-Null
        }
    }
    $result['Errors'] = @($errors)
    $result['Sources'] = @($sources)
    return [pscustomobject]$result
}

function New-CampaignTile {
    param([string]$Label, [AllowNull()][object]$Value, [string]$Caption = '', [string]$Group = '')
    $text = if ($null -eq $Value) { '' } elseif ($Value -is [bool]) { if ($Value) { 'Yes' } else { 'No' } } else { [string]$Value }
    return [pscustomobject]@{ Group = $Group; Label = $Label; Value = $text; Caption = $Caption }
}

function Format-CampaignRefList {
    param([AllowNull()][object]$Refs)
    if ($null -eq $Refs) { return '' }
    $names = foreach ($r in @($Refs)) { $n = Get-CampaignRefName -Ref $r; if ($n) { $n } }
    return (@($names) -join ', ')
}

function Test-CampaignScalar {
    param([AllowNull()][object]$Value)
    return ($Value -is [string] -or $Value -is [ValueType])
}

function ConvertTo-CampaignScalarTiles {
    # Generic renderer for objects whose schema is not pinned down (summary, stats, callableContacts):
    # every top-level scalar becomes a tile; one level of nested objects is flattened with a prefix.
    param([AllowNull()][object]$Object, [string]$Group, [string[]]$Skip = @(), [int]$Depth = 0)
    $tiles = [System.Collections.Generic.List[object]]::new()
    if ($null -eq $Object -or (Test-CampaignScalar -Value $Object)) { return @() }
    foreach ($prop in $Object.PSObject.Properties) {
        $name = $prop.Name
        if ($Skip -contains $name -or $name -eq 'selfUri') { continue }
        $value = $prop.Value
        if ($null -eq $value) { continue }
        if (Test-CampaignScalar -Value $value) {
            $shown = if ($value -is [string] -and $value -match '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}') { ConvertTo-CampaignLocalTime -Value $value } else { $value }
            $tiles.Add((New-CampaignTile -Label $name -Value $shown -Group $Group)) | Out-Null
        }
        elseif ($value -is [System.Collections.IEnumerable]) {
            $items = @($value)
            if ($items.Count -eq 0) { continue }
            if (Test-CampaignScalar -Value $items[0]) {
                $tiles.Add((New-CampaignTile -Label $name -Value ($items -join ', ') -Group $Group)) | Out-Null
            }
            else {
                $names = Format-CampaignRefList -Refs $items
                $shown = if ($names) { $names } else { "$($items.Count) item(s)" }
                $tiles.Add((New-CampaignTile -Label $name -Value $shown -Group $Group)) | Out-Null
            }
        }
        elseif ($Depth -lt 1) {
            $refId = Get-CampaignProperty -Object $value -Name 'id'
            $propCount = @($value.PSObject.Properties).Count
            if ($null -ne $refId -and $propCount -le 3) {
                $tiles.Add((New-CampaignTile -Label $name -Value (Get-CampaignRefName -Ref $value) -Group $Group)) | Out-Null
            }
            else {
                foreach ($t in @(ConvertTo-CampaignScalarTiles -Object $value -Group $Group -Depth ($Depth + 1))) {
                    $t.Label = "$name.$($t.Label)"
                    $tiles.Add($t) | Out-Null
                }
            }
        }
    }
    return @($tiles)
}

function ConvertTo-CampaignConfigTiles {
    # Configuration tiles from the campaign detail (voice Campaign or MessagingCampaign).
    param([AllowNull()][object]$Detail, [string]$MediaKind = 'voice')
    if ($null -eq $Detail) { return @() }
    $g = 'Configuration'
    $tiles = [System.Collections.Generic.List[object]]::new()
    $add = { param($label, $value, $caption) $tiles.Add((New-CampaignTile -Label $label -Value $value -Caption $caption -Group $g)) | Out-Null }
    $p = { param($n) Get-CampaignProperty -Object $Detail -Name $n }

    & $add 'Status' (& $p 'campaignStatus') ''
    & $add 'Division' (Get-CampaignRefName -Ref (& $p 'division')) ''
    & $add 'Contact list' (Get-CampaignRefName -Ref (& $p 'contactList')) ''
    if ($MediaKind -eq 'voice') {
        & $add 'Dialing mode' (& $p 'dialingMode') ''
        & $add 'Queue' (Get-CampaignRefName -Ref (& $p 'queue')) ''
        & $add 'Script' (Get-CampaignRefName -Ref (& $p 'script')) ''
        & $add 'Caller name' (& $p 'callerName') ''
        & $add 'Caller address' (& $p 'callerAddress') ''
        & $add 'Abandon rate target' (& $p 'abandonRate') 'percent'
        & $add 'Outbound lines' (& $p 'outboundLineCount') ''
        & $add 'Max calls per agent' (& $p 'maxCallsPerAgent') ''
        & $add 'No-answer timeout' (& $p 'noAnswerTimeout') 'seconds'
        & $add 'Preview timeout' (& $p 'previewTimeOutSeconds') 'seconds'
        & $add 'Priority' (& $p 'priority') ''
        & $add 'Call analysis response set' (Get-CampaignRefName -Ref (& $p 'callAnalysisResponseSet')) ''
        & $add 'Call analysis language' (& $p 'callAnalysisLanguage') ''
        & $add 'Edge group' (Get-CampaignRefName -Ref (& $p 'edgeGroup')) ''
        & $add 'Site' (Get-CampaignRefName -Ref (& $p 'site')) ''
        $phoneCols = @(& $p 'phoneColumns')
        if ($phoneCols.Count -gt 0) {
            $cols = foreach ($c in $phoneCols) { '{0} ({1})' -f (Get-CampaignProperty -Object $c -Name 'columnName'), (Get-CampaignProperty -Object $c -Name 'type') }
            & $add 'Phone columns' ($cols -join ', ') ''
        }
    }
    else {
        & $add 'Messages per minute' (& $p 'messagesPerMinute') ''
        $sms = & $p 'smsConfig'
        if ($null -ne $sms) {
            & $add 'SMS sender' (Get-CampaignProperty -Object $sms -Name 'senderSmsPhoneNumber') ''
            & $add 'SMS message column' (Get-CampaignProperty -Object $sms -Name 'messageColumn') ''
            & $add 'SMS phone column' (Get-CampaignProperty -Object $sms -Name 'phoneColumn') ''
        }
        $email = & $p 'emailConfig'
        if ($null -ne $email) {
            & $add 'Email columns' ((@(Get-CampaignProperty -Object $email -Name 'emailColumns')) -join ', ') ''
            & $add 'Email from' (Get-CampaignRefName -Ref (Get-CampaignProperty -Object $email -Name 'fromAddress')) ''
        }
    }
    & $add 'Always running' (& $p 'alwaysRunning') ''
    & $add 'Callable time set' (Get-CampaignRefName -Ref (& $p 'callableTimeSet')) ''
    & $add 'DNC lists' (Format-CampaignRefList -Refs (& $p 'dncLists')) ''
    & $add 'Rule sets' (Format-CampaignRefList -Refs (& $p 'ruleSets')) ''
    & $add 'Contact list filters' (Format-CampaignRefList -Refs (& $p 'contactListFilters')) ''
    $sorts = @(& $p 'contactSorts')
    if ($sorts.Count -gt 0) {
        $s = foreach ($cs in $sorts) { '{0} {1}' -f (Get-CampaignProperty -Object $cs -Name 'fieldName'), (Get-CampaignProperty -Object $cs -Name 'direction') }
        & $add 'Contact sorts' ($s -join ', ') ''
    }
    & $add 'Version' (& $p 'version') ''
    & $add 'Created' (ConvertTo-CampaignLocalTime -Value (& $p 'dateCreated')) ''
    & $add 'Modified' (ConvertTo-CampaignLocalTime -Value (& $p 'dateModified')) ''
    $errs = @(& $p 'errors')
    if ($errs.Count -gt 0) {
        $e = foreach ($x in $errs) {
            $m = Get-CampaignProperty -Object $x -Name 'error'
            if ($null -eq $m) { $m = $x }
            [string]$m
        }
        & $add 'Configuration errors' ($e -join '; ') ''
    }
    return @($tiles | Where-Object { -not [string]::IsNullOrWhiteSpace($_.Value) })
}

function ConvertTo-CampaignStatusTiles {
    # Progress / stats / diagnostics / summary tiles. Known fields get friendly labels and
    # captions; anything else the API returns is still shown through the generic renderer.
    param([Parameter(Mandatory)][object]$Snapshot)
    $tiles = [System.Collections.Generic.List[object]]::new()

    $progress = $Snapshot.Progress
    if ($null -ne $progress) {
        $g = 'Progress'
        $called = Get-CampaignProperty -Object $progress -Name 'numberOfContactsCalled'
        $messaged = Get-CampaignProperty -Object $progress -Name 'numberOfContactsMessaged'
        $total = Get-CampaignProperty -Object $progress -Name 'totalNumberOfContacts'
        $pct = Get-CampaignProperty -Object $progress -Name 'percentage'
        if ($null -ne $called) { $tiles.Add((New-CampaignTile -Label 'Contacts called' -Value $called -Caption "of $total" -Group $g)) | Out-Null }
        if ($null -ne $messaged) { $tiles.Add((New-CampaignTile -Label 'Contacts messaged' -Value $messaged -Caption "of $total" -Group $g)) | Out-Null }
        if ($null -ne $total -and $null -eq $called -and $null -eq $messaged) { $tiles.Add((New-CampaignTile -Label 'Total contacts' -Value $total -Group $g)) | Out-Null }
        if ($null -ne $pct) { $tiles.Add((New-CampaignTile -Label 'Complete' -Value ('{0}%' -f $pct) -Group $g)) | Out-Null }
        $skipped = Get-CampaignProperty -Object $progress -Name 'numberOfContactsSkipped'
        if ($null -ne $skipped -and -not (Test-CampaignScalar -Value $skipped)) {
            $parts = foreach ($sp in $skipped.PSObject.Properties) { '{0}: {1}' -f $sp.Name, $sp.Value }
            if (@($parts).Count -gt 0) { $tiles.Add((New-CampaignTile -Label 'Contacts skipped' -Value ($parts -join ', ') -Group $g)) | Out-Null }
        }
        elseif ($null -ne $skipped) { $tiles.Add((New-CampaignTile -Label 'Contacts skipped' -Value $skipped -Group $g)) | Out-Null }
        $skip = @('numberOfContactsCalled', 'numberOfContactsMessaged', 'totalNumberOfContacts', 'percentage', 'numberOfContactsSkipped', 'campaign', 'contactList')
        foreach ($t in @(ConvertTo-CampaignScalarTiles -Object $progress -Group $g -Skip $skip)) { $tiles.Add($t) | Out-Null }
    }

    $stats = $Snapshot.Stats
    if ($null -ne $stats) {
        $g = 'Live stats'
        $known = [ordered]@{
            contactRate              = @('Contact rate', 'connects per attempt')
            idleAgents               = @('Idle agents', '')
            effectiveIdleAgents      = @('Effective idle agents', '')
            adjustedCallsPerAgent    = @('Calls per agent', 'pacing')
            outstandingCalls         = @('Outstanding calls', 'dialed, not yet resolved')
            scheduledCalls           = @('Scheduled calls', 'callbacks and reschedules')
            timeZoneRescheduledCalls = @('Time-zone rescheduled', '')
            linesUtilization         = @('Line utilization', 'percent')
        }
        foreach ($k in $known.Keys) {
            $v = Get-CampaignProperty -Object $stats -Name $k
            if ($null -ne $v) { $tiles.Add((New-CampaignTile -Label $known[$k][0] -Value $v -Caption $known[$k][1] -Group $g)) | Out-Null }
        }
        foreach ($t in @(ConvertTo-CampaignScalarTiles -Object $stats -Group $g -Skip @($known.Keys))) { $tiles.Add($t) | Out-Null }
    }

    $diag = $Snapshot.Diagnostics
    if ($null -ne $diag) {
        $g = 'Diagnostics'
        $known = [ordered]@{
            outstandingInteractionsCount = @('Outstanding interactions', 'in progress')
            scheduledInteractionsCount   = @('Scheduled interactions', '')
            timeZoneRescheduledCalls     = @('Time-zone rescheduled', '')
        }
        foreach ($k in $known.Keys) {
            $v = Get-CampaignProperty -Object $diag -Name $k
            if ($null -ne $v) { $tiles.Add((New-CampaignTile -Label $known[$k][0] -Value $v -Caption $known[$k][1] -Group $g)) | Out-Null }
        }
        $callable = Get-CampaignProperty -Object $diag -Name 'callableContacts'
        if ($null -ne $callable) {
            foreach ($t in @(ConvertTo-CampaignScalarTiles -Object $callable -Group $g)) { $t.Label = 'Callable: ' + $t.Label; $tiles.Add($t) | Out-Null }
        }
        $skillStats = Get-CampaignProperty -Object $diag -Name 'campaignSkillStatistics'
        if ($null -ne $skillStats) {
            foreach ($t in @(ConvertTo-CampaignScalarTiles -Object $skillStats -Group $g)) { $t.Label = 'Skills: ' + $t.Label; $tiles.Add($t) | Out-Null }
        }
        $errs = @(Get-CampaignProperty -Object $diag -Name 'campaignErrors')
        if ($errs.Count -gt 0) {
            $lines = foreach ($e in $errs) {
                $m = Get-CampaignProperty -Object $e -Name 'error'
                if ($null -eq $m) { $m = Get-CampaignProperty -Object $e -Name 'message' }
                if ($null -eq $m) { $m = $e }
                [string]$m
            }
            $tiles.Add((New-CampaignTile -Label 'Campaign errors' -Value ($lines -join '; ') -Caption "$($errs.Count) error(s)" -Group $g)) | Out-Null
        }
        else {
            $tiles.Add((New-CampaignTile -Label 'Campaign errors' -Value 'none' -Group $g)) | Out-Null
        }
        $skip = @($known.Keys) + @('callableContacts', 'campaignSkillStatistics', 'campaignErrors')
        foreach ($t in @(ConvertTo-CampaignScalarTiles -Object $diag -Group $g -Skip $skip)) { $tiles.Add($t) | Out-Null }
    }

    $summary = $Snapshot.Summary
    if ($null -ne $summary) {
        foreach ($t in @(ConvertTo-CampaignScalarTiles -Object $summary -Group 'Summary')) { $tiles.Add($t) | Out-Null }
    }

    return @($tiles | Where-Object { -not [string]::IsNullOrWhiteSpace($_.Value) })
}

# -----------------------------------------------------------------------------
# Campaign rules
# -----------------------------------------------------------------------------

function Get-CampaignRuleEntityIds {
    # IDs of every campaign / messaging campaign referenced by a CampaignRuleEntities object.
    param([AllowNull()][object]$Entities)
    if ($null -eq $Entities) { return @() }
    $ids = [System.Collections.Generic.List[string]]::new()
    foreach ($field in @('campaigns', 'emailCampaigns', 'smsCampaigns')) {
        foreach ($ref in @(Get-CampaignProperty -Object $Entities -Name $field)) {
            $id = Get-CampaignRefId -Ref $ref
            if ($id) { $ids.Add($id) | Out-Null }
        }
    }
    return @($ids)
}

function Get-CampaignRuleRole {
    # 'trigger' when the campaign is watched by the rule, 'target' when an action acts on it,
    # 'trigger+target' for both, '' when unrelated.
    param([Parameter(Mandatory)][object]$Rule, [Parameter(Mandatory)][string]$CampaignId)
    $isTrigger = (Get-CampaignRuleEntityIds -Entities (Get-CampaignProperty -Object $Rule -Name 'campaignRuleEntities')) -contains $CampaignId
    $isTarget = $false
    foreach ($action in @(Get-CampaignProperty -Object $Rule -Name 'campaignRuleActions')) {
        $ae = Get-CampaignProperty -Object $action -Name 'campaignRuleActionEntities'
        if ((Get-CampaignRuleEntityIds -Entities $ae) -contains $CampaignId) { $isTarget = $true; break }
        $useTriggering = Get-CampaignProperty -Object $ae -Name 'useTriggeringEntity'
        if ($isTrigger -and $useTriggering -eq $true) { $isTarget = $true; break }
    }
    if ($isTrigger -and $isTarget) { return 'trigger+target' }
    if ($isTrigger) { return 'trigger' }
    if ($isTarget) { return 'target' }
    return ''
}

function Format-CampaignRuleCondition {
    param([object]$Condition)
    $type = Get-CampaignProperty -Object $Condition -Name 'conditionType'
    $params = Get-CampaignProperty -Object $Condition -Name 'parameters'
    $op = Get-CampaignProperty -Object $params -Name 'operator'
    $val = Get-CampaignProperty -Object $params -Name 'value'
    $mode = Get-CampaignProperty -Object $params -Name 'dialingMode'
    $text = [string]$type
    if ($op -or $null -ne $val) { $text += " $op $val" }
    if ($mode) { $text += " [$mode]" }
    return $text.Trim()
}

function Format-CampaignRuleAction {
    param([object]$Action)
    $type = Get-CampaignProperty -Object $Action -Name 'actionType'
    $params = Get-CampaignProperty -Object $Action -Name 'parameters'
    $text = [string]$type
    if ($null -ne $params -and -not (Test-CampaignScalar -Value $params)) {
        $bits = foreach ($pp in $params.PSObject.Properties) {
            if ($null -ne $pp.Value -and -not [string]::IsNullOrWhiteSpace([string]$pp.Value)) { '{0}={1}' -f $pp.Name, $pp.Value }
        }
        if (@($bits).Count -gt 0) { $text += ' (' + ($bits -join ', ') + ')' }
    }
    $ae = Get-CampaignProperty -Object $Action -Name 'campaignRuleActionEntities'
    $targets = [System.Collections.Generic.List[string]]::new()
    foreach ($field in @('campaigns', 'sequences', 'emailCampaigns', 'smsCampaigns')) {
        $names = Format-CampaignRefList -Refs (Get-CampaignProperty -Object $ae -Name $field)
        if ($names) { $targets.Add($names) | Out-Null }
    }
    if ((Get-CampaignProperty -Object $ae -Name 'useTriggeringEntity') -eq $true) { $targets.Add('triggering entity') | Out-Null }
    if ($targets.Count -gt 0) { $text += ' -> ' + ($targets -join ', ') }
    return $text.Trim()
}

function ConvertTo-CampaignRuleRow {
    param([Parameter(Mandatory)][object]$Rule, [Parameter(Mandatory)][string]$CampaignId)
    $conds = foreach ($c in @(Get-CampaignProperty -Object $Rule -Name 'campaignRuleConditions')) { Format-CampaignRuleCondition -Condition $c }
    $acts = foreach ($a in @(Get-CampaignProperty -Object $Rule -Name 'campaignRuleActions')) { Format-CampaignRuleAction -Action $a }
    $matchAny = Get-CampaignProperty -Object $Rule -Name 'matchAnyConditions'
    $joiner = if ($matchAny) { ' OR ' } else { ' AND ' }
    return [pscustomobject]@{
        Name       = [string](Get-CampaignProperty -Object $Rule -Name 'name')
        Enabled    = [bool](Get-CampaignProperty -Object $Rule -Name 'enabled')
        Role       = Get-CampaignRuleRole -Rule $Rule -CampaignId $CampaignId
        Conditions = (@($conds) -join $joiner)
        Actions    = (@($acts) -join '; ')
        Modified   = ConvertTo-CampaignLocalTime -Value (Get-CampaignProperty -Object $Rule -Name 'dateModified')
        RuleId     = [string](Get-CampaignProperty -Object $Rule -Name 'id')
        Raw        = $Rule
    }
}

function Get-CampaignRuleRows {
    # The rules endpoint filters by rule name only, so every rule is paged once and matched
    # client-side against the campaign's ID (as trigger entity or action target).
    param(
        [Parameter(Mandatory)][scriptblock]$Request,
        [Parameter(Mandatory)][string]$CampaignId,
        [int]$MaxPages = $script:CampaignMaxListPages,
        [scriptblock]$OnProgress
    )
    $page = Get-CampaignPagedEntities -Request $Request -Path '/api/v2/outbound/campaignrules' -MaxPages $MaxPages -OnProgress $OnProgress
    $rows = foreach ($rule in $page.Entities) {
        $role = Get-CampaignRuleRole -Rule $rule -CampaignId $CampaignId
        if ($role) { ConvertTo-CampaignRuleRow -Rule $rule -CampaignId $CampaignId }
    }
    return [pscustomobject]@{ Rows = @($rows); TotalRules = $page.Entities.Count; PagesRead = $page.PagesRead; Truncated = $page.Truncated }
}

# -----------------------------------------------------------------------------
# Outbound events
# -----------------------------------------------------------------------------

function Test-CampaignEventMatch {
    # True when the event references the campaign: entity refs, message params, or (fallback)
    # anywhere in the serialized event, so schema drift does not hide events.
    param([Parameter(Mandatory)][object]$EventRecord, [Parameter(Mandatory)][string]$CampaignId)
    $candidates = [System.Collections.Generic.List[string]]::new()
    $candidates.Add((Get-CampaignRefId -Ref (Get-CampaignProperty -Object $EventRecord -Name 'entity'))) | Out-Null
    foreach ($e in @(Get-CampaignProperty -Object $EventRecord -Name 'entities')) { $candidates.Add((Get-CampaignRefId -Ref $e)) | Out-Null }
    $msg = Get-CampaignProperty -Object $EventRecord -Name 'eventMessage'
    if ($null -ne $msg) {
        foreach ($e in @(Get-CampaignProperty -Object $msg -Name 'entities')) { $candidates.Add((Get-CampaignRefId -Ref $e)) | Out-Null }
        $candidates.Add((Get-CampaignRefId -Ref (Get-CampaignProperty -Object $msg -Name 'entity'))) | Out-Null
        $params = Get-CampaignProperty -Object $msg -Name 'messageParams'
        if ($null -ne $params -and -not (Test-CampaignScalar -Value $params)) {
            foreach ($pp in $params.PSObject.Properties) { $candidates.Add([string]$pp.Value) | Out-Null }
        }
    }
    foreach ($c in $candidates) { if ($c -and [string]::Equals($c, $CampaignId, [StringComparison]::OrdinalIgnoreCase)) { return $true } }
    try {
        $json = $EventRecord | ConvertTo-Json -Depth 12 -Compress
        return ($json.IndexOf($CampaignId, [StringComparison]::OrdinalIgnoreCase) -ge 0)
    }
    catch { return $false }
}

function ConvertTo-CampaignEventRow {
    param([Parameter(Mandatory)][object]$EventRecord)
    $msg = Get-CampaignProperty -Object $EventRecord -Name 'eventMessage'
    $text = Get-CampaignProperty -Object $msg -Name 'messageWithParams'
    if ([string]::IsNullOrWhiteSpace([string]$text)) { $text = Get-CampaignProperty -Object $msg -Name 'message' }
    $ts = Get-CampaignProperty -Object $EventRecord -Name 'timestamp'
    if ($null -eq $ts) { $ts = Get-CampaignProperty -Object $EventRecord -Name 'dateCreated' }
    return [pscustomobject]@{
        Timestamp     = ConvertTo-CampaignLocalTime -Value $ts
        Level         = [string](Get-CampaignProperty -Object $EventRecord -Name 'level')
        Category      = [string](Get-CampaignProperty -Object $EventRecord -Name 'category')
        Code          = [string](Get-CampaignProperty -Object $msg -Name 'code')
        Message       = [string]$text
        CorrelationId = [string](Get-CampaignProperty -Object $EventRecord -Name 'correlationId')
        EventId       = [string](Get-CampaignProperty -Object $EventRecord -Name 'id')
        SortKey       = [string]$ts
        Raw           = $EventRecord
    }
}

function Get-CampaignEventRows {
    # The events endpoint is org-wide: the newest pages are read and filtered to the campaign.
    param(
        [Parameter(Mandatory)][scriptblock]$Request,
        [Parameter(Mandatory)][string]$CampaignId,
        [int]$MaxPages = $script:CampaignMaxEventPages,
        [scriptblock]$OnProgress
    )
    $q = @{ sortBy = 'timestamp'; sortOrder = 'descending' }
    $page = Get-CampaignPagedEntities -Request $Request -Path '/api/v2/outbound/events' -QueryParams $q -MaxPages $MaxPages -OnProgress $OnProgress
    $rows = foreach ($ev in $page.Entities) {
        if (Test-CampaignEventMatch -EventRecord $ev -CampaignId $CampaignId) { ConvertTo-CampaignEventRow -EventRecord $ev }
    }
    $sorted = @($rows | Sort-Object -Property SortKey -Descending)
    return [pscustomobject]@{ Rows = $sorted; TotalEvents = $page.Entities.Count; PagesRead = $page.PagesRead; Truncated = $page.Truncated }
}

# -----------------------------------------------------------------------------
# Bridge to the conversation query
# -----------------------------------------------------------------------------

function Get-CampaignAnalysisInterval {
    # Default query window for a campaign's conversations: from the campaign's creation date when
    # it was created within the last MaxDays days, otherwise the last MaxDays days; through today.
    # Calendar dates are taken in TimeZone (the caller passes the business zone; default is the machine's).
    param([Parameter(Mandatory)][object]$Campaign, [DateTime]$Today = [DateTime]::Today, [int]$MaxDays = 30, [TimeZoneInfo]$TimeZone = [TimeZoneInfo]::Local)
    $floor = $Today.AddDays(-($MaxDays - 1))
    $start = $floor
    $reason = "last $MaxDays days"
    $createdText = [string](Get-CampaignProperty -Object (Get-CampaignProperty -Object $Campaign -Name 'Raw') -Name 'dateCreated')
    if ([string]::IsNullOrWhiteSpace($createdText)) { $createdText = [string](Get-CampaignProperty -Object $Campaign -Name 'Created') }
    $created = [DateTime]::MinValue
    if (-not [string]::IsNullOrWhiteSpace($createdText) -and [DateTime]::TryParse($createdText, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::RoundtripKind, [ref]$created)) {
        $createdDate = if ($created.Kind -eq [DateTimeKind]::Unspecified) { $created.Date } else { [TimeZoneInfo]::ConvertTimeFromUtc($created.ToUniversalTime(), $TimeZone).Date }
        if ($createdDate -gt $floor -and $createdDate -le $Today) { $start = $createdDate; $reason = 'since campaign creation' }
    }
    return [pscustomobject]@{
        Start     = $start
        End       = $Today
        Reason    = $reason
        Dimension = 'outboundCampaignId'
        Value     = [string](Get-CampaignProperty -Object $Campaign -Name 'CampaignId')
    }
}
