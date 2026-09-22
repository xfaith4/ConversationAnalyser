# Pester 5 tests for src/campaign/CampaignAnalysis.ps1 (pure logic only; no network, no WPF).
# Every API-facing function takes a -Request scriptblock; these tests inject a fake that serves
# canned pages and records the calls it receives.
# Run: Invoke-Pester -Path .\tests\CampaignAnalysis.Tests.ps1 -Output Detailed

BeforeAll {
    Set-StrictMode -Version Latest   # the module must stay clean under strict mode even though the app does not enable it
    . (Join-Path $PSScriptRoot '..\src\campaign\CampaignAnalysis.ps1')

    $script:campA = 'aaaaaaaa-0000-0000-0000-000000000001'
    $script:campB = 'bbbbbbbb-0000-0000-0000-000000000002'
    $script:campSms = 'cccccccc-0000-0000-0000-000000000003'

    function New-FakeRequest {
        # Routes: path -> canned object, or scriptblock receiving the query hashtable.
        # Calls: list that records every request (Method, Path, Query).
        param([hashtable]$Routes, [System.Collections.Generic.List[object]]$Calls)
        return {
            param([string]$Method, [string]$Path, [hashtable]$Query)
            $Calls.Add([pscustomobject]@{ Method = $Method; Path = $Path; Query = $Query }) | Out-Null
            if ($Routes.ContainsKey($Path)) {
                $handler = $Routes[$Path]
                if ($handler -is [scriptblock]) { return & $handler $Query }
                return $handler
            }
            throw "HTTP 404 for $Path"
        }.GetNewClosure()
    }

    function New-PagedRoute {
        # Serves $Pages[pageNumber-1] with pageCount set when -WithPageCount.
        param([object[][]]$Pages, [switch]$WithPageCount)
        $pageList = $Pages
        $withCount = [bool]$WithPageCount
        return {
            param([hashtable]$q)
            $n = [int]$q['pageNumber']
            $entities = if ($n -le $pageList.Count) { @($pageList[$n - 1]) } else { @() }
            $r = [ordered]@{ entities = $entities; pageSize = [int]$q['pageSize']; pageNumber = $n }
            if ($withCount) { $r['pageCount'] = $pageList.Count }
            [pscustomobject]$r
        }.GetNewClosure()
    }

    function New-CommonCampaign {
        param([string]$Id, [string]$Name, [string]$Media = 'voice', [string]$Status = 'on', [string]$Created = '2026-09-10T08:00:00.000Z')
        [pscustomobject]@{
            id = $Id; name = $Name; mediaType = $Media; campaignStatus = $Status
            division = [pscustomobject]@{ id = 'div-1'; name = 'Home' }
            dateCreated = $Created; dateModified = '2026-09-20T12:34:56.000Z'; version = 7
        }
    }
}

Describe 'Get-CampaignPagedEntities' {
    It 'follows pageCount and returns every entity in order' {
        $calls = [System.Collections.Generic.List[object]]::new()
        $pages = @(, @(@{ id = 'a' }, @{ id = 'b' })), @(, @(@{ id = 'c' }))
        $req = New-FakeRequest -Calls $calls -Routes @{ '/x' = (New-PagedRoute -Pages $pages -WithPageCount) }
        $r = Get-CampaignPagedEntities -Request $req -Path '/x' -PageSize 2
        @($r.Entities).id | Should -Be @('a', 'b', 'c')
        $r.PagesRead | Should -Be 2
        $r.Truncated | Should -BeFalse
        $calls.Count | Should -Be 2
        $calls[0].Query['pageSize'] | Should -Be '2'
        $calls[1].Query['pageNumber'] | Should -Be '2'
    }

    It 'stops on a short page when pageCount is absent' {
        $calls = [System.Collections.Generic.List[object]]::new()
        $pages = @(, @(@{ id = 'a' }, @{ id = 'b' })), @(, @(@{ id = 'c' }))
        $req = New-FakeRequest -Calls $calls -Routes @{ '/x' = (New-PagedRoute -Pages $pages) }
        $r = Get-CampaignPagedEntities -Request $req -Path '/x' -PageSize 2
        $r.Entities.Count | Should -Be 3
        $calls.Count | Should -Be 2
    }

    It 'honours MaxPages and reports truncation' {
        $calls = [System.Collections.Generic.List[object]]::new()
        $pages = @(, @(@{ id = 'a' })), @(, @(@{ id = 'b' })), @(, @(@{ id = 'c' }))
        $req = New-FakeRequest -Calls $calls -Routes @{ '/x' = (New-PagedRoute -Pages $pages -WithPageCount) }
        $r = Get-CampaignPagedEntities -Request $req -Path '/x' -PageSize 1 -MaxPages 2
        $r.Entities.Count | Should -Be 2
        $r.Truncated | Should -BeTrue
    }

    It 'passes extra query parameters through on every page' {
        $calls = [System.Collections.Generic.List[object]]::new()
        $req = New-FakeRequest -Calls $calls -Routes @{ '/x' = (New-PagedRoute -Pages @(, @(@{ id = 'a' })) -WithPageCount) }
        Get-CampaignPagedEntities -Request $req -Path '/x' -QueryParams @{ sortBy = 'timestamp' } | Out-Null
        $calls[0].Query['sortBy'] | Should -Be 'timestamp'
    }
}

Describe 'ConvertTo-CampaignRow / Get-OutboundCampaignList / Select-CampaignRows' {
    It 'flattens a CommonCampaign and defaults media type to voice' {
        $c = New-CommonCampaign -Id $script:campA -Name 'Renewals'
        $c.PSObject.Properties.Remove('mediaType')
        $row = ConvertTo-CampaignRow -Campaign $c
        $row.Name | Should -Be 'Renewals'
        $row.MediaType | Should -Be 'voice'
        $row.Status | Should -Be 'on'
        $row.Division | Should -Be 'Home'
        $row.CampaignId | Should -Be $script:campA
        $row.Modified | Should -Match '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
        $row.Raw | Should -Be $c
    }

    It 'lists campaigns sorted by name, case-insensitively' {
        $calls = [System.Collections.Generic.List[object]]::new()
        $entities = @(
            (New-CommonCampaign -Id $script:campB -Name 'zeta'),
            (New-CommonCampaign -Id $script:campSms -Name 'Alpha SMS' -Media 'sms'),
            (New-CommonCampaign -Id $script:campA -Name 'beta')
        )
        $req = New-FakeRequest -Calls $calls -Routes @{ '/api/v2/outbound/campaigns/all' = (New-PagedRoute -Pages @(, $entities) -WithPageCount) }
        $r = Get-OutboundCampaignList -Request $req
        @($r.Rows).Name | Should -Be @('Alpha SMS', 'beta', 'zeta')
        $calls[0].Path | Should -Be '/api/v2/outbound/campaigns/all'
    }

    It 'filters rows by any field, case-insensitively, requiring every term' {
        $rows = @(
            (ConvertTo-CampaignRow -Campaign (New-CommonCampaign -Id $script:campA -Name 'Renewals Q3' -Status 'on')),
            (ConvertTo-CampaignRow -Campaign (New-CommonCampaign -Id $script:campB -Name 'Winback' -Status 'off')),
            (ConvertTo-CampaignRow -Campaign (New-CommonCampaign -Id $script:campSms -Name 'Reminder' -Media 'sms' -Status 'on'))
        )
        @(Select-CampaignRows -Rows $rows -Text '').Count | Should -Be 3
        @(Select-CampaignRows -Rows $rows -Text $null).Count | Should -Be 3
        @(Select-CampaignRows -Rows $rows -Text 'renew').Name | Should -Be @('Renewals Q3')
        @(Select-CampaignRows -Rows $rows -Text 'SMS').Name | Should -Be @('Reminder')
        @(Select-CampaignRows -Rows $rows -Text 'on re').Name | Should -Be @('Renewals Q3', 'Reminder')
        @(Select-CampaignRows -Rows $rows -Text 'bbbbbbbb').Name | Should -Be @('Winback')
        @(Select-CampaignRows -Rows $rows -Text 'nomatch').Count | Should -Be 0
        @(Select-CampaignRows -Rows $null -Text 'x').Count | Should -Be 0
    }
}

Describe 'Get-CampaignApiPaths' {
    It 'routes voice campaigns to /outbound/campaigns with every status endpoint' {
        $p = Get-CampaignApiPaths -CampaignId $script:campA -MediaType 'voice'
        $p.Kind | Should -Be 'voice'
        $p.Detail | Should -Be "/api/v2/outbound/campaigns/$($script:campA)"
        $p.Progress | Should -Be "/api/v2/outbound/campaigns/$($script:campA)/progress"
        $p.Diagnostics | Should -Be "/api/v2/outbound/campaigns/$($script:campA)/diagnostics"
        $p.Stats | Should -Be "/api/v2/outbound/campaigns/$($script:campA)/stats"
        $p.Summary | Should -Be "/api/v2/outbound/diagnostics/campaigns/$($script:campA)/summary"
    }

    It 'routes sms and email campaigns to /outbound/messagingcampaigns without diagnostics or stats' {
        foreach ($m in @('sms', 'email', 'SMS')) {
            $p = Get-CampaignApiPaths -CampaignId $script:campSms -MediaType $m
            $p.Kind | Should -Be 'messaging'
            $p.Detail | Should -Be "/api/v2/outbound/messagingcampaigns/$($script:campSms)"
            $p.Progress | Should -Be "/api/v2/outbound/messagingcampaigns/$($script:campSms)/progress"
            $p.Diagnostics | Should -BeNullOrEmpty
            $p.Stats | Should -BeNullOrEmpty
        }
    }

    It 'treats a blank media type as voice and escapes the id' {
        (Get-CampaignApiPaths -CampaignId 'a b' -MediaType '').Detail | Should -Be '/api/v2/outbound/campaigns/a%20b'
    }
}

Describe 'Get-CampaignStatusSnapshot' {
    It 'collects each source independently and records failures without aborting' {
        $calls = [System.Collections.Generic.List[object]]::new()
        $base = "/api/v2/outbound/campaigns/$($script:campA)"
        $req = New-FakeRequest -Calls $calls -Routes @{
            $base                                                        = [pscustomobject]@{ id = $script:campA; name = 'Renewals'; dialingMode = 'preview' }
            "$base/progress"                                             = [pscustomobject]@{ numberOfContactsCalled = 40; totalNumberOfContacts = 100; percentage = 40 }
            "$base/diagnostics"                                          = { param($q) throw 'HTTP 403 forbidden' }
            "$base/stats"                                                = [pscustomobject]@{ contactRate = 0.25; idleAgents = 3 }
            "/api/v2/outbound/diagnostics/campaigns/$($script:campA)/summary" = [pscustomobject]@{ contactsRemaining = 60 }
        }
        $progressNames = [System.Collections.Generic.List[string]]::new()
        $s = Get-CampaignStatusSnapshot -Request $req -CampaignId $script:campA -MediaType 'voice' -OnProgress { param($n) $progressNames.Add($n) }
        $s.Detail.dialingMode | Should -Be 'preview'
        $s.Progress.percentage | Should -Be 40
        $s.Diagnostics | Should -BeNullOrEmpty
        $s.Stats.idleAgents | Should -Be 3
        $s.Summary.contactsRemaining | Should -Be 60
        @($s.Errors).Count | Should -Be 1
        $s.Errors[0] | Should -Match '^Diagnostics \(.*diagnostics\): HTTP 403'
        @($s.Sources) | Should -Be @('Detail', 'Progress', 'Stats', 'Summary')
        @($progressNames) | Should -Be @('Detail', 'Progress', 'Diagnostics', 'Stats', 'Summary')
        $calls.Count | Should -Be 5
    }

    It 'skips diagnostics and stats for messaging campaigns' {
        $calls = [System.Collections.Generic.List[object]]::new()
        $base = "/api/v2/outbound/messagingcampaigns/$($script:campSms)"
        $req = New-FakeRequest -Calls $calls -Routes @{
            $base            = [pscustomobject]@{ id = $script:campSms; messagesPerMinute = 10 }
            "$base/progress" = [pscustomobject]@{ numberOfContactsMessaged = 5; totalNumberOfContacts = 50 }
        }
        $s = Get-CampaignStatusSnapshot -Request $req -CampaignId $script:campSms -MediaType 'sms'
        $s.MediaKind | Should -Be 'messaging'
        $s.Detail.messagesPerMinute | Should -Be 10
        @($calls).Path | Should -Not -Contain "$base/diagnostics"
        @($calls).Path | Should -Not -Contain "$base/stats"
        @($s.Errors).Count | Should -Be 1   # summary route not served by the fake
    }
}

Describe 'ConvertTo-CampaignConfigTiles' {
    It 'renders voice configuration with names from entity refs and drops blank values' {
        $detail = [pscustomobject]@{
            campaignStatus = 'on'; dialingMode = 'predictive'; abandonRate = 3; outboundLineCount = 20
            queue = [pscustomobject]@{ id = 'q1'; name = 'Sales' }
            contactList = [pscustomobject]@{ id = 'cl1'; name = 'Sept leads' }
            division = [pscustomobject]@{ id = 'd1' }
            script = $null; callerName = ''; alwaysRunning = $false
            dncLists = @([pscustomobject]@{ id = 'dnc1'; name = 'Federal' }, [pscustomobject]@{ id = 'dnc2'; name = 'Internal' })
            phoneColumns = @([pscustomobject]@{ columnName = 'Cell'; type = 'cell' })
            contactSorts = @([pscustomobject]@{ fieldName = 'Priority'; direction = 'DESC' })
            errors = @([pscustomobject]@{ error = 'contact list is empty' })
            dateCreated = '2026-09-01T00:00:00Z'
        }
        $tiles = @(ConvertTo-CampaignConfigTiles -Detail $detail -MediaKind 'voice')
        $byLabel = @{}; foreach ($t in $tiles) { $byLabel[$t.Label] = $t.Value }
        $byLabel['Dialing mode'] | Should -Be 'predictive'
        $byLabel['Queue'] | Should -Be 'Sales'
        $byLabel['Contact list'] | Should -Be 'Sept leads'
        $byLabel['Division'] | Should -Be 'd1'
        $byLabel['DNC lists'] | Should -Be 'Federal, Internal'
        $byLabel['Phone columns'] | Should -Be 'Cell (cell)'
        $byLabel['Contact sorts'] | Should -Be 'Priority DESC'
        $byLabel['Always running'] | Should -Be 'No'
        $byLabel['Configuration errors'] | Should -Be 'contact list is empty'
        $byLabel['Abandon rate target'] | Should -Be '3'
        $byLabel.ContainsKey('Script') | Should -BeFalse
        $byLabel.ContainsKey('Caller name') | Should -BeFalse
        ($tiles | ForEach-Object { $_.Group } | Select-Object -Unique) | Should -Be @('Configuration')
    }

    It 'renders messaging configuration and returns nothing for a null detail' {
        $detail = [pscustomobject]@{
            campaignStatus = 'off'; messagesPerMinute = 12
            smsConfig = [pscustomobject]@{ senderSmsPhoneNumber = '+15555550100'; messageColumn = 'Body'; phoneColumn = 'Mobile' }
        }
        $tiles = @(ConvertTo-CampaignConfigTiles -Detail $detail -MediaKind 'messaging')
        ($tiles | Where-Object Label -eq 'Messages per minute').Value | Should -Be '12'
        ($tiles | Where-Object Label -eq 'SMS sender').Value | Should -Be '+15555550100'
        ($tiles | Where-Object Label -eq 'Dialing mode') | Should -BeNullOrEmpty
        @(ConvertTo-CampaignConfigTiles -Detail $null).Count | Should -Be 0
    }
}

Describe 'ConvertTo-CampaignStatusTiles' {
    It 'renders progress, live stats, diagnostics, and generic summary fields with groups' {
        $snapshot = [pscustomobject]@{
            Progress    = [pscustomobject]@{ numberOfContactsCalled = 40; totalNumberOfContacts = 100; percentage = 40; numberOfContactsSkipped = [pscustomobject]@{ dnc = 2; timezone = 1 }; campaign = [pscustomobject]@{ id = 'x' } }
            Stats       = [pscustomobject]@{ contactRate = 0.25; idleAgents = 3; somethingNew = 'n/a' }
            Diagnostics = [pscustomobject]@{ outstandingInteractionsCount = 4; scheduledInteractionsCount = 0; campaignErrors = @(); callableContacts = [pscustomobject]@{ contactList = [pscustomobject]@{ id = 'cl'; name = 'Sept' }; contactableContacts = 60 } }
            Summary     = [pscustomobject]@{ contactsRemaining = 60; lastRunAt = '2026-09-21T10:00:00Z'; selfUri = '/ignored'; nested = [pscustomobject]@{ deep = 1; refs = @([pscustomobject]@{ id = 'r'; name = 'Ref A' }) } }
        }
        $tiles = @(ConvertTo-CampaignStatusTiles -Snapshot $snapshot)
        $find = { param($label) ($tiles | Where-Object Label -eq $label | Select-Object -First 1) }

        (& $find 'Contacts called').Value | Should -Be '40'
        (& $find 'Contacts called').Caption | Should -Be 'of 100'
        (& $find 'Contacts called').Group | Should -Be 'Progress'
        (& $find 'Complete').Value | Should -Be '40%'
        (& $find 'Contacts skipped').Value | Should -Be 'dnc: 2, timezone: 1'
        ($tiles | Where-Object Label -eq 'campaign') | Should -BeNullOrEmpty

        (& $find 'Contact rate').Value | Should -Be '0.25'
        (& $find 'Contact rate').Group | Should -Be 'Live stats'
        (& $find 'somethingNew').Value | Should -Be 'n/a'

        (& $find 'Outstanding interactions').Value | Should -Be '4'
        (& $find 'Campaign errors').Value | Should -Be 'none'
        (& $find 'Callable: contactList').Value | Should -Be 'Sept'
        (& $find 'Callable: contactableContacts').Value | Should -Be '60'

        (& $find 'contactsRemaining').Group | Should -Be 'Summary'
        (& $find 'lastRunAt').Value | Should -Match '^\d{4}-\d{2}-\d{2} '
        (& $find 'nested.deep').Value | Should -Be '1'
        (& $find 'nested.refs').Value | Should -Be 'Ref A'
        ($tiles | Where-Object Label -eq 'selfUri') | Should -BeNullOrEmpty
    }

    It 'lists diagnostic errors and handles a snapshot with nothing loaded' {
        $snapshot = [pscustomobject]@{
            Progress = $null; Stats = $null; Summary = $null
            Diagnostics = [pscustomobject]@{ campaignErrors = @([pscustomobject]@{ error = 'no agents' }, [pscustomobject]@{ message = 'lines exhausted' }) }
        }
        $t = @(ConvertTo-CampaignStatusTiles -Snapshot $snapshot) | Where-Object Label -eq 'Campaign errors'
        $t.Value | Should -Be 'no agents; lines exhausted'
        $t.Caption | Should -Be '2 error(s)'
        $empty = [pscustomobject]@{ Progress = $null; Stats = $null; Diagnostics = $null; Summary = $null }
        @(ConvertTo-CampaignStatusTiles -Snapshot $empty).Count | Should -Be 0
    }
}

Describe 'Campaign rules' {
    BeforeAll {
        function New-Rule {
            param([string]$Id, [string]$Name, [string[]]$TriggerIds = @(), [string[]]$TargetIds = @(), [bool]$UseTriggering = $false, [bool]$MatchAny = $false, [string]$SmsTrigger = '')
            $entities = [pscustomobject]@{ campaigns = @($TriggerIds | ForEach-Object { [pscustomobject]@{ id = $_; name = "camp $_" } }); sequences = @() }
            if ($SmsTrigger) { $entities | Add-Member -NotePropertyName smsCampaigns -NotePropertyValue @([pscustomobject]@{ id = $SmsTrigger }) }
            [pscustomobject]@{
                id = $Id; name = $Name; enabled = $true; matchAnyConditions = $MatchAny; dateModified = '2026-09-19T00:00:00Z'
                campaignRuleEntities = $entities
                campaignRuleConditions = @(
                    [pscustomobject]@{ conditionType = 'campaignProgress'; parameters = [pscustomobject]@{ operator = 'greaterThan'; value = '50' } },
                    [pscustomobject]@{ conditionType = 'campaignAgents'; parameters = [pscustomobject]@{ operator = 'lessThan'; value = '2'; dialingMode = 'predictive' } }
                )
                campaignRuleActions = @(
                    [pscustomobject]@{
                        actionType = 'turnOnCampaign'; parameters = [pscustomobject]@{ priority = 5; dialingMode = $null }
                        campaignRuleActionEntities = [pscustomobject]@{ campaigns = @($TargetIds | ForEach-Object { [pscustomobject]@{ id = $_; name = "camp $_" } }); useTriggeringEntity = $UseTriggering }
                    }
                )
            }
        }
    }

    It 'classifies the campaign role as trigger, target, both, or unrelated' {
        (Get-CampaignRuleRole -Rule (New-Rule -Id 'r1' -Name 'a' -TriggerIds @($script:campA)) -CampaignId $script:campA) | Should -Be 'trigger'
        (Get-CampaignRuleRole -Rule (New-Rule -Id 'r2' -Name 'b' -TriggerIds @($script:campB) -TargetIds @($script:campA)) -CampaignId $script:campA) | Should -Be 'target'
        (Get-CampaignRuleRole -Rule (New-Rule -Id 'r3' -Name 'c' -TriggerIds @($script:campA) -TargetIds @($script:campA)) -CampaignId $script:campA) | Should -Be 'trigger+target'
        (Get-CampaignRuleRole -Rule (New-Rule -Id 'r4' -Name 'd' -TriggerIds @($script:campA) -UseTriggering $true) -CampaignId $script:campA) | Should -Be 'trigger+target'
        (Get-CampaignRuleRole -Rule (New-Rule -Id 'r5' -Name 'e' -TriggerIds @($script:campB)) -CampaignId $script:campA) | Should -Be ''
        (Get-CampaignRuleRole -Rule (New-Rule -Id 'r6' -Name 'f' -SmsTrigger $script:campSms) -CampaignId $script:campSms) | Should -Be 'trigger'
    }

    It 'formats conditions with the AND/OR joiner and actions with parameters and targets' {
        $row = ConvertTo-CampaignRuleRow -Rule (New-Rule -Id 'r1' -Name 'Boost' -TriggerIds @($script:campA) -TargetIds @($script:campB)) -CampaignId $script:campA
        $row.Conditions | Should -Be 'campaignProgress greaterThan 50 AND campaignAgents lessThan 2 [predictive]'
        $row.Actions | Should -Be "turnOnCampaign (priority=5) -> camp $($script:campB)"
        $row.Enabled | Should -BeTrue
        $row.Role | Should -Be 'trigger'
        $row.RuleId | Should -Be 'r1'
        (ConvertTo-CampaignRuleRow -Rule (New-Rule -Id 'r2' -Name 'x' -TriggerIds @($script:campA) -MatchAny $true) -CampaignId $script:campA).Conditions | Should -Match ' OR '
    }

    It 'pages every rule and keeps only the ones related to the campaign' {
        $calls = [System.Collections.Generic.List[object]]::new()
        $rules = @(
            (New-Rule -Id 'r1' -Name 'watch A' -TriggerIds @($script:campA)),
            (New-Rule -Id 'r2' -Name 'unrelated' -TriggerIds @($script:campB)),
            (New-Rule -Id 'r3' -Name 'acts on A' -TriggerIds @($script:campB) -TargetIds @($script:campA))
        )
        $req = New-FakeRequest -Calls $calls -Routes @{ '/api/v2/outbound/campaignrules' = (New-PagedRoute -Pages @(, $rules) -WithPageCount) }
        $r = Get-CampaignRuleRows -Request $req -CampaignId $script:campA
        @($r.Rows).Name | Should -Be @('watch A', 'acts on A')
        @($r.Rows).Role | Should -Be @('trigger', 'target')
        $r.TotalRules | Should -Be 3
    }
}

Describe 'Outbound events' {
    BeforeAll {
        function New-Event {
            param([string]$Id, [string]$Ts, [string]$EntityId = '', [hashtable]$Params = @{}, [string]$Level = 'INFO', [string]$Text = 'Campaign {campaignName} turned on')
            $ev = [pscustomobject]@{
                id = $Id; timestamp = $Ts; level = $Level; category = 'CAMPAIGN'; correlationId = "corr-$Id"
                eventMessage = [pscustomobject]@{ code = 'CAMPAIGN_STATE'; message = $Text; messageWithParams = $Text.Replace('{campaignName}', 'Renewals'); messageParams = [pscustomobject]$Params }
            }
            if ($EntityId) { $ev | Add-Member -NotePropertyName entities -NotePropertyValue @([pscustomobject]@{ id = $EntityId; name = 'Renewals' }) }
            $ev
        }
    }

    It 'matches events by entity ref, by message param, and by serialized fallback' {
        (Test-CampaignEventMatch -EventRecord (New-Event -Id 'e1' -Ts '2026-09-21T10:00:00Z' -EntityId $script:campA) -CampaignId $script:campA) | Should -BeTrue
        (Test-CampaignEventMatch -EventRecord (New-Event -Id 'e2' -Ts '2026-09-21T10:00:00Z' -Params @{ campaignId = $script:campA.ToUpperInvariant() }) -CampaignId $script:campA) | Should -BeTrue
        $deep = [pscustomobject]@{ id = 'e3'; timestamp = '2026-09-21T10:00:00Z'; details = [pscustomobject]@{ nested = [pscustomobject]@{ campaign = $script:campA } } }
        (Test-CampaignEventMatch -EventRecord $deep -CampaignId $script:campA) | Should -BeTrue
        (Test-CampaignEventMatch -EventRecord (New-Event -Id 'e4' -Ts '2026-09-21T10:00:00Z' -EntityId $script:campB) -CampaignId $script:campA) | Should -BeFalse
    }

    It 'returns campaign events newest first with the message rendered' {
        $calls = [System.Collections.Generic.List[object]]::new()
        $events = @(
            (New-Event -Id 'old' -Ts '2026-09-20T09:00:00Z' -EntityId $script:campA),
            (New-Event -Id 'other' -Ts '2026-09-21T09:30:00Z' -EntityId $script:campB),
            (New-Event -Id 'new' -Ts '2026-09-21T10:00:00Z' -EntityId $script:campA -Level 'ERROR')
        )
        $req = New-FakeRequest -Calls $calls -Routes @{ '/api/v2/outbound/events' = (New-PagedRoute -Pages @(, $events) -WithPageCount) }
        $r = Get-CampaignEventRows -Request $req -CampaignId $script:campA
        @($r.Rows).EventId | Should -Be @('new', 'old')
        $r.Rows[0].Level | Should -Be 'ERROR'
        $r.Rows[0].Message | Should -Be 'Campaign Renewals turned on'
        $r.Rows[0].Code | Should -Be 'CAMPAIGN_STATE'
        $r.TotalEvents | Should -Be 3
        $calls[0].Query['sortBy'] | Should -Be 'timestamp'
        $calls[0].Query['sortOrder'] | Should -Be 'descending'
    }
}

Describe 'Get-CampaignAnalysisInterval' {
    BeforeAll { $script:today = [DateTime]::new(2026, 9, 22) }

    It 'starts at the campaign creation date when it was created within the window' {
        $row = ConvertTo-CampaignRow -Campaign (New-CommonCampaign -Id $script:campA -Name 'x' -Created '2026-09-10T15:00:00Z')
        $i = Get-CampaignAnalysisInterval -Campaign $row -Today $script:today
        $i.Start | Should -Be ([DateTime]::new(2026, 9, 10))
        $i.End | Should -Be $script:today
        $i.Reason | Should -Be 'since campaign creation'
        $i.Dimension | Should -Be 'outboundCampaignId'
        $i.Value | Should -Be $script:campA
    }

    It 'falls back to the last 30 days for older campaigns and when the creation date is missing' {
        $old = ConvertTo-CampaignRow -Campaign (New-CommonCampaign -Id $script:campA -Name 'x' -Created '2025-01-01T00:00:00Z')
        (Get-CampaignAnalysisInterval -Campaign $old -Today $script:today).Start | Should -Be ([DateTime]::new(2026, 8, 24))
        (Get-CampaignAnalysisInterval -Campaign $old -Today $script:today).Reason | Should -Be 'last 30 days'
        $c = New-CommonCampaign -Id $script:campA -Name 'x'
        $c.PSObject.Properties.Remove('dateCreated')
        $none = ConvertTo-CampaignRow -Campaign $c
        (Get-CampaignAnalysisInterval -Campaign $none -Today $script:today).Start | Should -Be ([DateTime]::new(2026, 8, 24))
        (Get-CampaignAnalysisInterval -Campaign $none -Today $script:today -MaxDays 7).Start | Should -Be ([DateTime]::new(2026, 9, 16))
    }
}
