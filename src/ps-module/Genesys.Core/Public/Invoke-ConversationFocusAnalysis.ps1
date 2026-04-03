function Invoke-ConversationFocusAnalysis {
    [CmdletBinding(SupportsShouldProcess = $true)]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [string]$NormalizedPath,

        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [string]$ProfilePath,

        [string]$OutputRoot = 'out'
    )

    if (-not (Test-Path -Path $ProfilePath -PathType Leaf)) {
        throw "Profile '$($ProfilePath)' was not found."
    }

    $profile = Get-Content -Path $ProfilePath -Raw -Encoding utf8 | ConvertFrom-Json
    $entity = [string]$profile.source
    if ([string]::IsNullOrWhiteSpace($entity)) {
        throw 'Focus profile requires a source value.'
    }

    $entityPath = Resolve-NormalizedEntityPath -NormalizedPath $NormalizedPath -Entity $entity
    $resolvedOutputRoot = Resolve-GenesysOutputRootPath -OutputRoot $OutputRoot

    if ($PSCmdlet.ShouldProcess($entityPath, "Analyze focus profile '$($profile.name)'") -eq $false) {
        return
    }

    $runContext = New-RunContext -DatasetKey 'conversation-focus-analysis' -OutputRoot $resolvedOutputRoot
    Write-RunEvent -RunContext $runContext -EventType 'run.started' -Payload @{ normalizedPath = $NormalizedPath; profilePath = (Resolve-Path -Path $ProfilePath).Path } | Out-Null

    $records = @()
    foreach ($record in @(Read-Jsonl -Path $entityPath)) {
        if (Test-AnalysisFilters -Record $record -Filters @($profile.filters)) {
            $records += ,$record
        }
    }

    $groupBy = @($profile.groupBy)
    $trendByField = [string]$profile.trendBy
    $metrics = @($profile.metrics)
    if ($metrics.Count -eq 0) {
        throw 'Focus profile requires at least one metric.'
    }

    $groupBuckets = @{}
    foreach ($record in @($records)) {
        $groupIdentity = ConvertTo-GroupIdentity -Record $record -Fields $groupBy
        $groupKey = ConvertTo-GroupIdentityKey -Identity $groupIdentity
        if (-not $groupBuckets.ContainsKey($groupKey)) {
            $groupBuckets[$groupKey] = [ordered]@{
                Identity = $groupIdentity
                Records = [System.Collections.Generic.List[object]]::new()
            }
        }

        $groupBuckets[$groupKey].Records.Add($record) | Out-Null
    }

    $aggregatePath = Join-Path -Path $runContext.dataFolder -ChildPath 'aggregates.jsonl'
    $aggregateCount = 0
    foreach ($bucket in $groupBuckets.Values) {
        $row = [ordered]@{}
        foreach ($property in $bucket.Identity.PSObject.Properties) {
            $row[$property.Name] = $property.Value
        }

        foreach ($metric in $metrics) {
            $result = Measure-AnalysisMetric -Records @($bucket.Records) -Metric $metric
            $row[$result.Name] = $result.Value
        }

        Write-Jsonl -Path $aggregatePath -InputObject ([pscustomobject]$row)
        $aggregateCount++
    }

    $trendCount = 0
    $trendPath = Join-Path -Path $runContext.dataFolder -ChildPath 'trends.jsonl'
    if ([string]::IsNullOrWhiteSpace($trendByField) -eq $false) {
        $trendBuckets = @{}
        foreach ($record in @($records)) {
            $trendFields = @($groupBy + $trendByField)
            $trendIdentity = ConvertTo-GroupIdentity -Record $record -Fields $trendFields
            $trendKey = ConvertTo-GroupIdentityKey -Identity $trendIdentity
            if (-not $trendBuckets.ContainsKey($trendKey)) {
                $trendBuckets[$trendKey] = [ordered]@{
                    Identity = $trendIdentity
                    Records = [System.Collections.Generic.List[object]]::new()
                }
            }

            $trendBuckets[$trendKey].Records.Add($record) | Out-Null
        }

        foreach ($bucket in $trendBuckets.Values) {
            $row = [ordered]@{}
            foreach ($property in $bucket.Identity.PSObject.Properties) {
                $row[$property.Name] = $property.Value
            }

            foreach ($metric in $metrics) {
                $result = Measure-AnalysisMetric -Records @($bucket.Records) -Metric $metric
                $row[$result.Name] = $result.Value
            }

            Write-Jsonl -Path $trendPath -InputObject ([pscustomobject]$row)
            $trendCount++
        }
    }

    $summary = [ordered]@{
        datasetKey = $runContext.datasetKey
        runId = $runContext.runId
        profileName = [string]$profile.name
        profilePath = (Resolve-Path -Path $ProfilePath).Path
        sourceEntity = $entity
        sourceDataPath = $entityPath
        totals = [ordered]@{
            matchedRecords = $records.Count
            aggregateGroups = $aggregateCount
            trendGroups = $trendCount
        }
        generatedAtUtc = [DateTime]::UtcNow.ToString('o')
    }

    Copy-Item -Path (Resolve-Path -Path $ProfilePath).Path -Destination (Join-Path -Path $runContext.runFolder -ChildPath 'profile.json') -Force
    ($summary | ConvertTo-Json -Depth 100) | Set-Content -Path $runContext.summaryPath -Encoding utf8
    Write-RunEvent -RunContext $runContext -EventType 'run.completed' -Payload @{ itemCount = $aggregateCount } | Out-Null
    Write-Manifest -RunContext $runContext -Counts @{ matchedRecords = $records.Count; aggregateGroups = $aggregateCount; trendGroups = $trendCount } | Out-Null

    return [pscustomobject]@{
        RunContext = $runContext
        Summary = [pscustomobject]$summary
    }
}
