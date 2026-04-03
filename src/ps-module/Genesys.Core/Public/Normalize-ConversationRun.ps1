function Normalize-ConversationRun {
    [CmdletBinding(SupportsShouldProcess = $true)]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [string]$SourcePath,

        [string]$OutputRoot = 'out',

        [switch]$ContinueOnError
    )

    $dataPath = Resolve-ConversationDataPath -SourcePath $SourcePath
    $resolvedOutputRoot = Resolve-GenesysOutputRootPath -OutputRoot $OutputRoot
    $runContext = $null

    if ($PSCmdlet.ShouldProcess($dataPath, 'Normalize conversation detail run') -eq $false) {
        return
    }

    $runContext = New-RunContext -DatasetKey 'analytics-conversation-details-normalized' -OutputRoot $resolvedOutputRoot
    Write-RunEvent -RunContext $runContext -EventType 'run.started' -Payload @{ sourcePath = $dataPath } | Out-Null

    $tableCounts = [ordered]@{
        sourceRecords = 0
        conversationFacts = 0
        participantFacts = 0
        sessionFacts = 0
        segmentFacts = 0
        metricFacts = 0
        attributeFacts = 0
        duplicateConversationIds = 0
        normalizationErrors = 0
    }

    $seenConversationIds = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    $warnings = [System.Collections.Generic.List[string]]::new()
    $dataFiles = [ordered]@{
        conversations = (Join-Path -Path $runContext.dataFolder -ChildPath 'conversations.jsonl')
        participants = (Join-Path -Path $runContext.dataFolder -ChildPath 'participants.jsonl')
        sessions = (Join-Path -Path $runContext.dataFolder -ChildPath 'sessions.jsonl')
        segments = (Join-Path -Path $runContext.dataFolder -ChildPath 'segments.jsonl')
        metrics = (Join-Path -Path $runContext.dataFolder -ChildPath 'metrics.jsonl')
        attributes = (Join-Path -Path $runContext.dataFolder -ChildPath 'attributes.jsonl')
    }

    foreach ($lineRecord in @(Read-Jsonl -Path $dataPath)) {
        foreach ($conversation in @(ConvertTo-ConversationRecordList -InputObject $lineRecord)) {
            $tableCounts.sourceRecords++

            try {
                $conversationId = Get-ConversationRecordId -Conversation $conversation
                if ([string]::IsNullOrWhiteSpace($conversationId)) {
                    throw 'Conversation record is missing conversationId.'
                }

                if (-not $seenConversationIds.Add($conversationId)) {
                    $tableCounts.duplicateConversationIds++
                    continue
                }

                $conversationFact = ConvertTo-ConversationFact -Conversation $conversation
                Write-Jsonl -Path $dataFiles.conversations -InputObject $conversationFact
                $tableCounts.conversationFacts++

                foreach ($fact in @(ConvertTo-ConversationParticipantFacts -Conversation $conversation)) {
                    Write-Jsonl -Path $dataFiles.participants -InputObject $fact
                    $tableCounts.participantFacts++
                }

                foreach ($fact in @(ConvertTo-ConversationSessionFacts -Conversation $conversation)) {
                    Write-Jsonl -Path $dataFiles.sessions -InputObject $fact
                    $tableCounts.sessionFacts++
                }

                foreach ($fact in @(ConvertTo-ConversationSegmentFacts -Conversation $conversation)) {
                    Write-Jsonl -Path $dataFiles.segments -InputObject $fact
                    $tableCounts.segmentFacts++
                }

                foreach ($fact in @(ConvertTo-ConversationMetricFacts -Conversation $conversation)) {
                    Write-Jsonl -Path $dataFiles.metrics -InputObject $fact
                    $tableCounts.metricFacts++
                }

                foreach ($fact in @(ConvertTo-ConversationAttributeFacts -Conversation $conversation)) {
                    Write-Jsonl -Path $dataFiles.attributes -InputObject $fact
                    $tableCounts.attributeFacts++
                }
            }
            catch {
                $tableCounts.normalizationErrors++
                $warnings.Add($_.Exception.Message) | Out-Null
                Write-RunEvent -RunContext $runContext -EventType 'normalization.record_failed' -Payload @{
                    sourcePath = $dataPath
                    message = $_.Exception.Message
                } | Out-Null

                if (-not $ContinueOnError) {
                    Write-Manifest -RunContext $runContext -Counts $tableCounts -Warnings @($warnings) | Out-Null
                    throw
                }
            }
        }
    }

    $summary = [ordered]@{
        datasetKey = $runContext.datasetKey
        runId = $runContext.runId
        sourcePath = $dataPath
        outputs = $dataFiles
        totals = $tableCounts
        generatedAtUtc = [DateTime]::UtcNow.ToString('o')
    }

    ($summary | ConvertTo-Json -Depth 100) | Set-Content -Path $runContext.summaryPath -Encoding utf8
    Write-RunEvent -RunContext $runContext -EventType 'run.completed' -Payload @{ itemCount = $tableCounts.conversationFacts } | Out-Null
    Write-Manifest -RunContext $runContext -Counts $tableCounts -Warnings @($warnings | Select-Object -Unique) | Out-Null

    return [pscustomobject]@{
        RunContext = $runContext
        Summary = [pscustomobject]$summary
    }
}
