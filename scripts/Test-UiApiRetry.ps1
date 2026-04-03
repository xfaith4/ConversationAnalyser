[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot '..\src\ui\UiApiRetry.ps1')

function Assert-True {
    param(
        [bool]$Condition,
        [string]$Message
    )

    if (-not $Condition) {
        throw $Message
    }
}

function Assert-Equal {
    param(
        $Actual,
        $Expected,
        [string]$Message
    )

    if ($Actual -ne $Expected) {
        throw "$Message Expected '$Expected' but got '$Actual'."
    }
}

function New-StubHttpException {
    param(
        [int]$StatusCode,
        [string]$Message = 'stub request failed',
        [AllowNull()]
        [object]$RetryAfter = $null
    )

    $exception = [System.Exception]::new($Message)
    $headers = @{}
    if ($PSBoundParameters.ContainsKey('RetryAfter') -and $null -ne $RetryAfter) {
        $headers['Retry-After'] = [string]$RetryAfter
    }

    $response = [pscustomobject]@{
        StatusCode = $StatusCode
        Headers = $headers
    }

    Add-Member -InputObject $exception -NotePropertyName Response -NotePropertyValue $response
    return $exception
}

$retrySettings = Resolve-UiApiRetrySettings -RetryProfile ([pscustomobject]@{
    maxRetries = 2
    baseDelaySeconds = 1
    maxDelaySeconds = 10
    jitterSeconds = 0
    retryOnStatusCodes = @(429, 500)
    retryOnMethods = @('GET', 'DELETE')
    allowRetryOnPost = $false
    randomSeed = 17
})

function Invoke-Scenario {
    param(
        [string]$Name,
        [scriptblock]$Body
    )

    & $Body
    Write-Host "[pass] $Name"
}

Invoke-Scenario -Name 'success' -Body {
    $result = Invoke-UiApiRequest -BaseUri 'https://api.example.test' -Method 'GET' -Path '/ok' -RetrySettings $retrySettings -RequestInvoker {
        param([hashtable]$InvokeParams)
        return [pscustomobject]@{
            ok = $true
            uri = $InvokeParams.Uri
        }
    }

    Assert-True ($result.ok) 'Expected success scenario to return the stub result.'
    Assert-Equal $result.uri 'https://api.example.test/ok' 'Expected the request URI to be assembled correctly.'
}

Invoke-Scenario -Name 'single retry then success' -Body {
    $state = [pscustomobject]@{ Attempts = 0 }
    $delays = [System.Collections.Generic.List[double]]::new()
    $logEntries = [System.Collections.Generic.List[object]]::new()

    $result = Invoke-UiApiRequest -BaseUri 'https://api.example.test' -Method 'GET' -Path '/retry-once' -RetrySettings $retrySettings -RequestInvoker {
        param([hashtable]$InvokeParams)
        $state.Attempts++
        if ($state.Attempts -eq 1) {
            throw (New-StubHttpException -StatusCode 429 -Message 'rate limited' -RetryAfter 7)
        }

        return [pscustomobject]@{ ok = $true }
    }.GetNewClosure() -SleepAction {
        param([double]$Seconds)
        $delays.Add($Seconds) | Out-Null
    }.GetNewClosure() -LogAction {
        param($Entry)
        $logEntries.Add($Entry) | Out-Null
    }.GetNewClosure()

    Assert-True ($result.ok) 'Expected retry-then-success scenario to succeed.'
    Assert-Equal $state.Attempts 2 'Expected exactly two attempts.'
    Assert-Equal $delays.Count 1 'Expected exactly one retry delay.'
    Assert-Equal $delays[0] 7 'Expected Retry-After to control the retry delay.'
    Assert-Equal $logEntries.Count 1 'Expected one retry log entry.'
    Assert-Equal $logEntries[0].StatusCode 429 'Expected retry log entry to capture the HTTP status.'
}

Invoke-Scenario -Name 'repeated 429 exhausts retries' -Body {
    $state = [pscustomobject]@{ Attempts = 0 }
    $delays = [System.Collections.Generic.List[double]]::new()

    try {
        Invoke-UiApiRequest -BaseUri 'https://api.example.test' -Method 'GET' -Path '/too-many-requests' -RetrySettings $retrySettings -RequestInvoker {
            param([hashtable]$InvokeParams)
            $state.Attempts++
            throw (New-StubHttpException -StatusCode 429 -Message 'rate limited again' -RetryAfter 3)
        }.GetNewClosure() -SleepAction {
            param([double]$Seconds)
            $delays.Add($Seconds) | Out-Null
        }.GetNewClosure() | Out-Null

        throw 'Expected repeated 429 scenario to fail.'
    }
    catch {
        $summary = Get-UiRequestFailureSummary -Exception $_.Exception
        Assert-Equal $state.Attempts 3 'Expected three attempts before retries were exhausted.'
        Assert-Equal $summary.FailureKind 'retry-exhausted' 'Expected repeated 429 failures to be reported as retry exhaustion.'
        Assert-Equal $summary.StatusCode 429 'Expected the final failure status to be preserved.'
        Assert-Equal $delays.Count 2 'Expected two retry delays for three attempts.'
        Assert-Equal $delays[0] 3 'Expected Retry-After to control the first retry delay.'
        Assert-Equal $delays[1] 3 'Expected Retry-After to control the second retry delay.'
    }
}

Invoke-Scenario -Name 'repeated 500 exhausts retries' -Body {
    $state = [pscustomobject]@{ Attempts = 0 }
    $delays = [System.Collections.Generic.List[double]]::new()

    try {
        Invoke-UiApiRequest -BaseUri 'https://api.example.test' -Method 'GET' -Path '/server-error' -RetrySettings $retrySettings -RequestInvoker {
            param([hashtable]$InvokeParams)
            $state.Attempts++
            throw (New-StubHttpException -StatusCode 500 -Message 'server error')
        }.GetNewClosure() -SleepAction {
            param([double]$Seconds)
            $delays.Add($Seconds) | Out-Null
        }.GetNewClosure() | Out-Null

        throw 'Expected repeated 500 scenario to fail.'
    }
    catch {
        $summary = Get-UiRequestFailureSummary -Exception $_.Exception
        Assert-Equal $state.Attempts 3 'Expected three attempts before retries were exhausted.'
        Assert-Equal $summary.FailureKind 'retry-exhausted' 'Expected repeated 500 failures to be reported as retry exhaustion.'
        Assert-Equal $summary.StatusCode 500 'Expected the final 500 status to be preserved.'
        Assert-Equal $delays.Count 2 'Expected two retry delays for three attempts.'
        Assert-Equal $delays[0] 1 'Expected exponential backoff to start at one second.'
        Assert-Equal $delays[1] 2 'Expected exponential backoff to double on the second retry.'
    }
}

Invoke-Scenario -Name 'cancel failure warning text' -Body {
    try {
        Invoke-UiApiRequest -BaseUri 'https://api.example.test' -Method 'DELETE' -Path '/jobs/job-123' -RetrySettings $retrySettings -RequestInvoker {
            param([hashtable]$InvokeParams)
            throw (New-StubHttpException -StatusCode 500 -Message 'delete failed')
        } -SleepAction { param([double]$Seconds) } | Out-Null

        throw 'Expected delete scenario to fail.'
    }
    catch {
        $summary = Get-UiRequestFailureSummary -Exception $_.Exception
        $warningMessage = "Warning: cancel/delete request failed for job job-123. $($summary.DisplayText)"
        Assert-True ($warningMessage -like 'Warning: cancel/delete request failed for job job-123.*') 'Expected the warning prefix to be stable.'
        Assert-True ($warningMessage -like '*transient request failed after 3 attempts*') 'Expected the warning text to preserve retry exhaustion context.'
    }
}

Write-Host 'All retry validation scenarios passed.'
