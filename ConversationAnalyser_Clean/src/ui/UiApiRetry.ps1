function Get-UiHttpStatusCode {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [System.Exception]$Exception
    )

    if ($null -ne $Exception.Response -and $null -ne $Exception.Response.StatusCode) {
        return [int]$Exception.Response.StatusCode
    }

    return $null
}

function Get-UiRetryAfterSeconds {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [System.Exception]$Exception,

        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    if ($null -ne $Exception.Response -and $null -ne $Exception.Response.Headers) {
        $headers = $Exception.Response.Headers
        $retryAfterHeader = $null

        if ($headers -is [System.Collections.IDictionary]) {
            $retryAfterHeader = $headers['Retry-After']
        }
        elseif ($headers.PSObject.Properties.Name -contains 'Retry-After') {
            $retryAfterHeader = $headers.'Retry-After'
        }

        if ([string]::IsNullOrWhiteSpace([string]$retryAfterHeader) -eq $false) {
            $secondsValue = 0
            if ([int]::TryParse([string]$retryAfterHeader, [ref]$secondsValue)) {
                return [Math]::Max(0, $secondsValue)
            }

            $retryAfterDate = [DateTimeOffset]::MinValue
            if ([DateTimeOffset]::TryParse([string]$retryAfterHeader, [ref]$retryAfterDate)) {
                $deltaSeconds = [Math]::Ceiling(($retryAfterDate - [DateTimeOffset]::UtcNow).TotalSeconds)
                return [Math]::Max(0, [int]$deltaSeconds)
            }
        }
    }

    $regexMatch = [regex]::Match($Message, 'Retry the request in\s*\[?(\d+)\]?\s*seconds', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    if ($regexMatch.Success) {
        return [int]$regexMatch.Groups[1].Value
    }

    return $null
}

function Resolve-UiApiRetrySettings {
    [CmdletBinding()]
    param(
        [psobject]$RetryProfile
    )

    $maxRetries = 3
    $baseDelaySeconds = 1.0
    $maxDelaySeconds = 30.0
    $jitterSeconds = 0.25
    $allowRetryOnPost = $false
    $retryOnStatusCodes = @(429, 500, 502, 503, 504)
    $retryOnMethods = @('GET', 'HEAD', 'OPTIONS', 'DELETE')
    $randomSeed = 17

    if ($null -ne $RetryProfile) {
        if ($RetryProfile.PSObject.Properties.Name -contains 'maxRetries' -and $null -ne $RetryProfile.maxRetries) {
            $maxRetries = [int]$RetryProfile.maxRetries
        }

        if ($RetryProfile.PSObject.Properties.Name -contains 'baseDelaySeconds' -and $null -ne $RetryProfile.baseDelaySeconds) {
            $baseDelaySeconds = [double]$RetryProfile.baseDelaySeconds
        }

        if ($RetryProfile.PSObject.Properties.Name -contains 'maxDelaySeconds' -and $null -ne $RetryProfile.maxDelaySeconds) {
            $maxDelaySeconds = [double]$RetryProfile.maxDelaySeconds
        }

        if ($RetryProfile.PSObject.Properties.Name -contains 'jitterSeconds' -and $null -ne $RetryProfile.jitterSeconds) {
            $jitterSeconds = [double]$RetryProfile.jitterSeconds
        }

        if ($RetryProfile.PSObject.Properties.Name -contains 'allowRetryOnPost' -and $null -ne $RetryProfile.allowRetryOnPost) {
            $allowRetryOnPost = [bool]$RetryProfile.allowRetryOnPost
        }

        if ($RetryProfile.PSObject.Properties.Name -contains 'randomSeed' -and $null -ne $RetryProfile.randomSeed) {
            $randomSeed = [int]$RetryProfile.randomSeed
        }

        if ($RetryProfile.PSObject.Properties.Name -contains 'retryOnStatusCodes' -and $null -ne $RetryProfile.retryOnStatusCodes) {
            $statusCodeList = [System.Collections.Generic.List[int]]::new()
            foreach ($statusCode in @($RetryProfile.retryOnStatusCodes)) {
                if ($null -eq $statusCode) {
                    continue
                }

                $statusCodeList.Add([int]$statusCode) | Out-Null
            }

            if ($statusCodeList.Count -gt 0) {
                $retryOnStatusCodes = @($statusCodeList)
            }
        }

        if ($RetryProfile.PSObject.Properties.Name -contains 'retryOnMethods' -and $null -ne $RetryProfile.retryOnMethods) {
            $methodSet = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
            foreach ($method in @($RetryProfile.retryOnMethods)) {
                if ([string]::IsNullOrWhiteSpace([string]$method)) {
                    continue
                }

                $methodSet.Add(([string]$method).ToUpperInvariant()) | Out-Null
            }

            if ($methodSet.Count -gt 0) {
                $retryOnMethods = @($methodSet)
            }
        }
    }

    if ($allowRetryOnPost) {
        if (@($retryOnMethods | Where-Object { $_ -eq 'POST' }).Count -eq 0) {
            $retryOnMethods = @($retryOnMethods + @('POST'))
        }
    }
    else {
        $retryOnMethods = @($retryOnMethods | Where-Object { $_ -ne 'POST' })
    }

    return [pscustomobject]@{
        maxRetries = $maxRetries
        baseDelaySeconds = $baseDelaySeconds
        maxDelaySeconds = $maxDelaySeconds
        jitterSeconds = $jitterSeconds
        allowRetryOnPost = $allowRetryOnPost
        retryOnStatusCodes = @($retryOnStatusCodes)
        retryOnMethods = @($retryOnMethods)
        randomSeed = $randomSeed
    }
}

function Test-UiRetryableStatusCode {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [Nullable[int]]$StatusCode,

        [int[]]$RetryOnStatusCodes
    )

    if ($null -eq $StatusCode) {
        return $false
    }

    $statusValue = [int]$StatusCode
    if ($statusValue -ge 500 -and $statusValue -le 599) {
        return $true
    }

    foreach ($retryStatus in @($RetryOnStatusCodes)) {
        if ($statusValue -eq [int]$retryStatus) {
            return $true
        }
    }

    return $false
}

function Get-UiRequestFailureSummary {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [System.Exception]$Exception
    )

    $statusCode = $null
    $attempts = 1
    $retryable = $false
    $failureKind = 'fatal'

    if ($Exception.Data.Contains('UiApiStatusCode')) {
        $statusCode = [Nullable[int]][int]$Exception.Data['UiApiStatusCode']
    }
    else {
        $statusCode = Get-UiHttpStatusCode -Exception $Exception
    }

    if ($Exception.Data.Contains('UiApiAttempts')) {
        $attempts = [int]$Exception.Data['UiApiAttempts']
    }

    if ($Exception.Data.Contains('UiApiRetryable')) {
        $retryable = [bool]$Exception.Data['UiApiRetryable']
    }

    if ($Exception.Data.Contains('UiApiFailureKind')) {
        $failureKind = [string]$Exception.Data['UiApiFailureKind']
    }
    elseif ($retryable) {
        $failureKind = 'retry-exhausted'
    }

    $statusText = if ($null -ne $statusCode) { "HTTP $statusCode" } else { 'no HTTP status' }
    $displayText = switch ($failureKind) {
        'retry-exhausted' { "transient request failed after $attempts attempts ($statusText): $($Exception.Message)" }
        default           { "fatal request failure ($statusText): $($Exception.Message)" }
    }

    return [pscustomobject]@{
        StatusCode = $statusCode
        Attempts = $attempts
        Retryable = $retryable
        FailureKind = $failureKind
        Message = [string]$Exception.Message
        DisplayText = $displayText
    }
}

function Invoke-UiRequestWithRetry {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$Request,

        [psobject]$RetrySettings,

        [scriptblock]$RequestInvoker,

        [scriptblock]$SleepAction,

        [scriptblock]$LogAction
    )

    if ($null -eq $RetrySettings) {
        $RetrySettings = Resolve-UiApiRetrySettings
    }

    if ($null -eq $RequestInvoker) {
        $RequestInvoker = {
            param([hashtable]$InvokeParams)
            Invoke-RestMethod @InvokeParams
        }
    }

    if ($null -eq $SleepAction) {
        $SleepAction = { param([double]$Seconds) Start-Sleep -Seconds $Seconds }
    }

    $effectiveMethod = 'GET'
    if ($Request.PSObject.Properties.Name -contains 'Method' -and [string]::IsNullOrWhiteSpace([string]$Request.Method) -eq $false) {
        $effectiveMethod = ([string]$Request.Method).ToUpperInvariant()
    }

    $allowedMethods = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($retryMethod in @($RetrySettings.retryOnMethods)) {
        if ([string]::IsNullOrWhiteSpace([string]$retryMethod)) {
            continue
        }

        $allowedMethods.Add(([string]$retryMethod).ToUpperInvariant()) | Out-Null
    }

    if ($allowedMethods.Count -eq 0) {
        $allowedMethods.Add('GET') | Out-Null
        $allowedMethods.Add('HEAD') | Out-Null
        $allowedMethods.Add('OPTIONS') | Out-Null
        $allowedMethods.Add('DELETE') | Out-Null
    }

    if ($RetrySettings.allowRetryOnPost) {
        $allowedMethods.Add('POST') | Out-Null
    }
    else {
        $allowedMethods.Remove('POST') | Out-Null
    }

    $retryableMethod = $allowedMethods.Contains($effectiveMethod)
    $attempt = 0
    $maxAttempts = [int]$RetrySettings.maxRetries + 1
    $random = [System.Random]::new([int]$RetrySettings.randomSeed)

    while ($true) {
        $attempt++
        try {
            $invokeParams = @{
                Uri = [string]$Request.Uri
                Method = $effectiveMethod
                ErrorAction = 'Stop'
                TimeoutSec = 120
            }

            if ($Request.PSObject.Properties.Name -contains 'Headers' -and $null -ne $Request.Headers) {
                $invokeParams.Headers = $Request.Headers
            }

            if ($Request.PSObject.Properties.Name -contains 'TimeoutSec' -and $null -ne $Request.TimeoutSec) {
                $invokeParams.TimeoutSec = [int]$Request.TimeoutSec
            }

            if ($Request.PSObject.Properties.Name -contains 'Body' -and [string]::IsNullOrWhiteSpace([string]$Request.Body) -eq $false) {
                $invokeParams.Body = $Request.Body

                if ($Request.PSObject.Properties.Name -contains 'ContentType' -and [string]::IsNullOrWhiteSpace([string]$Request.ContentType) -eq $false) {
                    $invokeParams.ContentType = [string]$Request.ContentType
                }
                elseif ($effectiveMethod -in @('POST', 'PUT', 'PATCH')) {
                    $bodyString = [string]$Request.Body
                    $trimmedBody = $bodyString.TrimStart()
                    if ($trimmedBody.StartsWith('{') -or $trimmedBody.StartsWith('[')) {
                        $invokeParams.ContentType = 'application/json'
                    }
                }
            }

            $result = & $RequestInvoker $invokeParams
            if ($null -ne $result -and $result.PSObject.Properties.Name -contains 'Result') {
                return $result.Result
            }

            return $result
        }
        catch {
            $exception = $_.Exception
            $statusCode = Get-UiHttpStatusCode -Exception $exception
            $message = [string]$exception.Message
            $isRetryableStatus = Test-UiRetryableStatusCode -StatusCode $statusCode -RetryOnStatusCodes $RetrySettings.retryOnStatusCodes
            $isRetryableError = $retryableMethod -and $isRetryableStatus

            if ($isRetryableError -eq $false -or $attempt -ge $maxAttempts) {
                if ($null -ne $statusCode) {
                    $exception.Data['UiApiStatusCode'] = [int]$statusCode
                }
                $exception.Data['UiApiAttempts'] = $attempt
                $exception.Data['UiApiRetryable'] = $isRetryableError
                $exception.Data['UiApiFailureKind'] = if ($isRetryableError) { 'retry-exhausted' } else { 'fatal' }
                throw
            }

            $retryAfterSeconds = Get-UiRetryAfterSeconds -Exception $exception -Message $message
            $backoff = [Math]::Min([double]$RetrySettings.maxDelaySeconds, [double]$RetrySettings.baseDelaySeconds * [Math]::Pow(2, $attempt - 1))
            if ($null -ne $retryAfterSeconds) {
                $backoff = [Math]::Min([double]$RetrySettings.maxDelaySeconds, [double]$retryAfterSeconds)
            }

            $jitterOffset = 0
            if ([double]$RetrySettings.jitterSeconds -gt 0) {
                $jitterOffset = ($random.NextDouble() * 2 - 1) * [double]$RetrySettings.jitterSeconds
            }

            $delaySeconds = [Math]::Max(0, [Math]::Round($backoff + $jitterOffset, 3))

            if ($null -ne $LogAction) {
                & $LogAction ([pscustomobject]@{
                    Attempt = $attempt
                    MaxAttempts = $maxAttempts
                    Method = $effectiveMethod
                    Uri = [string]$Request.Uri
                    StatusCode = $statusCode
                    DelaySeconds = $delaySeconds
                    RetryAfterSeconds = $retryAfterSeconds
                })
            }

            & $SleepAction $delaySeconds
        }
    }
}

function Invoke-UiApiRequest {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$BaseUri,

        [Parameter(Mandatory = $true)]
        [string]$Method,

        [Parameter(Mandatory = $true)]
        [string]$Path,

        [hashtable]$Headers,

        [string]$Body,

        [hashtable]$QueryParams,

        [psobject]$RetrySettings,

        [scriptblock]$RequestInvoker,

        [scriptblock]$SleepAction,

        [scriptblock]$LogAction
    )

    $uri = "$BaseUri$Path"
    if ($QueryParams -and $QueryParams.Count -gt 0) {
        $qs = ($QueryParams.GetEnumerator() |
            ForEach-Object { "$($_.Key)=$([Uri]::EscapeDataString([string]$_.Value))" }) -join '&'
        $uri = "$uri?$qs"
    }

    $request = [ordered]@{
        Uri = $uri
        Method = $Method
        Headers = $Headers
        TimeoutSec = 120
    }

    if ([string]::IsNullOrWhiteSpace($Body) -eq $false) {
        $request['Body'] = $Body
        $request['ContentType'] = 'application/json'
    }

    return Invoke-UiRequestWithRetry -Request ([pscustomobject]$request) -RetrySettings $RetrySettings -RequestInvoker $RequestInvoker -SleepAction $SleepAction -LogAction $LogAction
}
