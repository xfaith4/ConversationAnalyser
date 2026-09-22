# -----------------------------------------------------------------------------
# PkceAuth.ps1 - Genesys Cloud OAuth 2.0 Authorization Code + PKCE (RFC 7636)
#
# Pure auth logic: no WPF, no app state. The host supplies optional callbacks for
# opening the browser, pumping its UI loop while waiting, and a manual fallback.
#
# Flow (https://developer.genesys.cloud/authorization/platform-auth/use-pkce):
#   1. Generate a code_verifier (43-128 chars) and its S256 code_challenge.
#   2. Open https://login.<region>/oauth/authorize?response_type=code&client_id=..
#      &redirect_uri=..&code_challenge=..&code_challenge_method=S256&state=..
#   3. Genesys redirects the browser to redirect_uri?code=..&state=..; a local
#      HttpListener on that URI captures the code.
#   4. POST https://login.<region>/oauth/token with grant_type=authorization_code,
#      code, redirect_uri, code_verifier, client_id. No Authorization header and
#      no client secret: the OAuth client is a public "Code Authorization" client.
#
# Adapted from GenesysCloudAuthenticator.ps1 (New-PKCEChallenge / Build-AuthorizeUrl).
# Windows PowerShell 5.1 and PowerShell 7+.
# -----------------------------------------------------------------------------

function ConvertTo-PkceBase64Url {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][byte[]]$Bytes)
    # URL-safe base64 (RFC 4648 section 5): '+' -> '-', '/' -> '_', no '=' padding.
    return [Convert]::ToBase64String($Bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
}

function Get-PkceRandomBase64Url {
    [CmdletBinding()]
    param([int]$ByteCount = 32)
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $buffer = New-Object byte[] $ByteCount
        $rng.GetBytes($buffer)
        return ConvertTo-PkceBase64Url -Bytes $buffer
    }
    finally { $rng.Dispose() }
}

function Get-PkceCodeChallenge {
    <#
    .SYNOPSIS
        S256 challenge for a verifier: base64url(SHA256(ASCII(verifier))).
    #>
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][string]$CodeVerifier)
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $hash = $sha.ComputeHash([System.Text.Encoding]::ASCII.GetBytes($CodeVerifier))
    }
    finally { $sha.Dispose() }
    return ConvertTo-PkceBase64Url -Bytes $hash
}

function New-PkceChallenge {
    <#
    .SYNOPSIS
        Generate a PKCE pair: 64 random bytes -> 86-char base64url verifier, S256 challenge.
    .OUTPUTS
        [pscustomobject] Verifier, Challenge, Method ('S256')
    #>
    [CmdletBinding()]
    param()
    $verifier = Get-PkceRandomBase64Url -ByteCount 64
    return [pscustomobject]@{
        Verifier  = $verifier
        Challenge = Get-PkceCodeChallenge -CodeVerifier $verifier
        Method    = 'S256'
    }
}

function Test-PkceRedirectUri {
    <#
    .SYNOPSIS
        True when the redirect URI is an absolute http(s) URI the local listener can serve.
    #>
    [CmdletBinding()]
    param([string]$RedirectUri)
    if ([string]::IsNullOrWhiteSpace($RedirectUri)) { return $false }
    $uri = $null
    if (-not [System.Uri]::TryCreate($RedirectUri, [System.UriKind]::Absolute, [ref]$uri)) { return $false }
    return ($uri.Scheme -eq 'http' -or $uri.Scheme -eq 'https')
}

function Get-PkceListenerPrefix {
    <#
    .SYNOPSIS
        HttpListener prefix for a redirect URI (scheme://host:port/path/ with a trailing slash).
    #>
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][string]$RedirectUri)
    if (-not (Test-PkceRedirectUri -RedirectUri $RedirectUri)) {
        throw "Redirect URI '$RedirectUri' must be an absolute http(s) URI such as http://localhost:8085/callback/"
    }
    $uri = [System.Uri]$RedirectUri
    $path = $uri.AbsolutePath
    if (-not $path.EndsWith('/')) { $path += '/' }
    return "{0}://{1}:{2}{3}" -f $uri.Scheme, $uri.Host, $uri.Port, $path
}

function Get-PkceAuthorizeUri {
    <#
    .SYNOPSIS
        Build the /oauth/authorize URL for the PKCE flow.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Region,
        [Parameter(Mandatory = $true)][string]$ClientId,
        [Parameter(Mandatory = $true)][string]$RedirectUri,
        [Parameter(Mandatory = $true)][string]$CodeChallenge,
        [Parameter(Mandatory = $true)][string]$State,
        [string]$CodeChallengeMethod = 'S256',
        [string]$Scope
    )
    $q = @(
        'response_type=code',
        "client_id=$([System.Uri]::EscapeDataString($ClientId))",
        "redirect_uri=$([System.Uri]::EscapeDataString($RedirectUri))",
        "code_challenge=$([System.Uri]::EscapeDataString($CodeChallenge))",
        "code_challenge_method=$([System.Uri]::EscapeDataString($CodeChallengeMethod))",
        "state=$([System.Uri]::EscapeDataString($State))"
    )
    if (-not [string]::IsNullOrWhiteSpace($Scope)) { $q += "scope=$([System.Uri]::EscapeDataString($Scope))" }
    return "https://login.$Region/oauth/authorize?$($q -join '&')"
}

function Get-PkceCallbackResult {
    <#
    .SYNOPSIS
        Parse code/state/error from a redirect URL (or a bare pasted code).
    .OUTPUTS
        [pscustomobject] Code, State, Error, ErrorDescription
    #>
    [CmdletBinding()]
    param([string]$Text)
    $result = [pscustomobject]@{ Code = $null; State = $null; Error = $null; ErrorDescription = $null }
    if ([string]::IsNullOrWhiteSpace($Text)) { return $result }
    $t = $Text.Trim()

    $query = $null
    $uri = $null
    if ([System.Uri]::TryCreate($t, [System.UriKind]::Absolute, [ref]$uri)) {
        $query = $uri.Query
        # Some browsers put the parameters in the fragment when copied from the address bar.
        if ([string]::IsNullOrWhiteSpace($query) -and $uri.Fragment -match '[?&]?(code|state|error)=') { $query = $uri.Fragment.TrimStart('#') }
    }
    elseif ($t -match '(^|[?&])(code|state|error)=') { $query = $t }

    if ($null -eq $query) {
        $result.Code = $t   # treat the whole string as a bare authorization code
        return $result
    }

    foreach ($pair in $query.TrimStart('?').Split('&')) {
        if ($pair -notmatch '=') { continue }
        $kv = $pair.Split('=', 2)
        $key = [System.Uri]::UnescapeDataString($kv[0])
        $val = [System.Uri]::UnescapeDataString($kv[1].Replace('+', ' '))
        switch ($key) {
            'code' { $result.Code = $val }
            'state' { $result.State = $val }
            'error' { $result.Error = $val }
            'error_description' { $result.ErrorDescription = $val }
        }
    }
    return $result
}

function Get-PkceHttpErrorText {
    [CmdletBinding()]
    param([System.Management.Automation.ErrorRecord]$ErrorRecord)
    $msg = $ErrorRecord.Exception.Message
    try {
        if ($null -ne $ErrorRecord.ErrorDetails -and -not [string]::IsNullOrWhiteSpace($ErrorRecord.ErrorDetails.Message)) {
            $msg = $ErrorRecord.ErrorDetails.Message
        }
        elseif ($null -ne $ErrorRecord.Exception.Response -and $ErrorRecord.Exception.Response -is [System.Net.HttpWebResponse]) {
            $stream = $ErrorRecord.Exception.Response.GetResponseStream()
            if ($null -ne $stream) {
                $reader = New-Object System.IO.StreamReader($stream)
                $body = $reader.ReadToEnd()
                $reader.Dispose()
                if (-not [string]::IsNullOrWhiteSpace($body)) { $msg = $body }
            }
        }
    }
    catch {}
    try {
        $j = $msg | ConvertFrom-Json -ErrorAction Stop
        if ($null -ne $j.error) {
            $desc = if ($j.description) { $j.description } elseif ($j.error_description) { $j.error_description } else { '' }
            return ("{0}: {1}" -f $j.error, $desc).TrimEnd(': ')
        }
    }
    catch {}
    return $msg
}

function ConvertTo-PkceTokenRecord {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][psobject]$TokenResponse,
        [Parameter(Mandatory = $true)][string]$Region,
        [Parameter(Mandatory = $true)][string]$ClientId
    )
    if ($null -eq $TokenResponse.access_token) { throw 'Token response did not include an access_token.' }
    $expiresIn = 0
    if ($null -ne $TokenResponse.expires_in) { $expiresIn = [int]$TokenResponse.expires_in }
    $refresh = $null
    if ($TokenResponse.PSObject.Properties.Name -contains 'refresh_token') { $refresh = [string]$TokenResponse.refresh_token }
    return [pscustomobject]@{
        AccessToken  = [string]$TokenResponse.access_token
        TokenType    = [string]$TokenResponse.token_type
        RefreshToken = $refresh
        ExpiresAt    = if ($expiresIn -gt 0) { [DateTime]::Now.AddSeconds($expiresIn) } else { $null }
        Region       = $Region
        ClientId     = $ClientId
        ObtainedAt   = [DateTime]::Now
    }
}

function Invoke-PkceTokenExchange {
    <#
    .SYNOPSIS
        Exchange an authorization code for tokens. Public client: client_id in the body, no Basic auth.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Region,
        [Parameter(Mandatory = $true)][string]$ClientId,
        [Parameter(Mandatory = $true)][string]$RedirectUri,
        [Parameter(Mandatory = $true)][string]$Code,
        [Parameter(Mandatory = $true)][string]$CodeVerifier
    )
    $body = @{
        grant_type    = 'authorization_code'
        code          = $Code
        redirect_uri  = $RedirectUri
        code_verifier = $CodeVerifier
        client_id     = $ClientId
    }
    try {
        $resp = Invoke-RestMethod -Uri "https://login.$Region/oauth/token" -Method POST `
            -ContentType 'application/x-www-form-urlencoded' -Body $body -ErrorAction Stop
    }
    catch { throw "Token exchange failed: $(Get-PkceHttpErrorText -ErrorRecord $_)" }
    return ConvertTo-PkceTokenRecord -TokenResponse $resp -Region $Region -ClientId $ClientId
}

function Invoke-PkceTokenRefresh {
    <#
    .SYNOPSIS
        Refresh a PKCE token. Only works when the OAuth client has refresh tokens enabled.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Region,
        [Parameter(Mandatory = $true)][string]$ClientId,
        [Parameter(Mandatory = $true)][string]$RefreshToken
    )
    $body = @{
        grant_type    = 'refresh_token'
        refresh_token = $RefreshToken
        client_id     = $ClientId
    }
    try {
        $resp = Invoke-RestMethod -Uri "https://login.$Region/oauth/token" -Method POST `
            -ContentType 'application/x-www-form-urlencoded' -Body $body -ErrorAction Stop
    }
    catch { throw "Token refresh failed: $(Get-PkceHttpErrorText -ErrorRecord $_)" }
    $rec = ConvertTo-PkceTokenRecord -TokenResponse $resp -Region $Region -ClientId $ClientId
    if ([string]::IsNullOrWhiteSpace($rec.RefreshToken)) { $rec.RefreshToken = $RefreshToken }
    return $rec
}

function Get-PkceCallbackHtml {
    [CmdletBinding()]
    param([bool]$Success, [string]$Message)
    $title = if ($Success) { 'Signed in' } else { 'Sign-in failed' }
    $color = if ($Success) { '#1a7f37' } else { '#b42318' }
    $safe = [System.Net.WebUtility]::HtmlEncode($Message)
    return @"
<!DOCTYPE html><html><head><meta charset="utf-8"><title>Genesys Conversation Analyzer - $title</title>
<style>body{font-family:Segoe UI,Arial,sans-serif;margin:60px auto;max-width:520px;color:#222}h1{color:$color;font-size:22px}p{font-size:15px}</style>
</head><body><h1>$title</h1><p>$safe</p><p>You can close this browser tab and return to Genesys Conversation Analyzer.</p></body></html>
"@
}

function Wait-PkceAuthorizationCode {
    <#
    .SYNOPSIS
        Listen on the redirect URI for the OAuth callback and return its code.
    .PARAMETER Listener
        A started [System.Net.HttpListener] whose prefix matches the redirect URI.
    .PARAMETER PumpAction
        Optional scriptblock run every 100 ms while waiting (for a UI message pump).
    .PARAMETER CancelCheck
        Optional scriptblock returning $true to abort the wait.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][System.Net.HttpListener]$Listener,
        [Parameter(Mandatory = $true)][string]$ExpectedState,
        [int]$TimeoutSeconds = 300,
        [scriptblock]$PumpAction,
        [scriptblock]$CancelCheck
    )
    $deadline = [DateTime]::Now.AddSeconds($TimeoutSeconds)
    while ($true) {
        $task = $Listener.GetContextAsync()
        while (-not $task.Wait(100)) {
            if ($null -ne $PumpAction) { & $PumpAction }
            if ($null -ne $CancelCheck -and (& $CancelCheck)) { throw 'Sign-in cancelled.' }
            if ([DateTime]::Now -gt $deadline) { throw "Timed out after $TimeoutSeconds seconds waiting for the browser sign-in to complete." }
        }
        $context = $task.Result
        $request = $context.Request
        $response = $context.Response

        # Browsers request /favicon.ico; anything without a code/error keeps the listener waiting.
        $parsed = Get-PkceCallbackResult -Text $request.Url.AbsoluteUri
        $hasPayload = -not [string]::IsNullOrWhiteSpace($parsed.Code) -or -not [string]::IsNullOrWhiteSpace($parsed.Error)
        if (-not $hasPayload -or $request.Url.AbsolutePath -like '*favicon*') {
            $response.StatusCode = 404
            $response.Close()
            continue
        }

        $failure = $null
        if (-not [string]::IsNullOrWhiteSpace($parsed.Error)) {
            $failure = ("{0}: {1}" -f $parsed.Error, $parsed.ErrorDescription).TrimEnd(': ')
        }
        elseif ($parsed.State -ne $ExpectedState) {
            $failure = 'State mismatch: the callback did not come from this sign-in attempt.'
        }

        $html = if ($null -eq $failure) { Get-PkceCallbackHtml -Success $true -Message 'Authentication succeeded.' } else { Get-PkceCallbackHtml -Success $false -Message $failure }
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($html)
        $response.ContentType = 'text/html; charset=utf-8'
        $response.ContentLength64 = $bytes.Length
        $response.OutputStream.Write($bytes, 0, $bytes.Length)
        $response.Close()

        if ($null -ne $failure) { throw "Authorization failed: $failure" }
        return [string]$parsed.Code
    }
}

function Invoke-PkceLogin {
    <#
    .SYNOPSIS
        Run the full interactive PKCE login and return a token record.
    .PARAMETER OpenBrowser
        Scriptblock receiving the authorize URL. Defaults to Start-Process.
    .PARAMETER ManualCodePrompt
        Optional scriptblock receiving the authorize URL, used when the local listener
        cannot start (port in use). It must return the pasted redirect URL or code, or $null.
    .OUTPUTS
        [pscustomobject] AccessToken, RefreshToken, ExpiresAt, Region, ClientId, ...
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Region,
        [Parameter(Mandatory = $true)][string]$ClientId,
        [Parameter(Mandatory = $true)][string]$RedirectUri,
        [string]$Scope,
        [int]$TimeoutSeconds = 300,
        [scriptblock]$OpenBrowser,
        [scriptblock]$PumpAction,
        [scriptblock]$CancelCheck,
        [scriptblock]$ManualCodePrompt,
        [scriptblock]$Log
    )
    if ([string]::IsNullOrWhiteSpace($ClientId)) { throw 'PKCE Client ID is required.' }
    if (-not (Test-PkceRedirectUri -RedirectUri $RedirectUri)) {
        throw "Redirect URI '$RedirectUri' must be an absolute http(s) URI such as http://localhost:8085/callback/"
    }
    if ($null -eq $OpenBrowser) { $OpenBrowser = { param($u) Start-Process $u | Out-Null } }
    if ($null -eq $Log) { $Log = { param($m) Write-Verbose $m } }

    $pkce = New-PkceChallenge
    $state = Get-PkceRandomBase64Url -ByteCount 16
    $authorizeUri = Get-PkceAuthorizeUri -Region $Region -ClientId $ClientId -RedirectUri $RedirectUri `
        -CodeChallenge $pkce.Challenge -CodeChallengeMethod $pkce.Method -State $state -Scope $Scope

    $listener = $null
    $code = $null
    try {
        $prefix = Get-PkceListenerPrefix -RedirectUri $RedirectUri
        $listener = New-Object System.Net.HttpListener
        $listener.Prefixes.Add($prefix)
        try {
            $listener.Start()
            & $Log "Listening for the OAuth callback on $prefix"
        }
        catch {
            $listener.Close()
            $listener = $null
            & $Log "Could not listen on $prefix ($($_.Exception.Message))."
            if ($null -eq $ManualCodePrompt) { throw "Could not start the local callback listener on $prefix. Close any app using that port, or change the redirect URI. $($_.Exception.Message)" }
        }

        & $Log 'Opening the browser for Genesys Cloud sign-in...'
        & $OpenBrowser $authorizeUri

        if ($null -ne $listener) {
            $code = Wait-PkceAuthorizationCode -Listener $listener -ExpectedState $state -TimeoutSeconds $TimeoutSeconds -PumpAction $PumpAction -CancelCheck $CancelCheck
        }
        else {
            $pasted = & $ManualCodePrompt $authorizeUri
            $parsed = Get-PkceCallbackResult -Text ([string]$pasted)
            if (-not [string]::IsNullOrWhiteSpace($parsed.Error)) { throw "Authorization failed: $($parsed.Error) $($parsed.ErrorDescription)".Trim() }
            if ([string]::IsNullOrWhiteSpace($parsed.Code)) { throw 'Sign-in cancelled: no authorization code was provided.' }
            if (-not [string]::IsNullOrWhiteSpace($parsed.State) -and $parsed.State -ne $state) { throw 'State mismatch: the pasted callback is not from this sign-in attempt.' }
            $code = $parsed.Code
        }
    }
    finally {
        if ($null -ne $listener) { try { $listener.Stop(); $listener.Close() } catch {} }
    }

    & $Log 'Exchanging the authorization code for a token...'
    return Invoke-PkceTokenExchange -Region $Region -ClientId $ClientId -RedirectUri $RedirectUri -Code $code -CodeVerifier $pkce.Verifier
}
