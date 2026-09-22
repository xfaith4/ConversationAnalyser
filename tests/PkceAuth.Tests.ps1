# Pester 5 tests for src/auth/PkceAuth.ps1 (pure logic only; no network, no browser).
# Run: Invoke-Pester -Path .\tests\PkceAuth.Tests.ps1 -Output Detailed

BeforeAll {
    . (Join-Path $PSScriptRoot '..\src\auth\PkceAuth.ps1')
}

Describe 'New-PkceChallenge' {
    It 'produces a base64url verifier of 43-128 unreserved characters' {
        $p = New-PkceChallenge
        $p.Verifier.Length | Should -BeGreaterOrEqual 43
        $p.Verifier.Length | Should -BeLessOrEqual 128
        $p.Verifier | Should -Match '^[A-Za-z0-9\-_]+$'
        $p.Method | Should -Be 'S256'
    }

    It 'derives the S256 challenge from the verifier' {
        $p = New-PkceChallenge
        (Get-PkceCodeChallenge -CodeVerifier $p.Verifier) | Should -Be $p.Challenge
        $p.Challenge.Length | Should -Be 43   # 32-byte SHA-256, base64url without padding
    }

    It 'matches the RFC 7636 appendix B example' {
        Get-PkceCodeChallenge -CodeVerifier 'dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk' |
            Should -Be 'E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM'
    }

    It 'generates a different verifier each time' {
        (New-PkceChallenge).Verifier | Should -Not -Be (New-PkceChallenge).Verifier
    }
}

Describe 'Get-PkceAuthorizeUri' {
    It 'builds the Genesys authorize URL with every PKCE parameter' {
        $u = Get-PkceAuthorizeUri -Region 'usw2.pure.cloud' -ClientId 'abc 123' -RedirectUri 'http://localhost:8085/callback/' `
            -CodeChallenge 'CH' -State 'ST' -Scope 'analytics:readonly'
        $u | Should -BeLike 'https://login.usw2.pure.cloud/oauth/authorize?*'
        $u | Should -Match 'response_type=code'
        $u | Should -Match 'client_id=abc%20123'
        $u | Should -Match 'redirect_uri=http%3A%2F%2Flocalhost%3A8085%2Fcallback%2F'
        $u | Should -Match 'code_challenge=CH'
        $u | Should -Match 'code_challenge_method=S256'
        $u | Should -Match 'state=ST'
        $u | Should -Match 'scope=analytics%3Areadonly'
    }

    It 'omits scope when blank' {
        Get-PkceAuthorizeUri -Region 'r' -ClientId 'c' -RedirectUri 'http://localhost:1/' -CodeChallenge 'x' -State 's' |
            Should -Not -Match 'scope='
    }
}

Describe 'Get-PkceListenerPrefix' {
    It 'adds the port and a trailing slash' {
        Get-PkceListenerPrefix -RedirectUri 'http://localhost:8085/callback' | Should -Be 'http://localhost:8085/callback/'
        Get-PkceListenerPrefix -RedirectUri 'http://localhost/cb/' | Should -Be 'http://localhost:80/cb/'
    }

    It 'rejects non-http redirect URIs' {
        { Get-PkceListenerPrefix -RedirectUri 'myapp://callback' } | Should -Throw
        { Get-PkceListenerPrefix -RedirectUri 'not a uri' } | Should -Throw
        Test-PkceRedirectUri -RedirectUri '' | Should -BeFalse
    }
}

Describe 'Get-PkceCallbackResult' {
    It 'parses code and state from a redirect URL' {
        $r = Get-PkceCallbackResult -Text 'http://localhost:8085/callback/?code=abc%2Bdef&state=s1'
        $r.Code | Should -Be 'abc+def'
        $r.State | Should -Be 's1'
        $r.Error | Should -BeNullOrEmpty
    }

    It 'parses an error response' {
        $r = Get-PkceCallbackResult -Text 'http://localhost:8085/callback/?error=access_denied&error_description=User%20cancelled'
        $r.Error | Should -Be 'access_denied'
        $r.ErrorDescription | Should -Be 'User cancelled'
        $r.Code | Should -BeNullOrEmpty
    }

    It 'treats a bare string as the code' {
        (Get-PkceCallbackResult -Text ' rawcode ').Code | Should -Be 'rawcode'
    }

    It 'accepts a pasted query string without a host' {
        (Get-PkceCallbackResult -Text 'code=xyz&state=q').Code | Should -Be 'xyz'
    }
}

Describe 'ConvertTo-PkceTokenRecord' {
    It 'maps the token response and computes expiry' {
        $resp = [pscustomobject]@{ access_token = 'tok'; token_type = 'bearer'; expires_in = 3600; refresh_token = 'ref' }
        $rec = ConvertTo-PkceTokenRecord -TokenResponse $resp -Region 'r' -ClientId 'c'
        $rec.AccessToken | Should -Be 'tok'
        $rec.RefreshToken | Should -Be 'ref'
        $rec.ExpiresAt | Should -BeGreaterThan ([DateTime]::Now.AddMinutes(55))
        $rec.Region | Should -Be 'r'
    }

    It 'throws when access_token is missing' {
        { ConvertTo-PkceTokenRecord -TokenResponse ([pscustomobject]@{ error = 'x' }) -Region 'r' -ClientId 'c' } | Should -Throw
    }
}

Describe 'Wait-PkceAuthorizationCode (loopback listener)' {
    It 'captures the code from a real local callback and validates state' {
        $port = 18085 + (Get-Random -Maximum 1000)
        $prefix = "http://localhost:$port/callback/"
        $listener = New-Object System.Net.HttpListener
        $listener.Prefixes.Add($prefix)
        $listener.Start()
        try {
            $job = Start-Job -ScriptBlock {
                param($u)
                Start-Sleep -Milliseconds 400
                Invoke-WebRequest -Uri $u -UseBasicParsing | Select-Object -ExpandProperty StatusCode
            } -ArgumentList "${prefix}?code=CODE1&state=STATE1"
            $code = Wait-PkceAuthorizationCode -Listener $listener -ExpectedState 'STATE1' -TimeoutSeconds 20
            $code | Should -Be 'CODE1'
            ($job | Wait-Job -Timeout 20 | Receive-Job) | Should -Be 200
        }
        finally {
            $listener.Stop(); $listener.Close()
            Get-Job | Remove-Job -Force
        }
    }

    It 'rejects a callback whose state does not match' {
        $port = 19085 + (Get-Random -Maximum 1000)
        $prefix = "http://localhost:$port/callback/"
        $listener = New-Object System.Net.HttpListener
        $listener.Prefixes.Add($prefix)
        $listener.Start()
        try {
            $job = Start-Job -ScriptBlock {
                param($u)
                Start-Sleep -Milliseconds 400
                try { Invoke-WebRequest -Uri $u -UseBasicParsing | Out-Null } catch {}
            } -ArgumentList "${prefix}?code=CODE1&state=WRONG"
            { Wait-PkceAuthorizationCode -Listener $listener -ExpectedState 'STATE1' -TimeoutSeconds 20 } | Should -Throw '*State mismatch*'
            $job | Wait-Job -Timeout 20 | Out-Null
        }
        finally {
            $listener.Stop(); $listener.Close()
            Get-Job | Remove-Job -Force
        }
    }
}
