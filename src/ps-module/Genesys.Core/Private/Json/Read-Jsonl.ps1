function Read-Jsonl {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [string]$Path
    )

    if (-not (Test-Path -Path $Path -PathType Leaf)) {
        throw "JSONL file '$($Path)' was not found."
    }

    $reader = [System.IO.File]::OpenText((Resolve-Path -Path $Path).Path)
    try {
        while (($line = $reader.ReadLine()) -ne $null) {
            if ([string]::IsNullOrWhiteSpace($line)) {
                continue
            }

            $trimmed = $line.Trim()
            if ([string]::IsNullOrWhiteSpace($trimmed)) {
                continue
            }

            $trimmed | ConvertFrom-Json
        }
    }
    finally {
        $reader.Dispose()
    }
}
