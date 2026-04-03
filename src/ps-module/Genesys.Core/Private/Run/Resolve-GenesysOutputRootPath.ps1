function Resolve-GenesysOutputRootPath {
    [CmdletBinding()]
    param(
        [string]$OutputRoot = 'out'
    )

    $effectiveOutputRoot = [string]$OutputRoot
    if ([string]::IsNullOrWhiteSpace($effectiveOutputRoot)) {
        $effectiveOutputRoot = 'out'
    }

    if ([System.IO.Path]::IsPathRooted($effectiveOutputRoot)) {
        return [System.IO.Path]::GetFullPath($effectiveOutputRoot)
    }

    return [System.IO.Path]::GetFullPath((Join-Path -Path (Get-Location).Path -ChildPath $effectiveOutputRoot))
}
