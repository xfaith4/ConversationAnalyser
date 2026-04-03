function Resolve-NormalizedEntityPath {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$NormalizedPath,

        [Parameter(Mandatory = $true)]
        [ValidateSet('conversations', 'participants', 'sessions', 'segments', 'metrics', 'attributes')]
        [string]$Entity
    )

    $resolvedBasePath = if (Test-Path -Path $NormalizedPath) { (Resolve-Path -Path $NormalizedPath).Path } else { $null }
    if ($null -eq $resolvedBasePath) {
        throw "Normalized path '$($NormalizedPath)' was not found."
    }

    if (Test-Path -Path $resolvedBasePath -PathType Leaf) {
        return $resolvedBasePath
    }

    $candidate = Join-Path -Path $resolvedBasePath -ChildPath "data/$($Entity).jsonl"
    if (Test-Path -Path $candidate -PathType Leaf) {
        return (Resolve-Path -Path $candidate).Path
    }

    $fallback = Join-Path -Path $resolvedBasePath -ChildPath "$($Entity).jsonl"
    if (Test-Path -Path $fallback -PathType Leaf) {
        return (Resolve-Path -Path $fallback).Path
    }

    throw "Normalized entity '$($Entity)' was not found under '$($resolvedBasePath)'."
}

function Get-AnalysisFieldValue {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$Record,

        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [string]$Field
    )

    if ($null -eq $Record) {
        return $null
    }

    if ($Field.Contains('.')) {
        $current = $Record
        foreach ($part in $Field.Split('.')) {
            if ($null -eq $current) {
                return $null
            }

            if ($current -is [System.Collections.IDictionary]) {
                if (-not $current.Contains($part) -and -not ($current -is [hashtable] -and $current.ContainsKey($part))) {
                    return $null
                }

                $current = $current[$part]
                continue
            }

            if ($current.PSObject.Properties.Name -notcontains $part) {
                return $null
            }

            $current = $current.$part
        }

        return $current
    }

    if ($Record -is [System.Collections.IDictionary]) {
        if ($Record.Contains($Field)) {
            return $Record[$Field]
        }

        if ($Record -is [hashtable] -and $Record.ContainsKey($Field)) {
            return $Record[$Field]
        }
    }

    if ($Record.PSObject.Properties.Name -contains $Field) {
        return $Record.$Field
    }

    return $null
}

function ConvertTo-AnalysisScalar {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$Value
    )

    if ($null -eq $Value) {
        return $null
    }

    if ($Value -is [bool] -or $Value -is [int] -or $Value -is [long] -or $Value -is [double] -or $Value -is [decimal] -or $Value -is [datetime]) {
        return $Value
    }

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
        return [string]::Join('|', @($Value | ForEach-Object { [string]$_ }))
    }

    $text = [string]$Value
    if ([string]::IsNullOrWhiteSpace($text)) {
        return ''
    }

    $dateValue = [DateTime]::MinValue
    if ([DateTime]::TryParse($text, [ref]$dateValue)) {
        return $dateValue
    }

    $doubleValue = 0.0
    if ([double]::TryParse($text, [ref]$doubleValue)) {
        return $doubleValue
    }

    return $text
}

function Test-AnalysisCondition {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$LeftValue,

        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [string]$Operator,

        [AllowNull()]
        [object]$RightValue
    )

    $normalizedOperator = $Operator.ToLowerInvariant()
    $left = ConvertTo-AnalysisScalar -Value $LeftValue
    $right = ConvertTo-AnalysisScalar -Value $RightValue

    switch ($normalizedOperator) {
        'eq' { return [object]::Equals($left, $right) }
        'ne' { return -not [object]::Equals($left, $right) }
        'gt' { return ($null -ne $left -and $null -ne $right -and $left -gt $right) }
        'gte' { return ($null -ne $left -and $null -ne $right -and $left -ge $right) }
        'lt' { return ($null -ne $left -and $null -ne $right -and $left -lt $right) }
        'lte' { return ($null -ne $left -and $null -ne $right -and $left -le $right) }
        'contains' { return ([string]$left).IndexOf([string]$right, [System.StringComparison]::OrdinalIgnoreCase) -ge 0 }
        'isempty' { return [string]::IsNullOrWhiteSpace([string]$left) }
        'notempty' { return -not [string]::IsNullOrWhiteSpace([string]$left) }
        'in' {
            $items = @($RightValue)
            foreach ($item in $items) {
                if ([object]::Equals($left, (ConvertTo-AnalysisScalar -Value $item))) {
                    return $true
                }
            }

            return $false
        }
        'notin' {
            return -not (Test-AnalysisCondition -LeftValue $LeftValue -Operator 'in' -RightValue $RightValue)
        }
        default {
            throw "Unsupported filter operator '$($Operator)'."
        }
    }
}

function Test-AnalysisFilters {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Record,

        [AllowNull()]
        [object[]]$Filters
    )

    foreach ($filter in @($Filters)) {
        if ($null -eq $filter) {
            continue
        }

        $field = [string](Get-AnalysisFieldValue -Record $filter -Field 'field')
        $operator = [string](Get-AnalysisFieldValue -Record $filter -Field 'operator')
        $value = Get-AnalysisFieldValue -Record $filter -Field 'value'

        if ([string]::IsNullOrWhiteSpace($field) -or [string]::IsNullOrWhiteSpace($operator)) {
            throw 'Each filter requires field and operator.'
        }

        $recordValue = Get-AnalysisFieldValue -Record $Record -Field $field
        if (-not (Test-AnalysisCondition -LeftValue $recordValue -Operator $operator -RightValue $value)) {
            return $false
        }
    }

    return $true
}

function ConvertTo-GroupIdentity {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Record,

        [string[]]$Fields
    )

    $identity = [ordered]@{}
    if ($null -eq $Fields -or $Fields.Count -eq 0) {
        $identity['All'] = 'All'
        return [pscustomobject]$identity
    }

    foreach ($field in $Fields) {
        $identity[$field] = [string](Get-AnalysisFieldValue -Record $Record -Field $field)
    }

    return [pscustomobject]$identity
}

function ConvertTo-GroupIdentityKey {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$Identity
    )

    $parts = [System.Collections.Generic.List[string]]::new()
    foreach ($property in $Identity.PSObject.Properties) {
        $parts.Add("$($property.Name)=$([string]$property.Value)") | Out-Null
    }

    return [string]::Join('||', $parts)
}

function Get-NumericAnalysisValues {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$Records,

        [Parameter(Mandatory = $true)]
        [string]$Field
    )

    $values = [System.Collections.Generic.List[double]]::new()
    foreach ($record in @($Records)) {
        $rawValue = Get-AnalysisFieldValue -Record $record -Field $Field
        $scalar = ConvertTo-AnalysisScalar -Value $rawValue
        if ($scalar -is [datetime]) {
            continue
        }

        $numericValue = 0.0
        if ([double]::TryParse([string]$scalar, [ref]$numericValue)) {
            $values.Add($numericValue) | Out-Null
        }
    }

    return @($values)
}

function Get-AnalysisPercentile {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [double[]]$Values,

        [Parameter(Mandatory = $true)]
        [ValidateRange(0, 100)]
        [double]$Percentile
    )

    if ($Values.Count -eq 0) {
        return $null
    }

    $sorted = @($Values | Sort-Object)
    $rank = [Math]::Ceiling(($Percentile / 100.0) * $sorted.Count)
    if ($rank -lt 1) {
        $rank = 1
    }

    if ($rank -gt $sorted.Count) {
        $rank = $sorted.Count
    }

    return [Math]::Round($sorted[$rank - 1], 3)
}

function Measure-AnalysisMetric {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$Records,

        [Parameter(Mandatory = $true)]
        [psobject]$Metric
    )

    $name = [string](Get-AnalysisFieldValue -Record $Metric -Field 'name')
    $aggregation = [string](Get-AnalysisFieldValue -Record $Metric -Field 'aggregation')
    $field = [string](Get-AnalysisFieldValue -Record $Metric -Field 'field')
    if ([string]::IsNullOrWhiteSpace($name)) {
        throw 'Each metric requires a name.'
    }

    if ([string]::IsNullOrWhiteSpace($aggregation)) {
        throw "Metric '$($name)' is missing aggregation."
    }

    $normalizedAggregation = $aggregation.ToLowerInvariant()
    switch ($normalizedAggregation) {
        'count' {
            return [pscustomobject]@{ Name = $name; Value = $Records.Count }
        }
        'countdistinct' {
            if ([string]::IsNullOrWhiteSpace($field)) {
                throw "Metric '$($name)' requires field for countDistinct."
            }

            $distinctValues = @($Records | ForEach-Object { Get-AnalysisFieldValue -Record $_ -Field $field } | Where-Object { $null -ne $_ -and [string]::IsNullOrWhiteSpace([string]$_) -eq $false } | Select-Object -Unique)
            return [pscustomobject]@{ Name = $name; Value = $distinctValues.Count }
        }
        'sum' {
            $values = @(Get-NumericAnalysisValues -Records $Records -Field $field)
            return [pscustomobject]@{ Name = $name; Value = [Math]::Round((($values | Measure-Object -Sum).Sum), 3) }
        }
        'avg' {
            $values = @(Get-NumericAnalysisValues -Records $Records -Field $field)
            $value = $null
            if ($values.Count -gt 0) {
                $value = [Math]::Round((($values | Measure-Object -Average).Average), 3)
            }

            return [pscustomobject]@{ Name = $name; Value = $value }
        }
        'min' {
            $values = @(Get-NumericAnalysisValues -Records $Records -Field $field)
            return [pscustomobject]@{ Name = $name; Value = if ($values.Count -gt 0) { ($values | Measure-Object -Minimum).Minimum } else { $null } }
        }
        'max' {
            $values = @(Get-NumericAnalysisValues -Records $Records -Field $field)
            return [pscustomobject]@{ Name = $name; Value = if ($values.Count -gt 0) { ($values | Measure-Object -Maximum).Maximum } else { $null } }
        }
        'p50' {
            $values = @(Get-NumericAnalysisValues -Records $Records -Field $field)
            return [pscustomobject]@{ Name = $name; Value = Get-AnalysisPercentile -Values $values -Percentile 50 }
        }
        'p95' {
            $values = @(Get-NumericAnalysisValues -Records $Records -Field $field)
            return [pscustomobject]@{ Name = $name; Value = Get-AnalysisPercentile -Values $values -Percentile 95 }
        }
        'ratetrue' {
            if ([string]::IsNullOrWhiteSpace($field)) {
                throw "Metric '$($name)' requires field for rateTrue."
            }

            if ($Records.Count -eq 0) {
                return [pscustomobject]@{ Name = $name; Value = $null }
            }

            $trueCount = 0
            foreach ($record in @($Records)) {
                $value = Get-AnalysisFieldValue -Record $record -Field $field
                if ($value -eq $true -or [string]$value -eq 'true' -or [string]$value -eq '1') {
                    $trueCount++
                }
            }

            return [pscustomobject]@{ Name = $name; Value = [Math]::Round(($trueCount / $Records.Count), 4) }
        }
        default {
            throw "Unsupported aggregation '$($aggregation)' for metric '$($name)'."
        }
    }
}
