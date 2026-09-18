#Requires -Version 5.1
<#
.SYNOPSIS
    GenesysConvAnalyzer.ps1 - Conversation Detail Analysis Tool for Genesys Cloud

.DESCRIPTION
    Purpose-built WPF application for deep conversation analytics using the async
    job pattern (/api/v2/analytics/conversations/details/jobs). Designed for orgs
    with high conversation volume where engineers need to:

    - Set up targeted async queries across any filter dimension
    - Collect and page through large result sets with live progress
    - Inspect conversations with full participant/segment/attribute detail
    - Export flattened rows (CSV) or full conversation objects (JSONL) for downstream tools

    The workflow is:
        Query Builder > Submit Job > Poll Status > Collect Results > Analyze / Export

    Region validation
    -----------------
    The Region field is validated before authentication and before every preview/submit.
    A valid region must be a non-empty hostname containing at least one dot and only
    hostname-safe characters (letters, digits, hyphens, dots).  Examples of accepted
    values: 'usw2.pure.cloud', 'mypurecloud.com', 'mypurecloud.com.au'.  Blank, whitespace,
    or malformed values produce an error message before any network call is made.

    Build-QueryRequestPreview also writes the validated region back into $script:baseUri
    so preview and submit always target the same endpoint.

    Date/time defaults and preset behavior
    ---------------------------------------
    Start time defaults to 00:00:00 when left blank; end time defaults to 23:59:59.
    Times are parsed in HH:mm or HH:mm:ss format (local time); an out-of-order or
    unparseable value raises an error before any request is built.
    Quick-preset buttons (Today, Yesterday, Last 7 Days, etc.) always reset both time
    boxes to 00:00:00 / 23:59:59 unconditionally, producing a deterministic full-day
    interval regardless of any value previously typed.

    Request preview and submit
    --------------------------
    The Preview button and the Submit button both call Build-QueryRequestPreview, which
    in turn calls Build-QueryBody.  Both operations therefore use the identical endpoint
    URI and request body.  The preview panel is updated by Submit before the API call
    so the logged body exactly matches what is sent.

    API retry policy
    ----------------
    Direct REST calls made through Invoke-GcApiRequest retry transient 429 and 5xx
    failures for GET/HEAD/OPTIONS/DELETE using exponential backoff. Retry-After is
    honored when present and each retry attempt is written to the job log. POST job
    submission is not retried automatically to avoid accidental duplicate async jobs.
    When retries are exhausted, the UI reports that separately from an immediate fatal
    request failure. Cancel/delete failures are logged as warnings instead of being
    ignored.

    Analysis and reporting
    ----------------------
    src/analysis/ConversationAnalysis.ps1 builds one cached profile per conversation
    (queue path, agent path, wrap-up, disconnect, flow, ~30 metrics, MOS, evaluations,
    surveys) and derives from it:
      - Results grid / CSV: ~90 standard columns (Column Selector picks what the grid
        shows; CSV always exports all of them plus selected attribute columns).
      - Detail panel: overview, per-session participants, chronological segment
        timeline, raw metrics, flows, and all participant attributes.
      - Report tab: KPIs, rule-based observations, and breakdowns by queue, agent,
        hour, date, media/direction, wrap-up, disconnect, IVR flow, voice quality,
        division, plus longest / lowest-MOS outliers. Export Report writes a
        self-contained HTML file and a JSON copy of the same numbers.
    After collection (when authenticated) queue, wrap-up code, division, skill, and
    language IDs are resolved to names; failures fall back to IDs and are logged.

.NOTES
    Requirements : Windows, PowerShell 5.1+, Genesys Cloud OAuth client credentials
    Reuses       : Auth + config patterns from GenesysCore-GUI.ps1

.EXAMPLE
    .\GenesysConvAnalyzer.ps1

.EXAMPLE
    .\GenesysConvAnalyzer.ps1 -DefaultRegion 'usw2.pure.cloud'
#>

[CmdletBinding()]
param(
    [string]$DefaultRegion = 'usw2.pure.cloud',
    [string]$ConfigPath
)

if (-not $IsWindows -and $PSVersionTable.PSVersion.Major -ge 6) {
    throw "This tool requires Windows (WPF is Windows-only)."
}

Add-Type -AssemblyName PresentationFramework
Add-Type -AssemblyName PresentationCore
Add-Type -AssemblyName WindowsBase
Add-Type -AssemblyName System.Windows.Forms

. (Join-Path $PSScriptRoot 'src/ui/UiApiRetry.ps1')
. (Join-Path $PSScriptRoot 'src/analysis/ConversationAnalysis.ps1')

# -----------------------------------------------------------------------------
# Config / Auth  (shared pattern with GenesysCore-GUI.ps1)
# -----------------------------------------------------------------------------

function Resolve-UIConfigPath {
    param([string]$ExplicitPath)
    $candidates = [System.Collections.Generic.List[string]]::new()
    if (-not [string]::IsNullOrWhiteSpace($ExplicitPath)) { $candidates.Add($ExplicitPath) | Out-Null }
    $candidates.Add('GenesysCore-GUI.config.json') | Out-Null
    $candidates.Add('genesys.env.json') | Out-Null
    foreach ($c in @($candidates)) {
        $r = if ([System.IO.Path]::IsPathRooted($c)) { $c } else { Join-Path $PSScriptRoot $c }
        if (Test-Path $r -PathType Leaf) { return (Resolve-Path $r).Path }
    }
    return (Join-Path $PSScriptRoot 'GenesysCore-GUI.config.json')
}

$script:configPath = Resolve-UIConfigPath -ExplicitPath $ConfigPath

function Read-GenesysEnvConfig {
    if (-not (Test-Path $script:configPath)) { return $null }
    try { return Get-Content $script:configPath -Raw -Encoding utf8 | ConvertFrom-Json } catch { return $null }
}

function Save-GenesysEnvConfig {
    param([string]$Region, [string]$ClientId)
    $cfg = [ordered]@{ region = $Region.Trim() }
    if (-not [string]::IsNullOrWhiteSpace($ClientId)) { $cfg['clientId'] = $ClientId.Trim() }
    try { ($cfg | ConvertTo-Json -Depth 5) | Set-Content $script:configPath -Encoding utf8 } catch {}
}

function Get-ConfigString {
    param([object]$ConfigObject, [string[]]$PropertyNames)
    if ($null -eq $ConfigObject) { return $null }
    foreach ($n in $PropertyNames) {
        if ($ConfigObject.PSObject.Properties.Name -contains $n) {
            $v = [string]$ConfigObject.$n
            if (-not [string]::IsNullOrWhiteSpace($v)) { return $v }
        }
    }
    return $null
}

# -----------------------------------------------------------------------------
# Script state
# -----------------------------------------------------------------------------

$script:accessToken = $null
$script:headers = @{}
$script:baseUri = "https://api.$DefaultRegion"
$script:gcApiRetrySettings = Resolve-UiApiRetrySettings
$script:gcApiRequestInvoker = {
    param([hashtable]$InvokeParams)
    Invoke-RestMethod @InvokeParams
}
$script:gcApiRetryLogAction = $null
$script:currentJobId = $null
$script:pollTimer = $null
$script:pollCount = 0
$script:jobSubmitTime = $null
$script:allConversations = [System.Collections.Generic.List[object]]::new()
$script:conversationIndex = @{}
$script:selectedAttrCols = [System.Collections.Generic.List[string]]::new()
$script:visibleStdCols = [System.Collections.Generic.List[string]]::new([string[]]@(Get-DefaultGridColumnNames))
$script:maxGridRows = 20000  # Display cap to keep the WPF grid responsive on large jobs
$script:exportRedactionMode = $true  # Safe-by-default export behavior

# -- Analysis caches (see src/analysis/ConversationAnalysis.ps1) ---------------
$script:profileCache = @{}           # conversationId -> profile (IDs only; valid across lookup refreshes)
$script:gridRowCache = $null         # flat rows for the grid, rebuilt when attributes or names change
$script:gridRowCacheKey = ''
$script:currentReport = $null        # collection report, rebuilt when data or names change
$script:lookups = New-ConversationLookupTable   # id -> name for queues, wrap-up codes, divisions, skills, languages
$script:lookupsLoaded = $false
$script:lookupVersion = 0            # bumped on every lookup refresh to invalidate cached rows
$script:maxLookupPages = 100         # 100 pages x 100 entities per reference type
$script:dataSource = ''              # shown in the report header ("Analytics job <id>" or "File <name>")
$script:dataQueryInterval = ''
$script:currentJobInterval = ''

# -- Polling and paging guardrails ---------------------------------------------
$script:maxPollCount = 600    # Stop polling after this many attempts (~30 min at 3s interval)
$script:maxPollTimeoutMinutes = 30     # Hard timeout regardless of poll count
$script:maxConsecutivePollErrors = 5      # Stop after this many consecutive poll failures
$script:consecutivePollErrors = 0      # Running count, reset on each successful poll

$script:maxPageCount = 500    # Stop paging after this many pages (500k conversations at 1000/page)
$script:seenCursors = $null  # HashSet populated during collection to detect cursor loops
$script:convFilterRows = [System.Collections.Generic.List[pscustomobject]]::new()
$script:segFilterRows = [System.Collections.Generic.List[pscustomobject]]::new()

# -----------------------------------------------------------------------------
# API helpers
# -----------------------------------------------------------------------------

function Invoke-GcApiRequest {
    param(
        [string]$Method,
        [string]$Path,
        [string]$Body,
        [hashtable]$QueryParams,
        [scriptblock]$RequestInvoker
    )

    $effectiveInvoker = if ($PSBoundParameters.ContainsKey('RequestInvoker') -and $null -ne $RequestInvoker) {
        $RequestInvoker
    }
    else {
        $script:gcApiRequestInvoker
    }

    return Invoke-UiApiRequest -BaseUri $script:baseUri -Method $Method -Path $Path -Headers $script:headers -Body $Body -QueryParams $QueryParams -RetrySettings $script:gcApiRetrySettings -RequestInvoker $effectiveInvoker -LogAction $script:gcApiRetryLogAction
}

function Submit-AnalyticsJob { param([string]$JsonBody) Invoke-GcApiRequest -Method 'POST' -Path '/api/v2/analytics/conversations/details/jobs' -Body $JsonBody }
function Get-AnalyticsJobStatus { param([string]$JobId)    Invoke-GcApiRequest -Method 'GET' -Path "/api/v2/analytics/conversations/details/jobs/$JobId" }
function Remove-AnalyticsJob {
    param([string]$JobId)

    try {
        Invoke-GcApiRequest -Method 'DELETE' -Path "/api/v2/analytics/conversations/details/jobs/$JobId" | Out-Null
        return $true
    }
    catch {
        $failureText = Format-UiApiFailure -Exception $_.Exception
        Append-JobLog "Warning: cancel/delete request failed for job $JobId. $failureText"
        return $false
    }
}

function Get-AnalyticsJobResults {
    param([string]$JobId, [int]$PageSize = 1000, [string]$Cursor)
    $q = @{ pageSize = [string]$PageSize }
    if (-not [string]::IsNullOrWhiteSpace($Cursor)) { $q['cursor'] = $Cursor }
    Invoke-GcApiRequest -Method 'GET' -Path "/api/v2/analytics/conversations/details/jobs/$JobId/results" -QueryParams $q
}

# -----------------------------------------------------------------------------
# Data transformation
# -----------------------------------------------------------------------------

# Field extraction, flat rows, and the collection report live in
# src/analysis/ConversationAnalysis.ps1. These wrappers add caching on top.

function Get-CachedConversationProfile {
    param([object]$Conv)
    $conversationId = [string]$Conv.conversationId
    if (-not [string]::IsNullOrWhiteSpace($conversationId) -and $script:profileCache.ContainsKey($conversationId)) {
        return $script:profileCache[$conversationId]
    }
    $conversationProfile = Get-ConversationProfile -Conversation $Conv
    if (-not [string]::IsNullOrWhiteSpace($conversationId)) { $script:profileCache[$conversationId] = $conversationProfile }
    return $conversationProfile
}

function Reset-AnalysisCaches {
    # Profiles hold IDs only, so a lookup refresh keeps them; rows and report embed names.
    param([switch]$IncludeProfiles)
    if ($IncludeProfiles) { $script:profileCache = @{} }
    $script:gridRowCache = $null
    $script:gridRowCacheKey = ''
    $script:currentReport = $null
}

function Clear-ConversationStore {
    $script:allConversations.Clear()
    $script:conversationIndex = @{}
    Reset-AnalysisCaches -IncludeProfiles
}
function Add-ConversationRecord {
    param([object]$Conversation)
    if ($null -eq $Conversation) { return }
    $script:allConversations.Add($Conversation) | Out-Null
    $conversationId = [string]$Conversation.conversationId
    if (-not [string]::IsNullOrWhiteSpace($conversationId)) {
        $script:conversationIndex[$conversationId] = $Conversation
    }
}

function Add-Conversations {
    param([object[]]$Conversations)
    foreach ($conversation in @($Conversations)) {
        Add-ConversationRecord -Conversation $conversation
    }
}

function Get-ConversationById {
    param([string]$ConversationId)
    if ([string]::IsNullOrWhiteSpace($ConversationId)) { return $null }
    if ($script:conversationIndex.ContainsKey($ConversationId)) {
        return $script:conversationIndex[$ConversationId]
    }
    return $null
}

function Test-SensitiveKey {
    param([string]$Key)
    if ([string]::IsNullOrWhiteSpace($Key)) { return $false }
    # (?<!voic)email keeps Voicemail / tVoicemailSec readable.
    return $Key -match '(?i)(password|secret|token|ani|dnis|phone|(?<!voic)email|address|ssn|externalContactId|participantName|firstName|lastName|fullName|displayName|customerName|wrapUpNote|remote|callbackNumbers|callbackUserName)'
}

function Protect-ScalarValue {
    param([AllowNull()][object]$Value, [string]$Key)
    if ($null -eq $Value) { return $null }
    if (-not $script:exportRedactionMode) { return $Value }
    if (-not (Test-SensitiveKey -Key $Key)) { return $Value }

    $text = [string]$Value
    if ([string]::IsNullOrEmpty($text)) { return $text }
    if ($text.Length -le 4) { return ('*' * $text.Length) }
    return ('*' * ($text.Length - 4)) + $text.Substring($text.Length - 4)
}

function Protect-ObjectForExport {
    param(
        [AllowNull()][object]$InputObject,
        [string]$CurrentKey = ''
    )

    if ($null -eq $InputObject) { return $null }

    if ($InputObject -is [string] -or $InputObject -is [ValueType]) {
        return (Protect-ScalarValue -Value $InputObject -Key $CurrentKey)
    }

    if ($InputObject -is [System.Collections.IDictionary]) {
        $clone = [ordered]@{}
        foreach ($key in $InputObject.Keys) {
            $clone[[string]$key] = Protect-ObjectForExport -InputObject $InputObject[$key] -CurrentKey ([string]$key)
        }
        return [pscustomobject]$clone
    }

    if ($InputObject -is [System.Collections.IEnumerable] -and $InputObject -isnot [string]) {
        $items = New-Object System.Collections.Generic.List[object]
        foreach ($item in $InputObject) {
            $items.Add((Protect-ObjectForExport -InputObject $item -CurrentKey $CurrentKey)) | Out-Null
        }
        return @($items)
    }

    $properties = $InputObject.PSObject.Properties
    if ($null -eq $properties -or $properties.Count -eq 0) {
        return (Protect-ScalarValue -Value $InputObject -Key $CurrentKey)
    }

    $clone = [ordered]@{}
    foreach ($property in $properties) {
        if ($property.IsGettable) {
            $clone[$property.Name] = Protect-ObjectForExport -InputObject $property.Value -CurrentKey $property.Name
        }
    }
    return [pscustomobject]$clone
}

function Get-ExportConversation {
    param([object]$Conversation)
    if (-not $script:exportRedactionMode) { return $Conversation }
    return Protect-ObjectForExport -InputObject $Conversation
}

function Read-ConversationsFromFile {
    # Streams conversations from a JSONL, JSON array, or single-object JSON file.
    # Returns a result object with .Conversations (list), .LineCount, .ErrorCount,
    # and .Errors (list of per-line error descriptions).
    # Accepts an optional -OnProgress scriptblock that receives a hashtable with
    # Count and LineNumber keys, called every 500 records for UI feedback.
    param(
        [string]$Path,
        [scriptblock]$OnProgress = $null
    )

    $conversations = [System.Collections.Generic.List[object]]::new()
    $errors = [System.Collections.Generic.List[string]]::new()
    $lineNumber = 0
    $progressInterval = 500

    function Report-Progress {
        if ($null -ne $OnProgress) {
            try { & $OnProgress @{ Count = $conversations.Count; LineNumber = $lineNumber } } catch {}
        }
    }

    function Add-ParsedObject {
        param([object]$Obj)
        if ($null -eq $Obj) { return }
        if ($Obj -is [System.Collections.IEnumerable] -and $Obj -isnot [string]) {
            foreach ($entry in @($Obj)) {
                $conversations.Add($entry) | Out-Null
            }
            return
        }
        if ($Obj.PSObject.Properties.Name -contains 'conversations') {
            foreach ($entry in @($Obj.conversations)) {
                $conversations.Add($entry) | Out-Null
            }
            return
        }
        $conversations.Add($Obj) | Out-Null
    }

    # Peek at first non-whitespace character to detect format
    $firstChar = $null
    $reader = $null
    try {
        $reader = [System.IO.StreamReader]::new($Path, [System.Text.Encoding]::UTF8, $true)
        while (-not $reader.EndOfStream) {
            $code = $reader.Read()
            if ($code -lt 0) { break }
            $ch = [char]$code
            if (-not [char]::IsWhiteSpace($ch)) { $firstChar = $ch; break }
        }
    }
    finally {
        if ($null -ne $reader) { $reader.Dispose() }
    }

    if ($null -eq $firstChar) {
        # Empty file
        return [pscustomobject]@{ Conversations = @(); LineCount = 0; ErrorCount = 0; Errors = @() }
    }

    # JSON array: must read whole file because ConvertFrom-Json needs the complete array.
    # This is the only non-streaming path; kept for backwards compatibility with JSON exports.
    if ($firstChar -eq '[') {
        try {
            $payload = Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json
            Add-ParsedObject -Obj $payload
        }
        catch {
            $errors.Add("JSON array parse failed: $($_.Exception.Message)") | Out-Null
        }
        return [pscustomobject]@{
            Conversations = @($conversations)
            LineCount     = 1
            ErrorCount    = $errors.Count
            Errors        = @($errors)
        }
    }

    # Line-by-line streaming (JSONL or single-object JSON that failed whole-file parse)
    $stream = $null
    try {
        $stream = [System.IO.StreamReader]::new($Path, [System.Text.Encoding]::UTF8, $true)
        while (-not $stream.EndOfStream) {
            $line = $stream.ReadLine()
            $lineNumber++
            if ([string]::IsNullOrWhiteSpace($line)) { continue }

            try {
                $obj = $line.Trim() | ConvertFrom-Json
                Add-ParsedObject -Obj $obj
            }
            catch {
                $errors.Add("Line $lineNumber : $($_.Exception.Message)") | Out-Null
            }

            if ($conversations.Count % $progressInterval -eq 0) { Report-Progress }
        }
    }
    finally {
        if ($null -ne $stream) { $stream.Dispose() }
    }

    Report-Progress

    return [pscustomobject]@{
        Conversations = @($conversations)
        LineCount     = $lineNumber
        ErrorCount    = $errors.Count
        Errors        = @($errors)
    }
}

# -----------------------------------------------------------------------------
# XAML
# -----------------------------------------------------------------------------

[xml]$xaml = @'
<Window xmlns="http://schemas.microsoft.com/winfx/2006/xaml/presentation"
        xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml"
        Title="Genesys Conversation Analyzer" Height="820" Width="1100"
        WindowStartupLocation="CenterScreen" FontSize="12">
  <Window.Resources>
    <Style TargetType="Button">
      <Setter Property="Padding"  Value="8,3"/>
      <Setter Property="Margin"   Value="2"/>
    </Style>
    <Style TargetType="GroupBox">
      <Setter Property="Padding" Value="6"/>
      <Setter Property="Margin"  Value="0,0,0,8"/>
    </Style>
    <Style TargetType="Label">
      <Setter Property="VerticalAlignment" Value="Center"/>
      <Setter Property="Padding" Value="2"/>
    </Style>
    <Style TargetType="ComboBox">
      <Setter Property="Margin" Value="2"/>
      <Setter Property="VerticalAlignment" Value="Center"/>
    </Style>
    <Style TargetType="TextBox">
      <Setter Property="Margin" Value="2"/>
      <Setter Property="VerticalAlignment" Value="Center"/>
    </Style>
  </Window.Resources>

  <Grid Margin="8">
    <Grid.RowDefinitions>
      <RowDefinition Height="Auto"/>
      <RowDefinition Height="*"/>
      <RowDefinition Height="Auto"/>
    </Grid.RowDefinitions>

    <!-- Auth -->
    <GroupBox Grid.Row="0" Header="Authentication">
      <Grid>
        <Grid.ColumnDefinitions>
          <ColumnDefinition Width="70"/>
          <ColumnDefinition Width="180"/>
          <ColumnDefinition Width="70"/>
          <ColumnDefinition Width="200"/>
          <ColumnDefinition Width="70"/>
          <ColumnDefinition Width="180"/>
          <ColumnDefinition Width="110"/>
          <ColumnDefinition Width="*"/>
        </Grid.ColumnDefinitions>
        <Label   Grid.Column="0" Content="Region:"/>
        <ComboBox Grid.Column="1" Name="RegionComboBox" IsEditable="True">
          <ComboBoxItem Content="mypurecloud.com" IsSelected="False"/>
            <ComboBoxItem Content="usw2.pure.cloud" IsSelected="True"/>
        </ComboBox>
        <Label   Grid.Column="2" Content="Client ID:"/>
        <TextBox Grid.Column="3" Name="ClientIdBox"/>
        <Label   Grid.Column="4" Content="Secret:"/>
        <PasswordBox Grid.Column="5" Name="ClientSecretBox" VerticalAlignment="Center" Margin="2"/>
        <Button  Grid.Column="6" Name="AuthButton" Content="Authenticate" Margin="6,2,2,2"/>
        <TextBlock Grid.Column="7" Name="AuthStatusLabel" VerticalAlignment="Center" Margin="6,0,0,0" FontWeight="Bold"/>
      </Grid>
    </GroupBox>

    <!-- Main Tabs -->
    <TabControl Grid.Row="1" Name="MainTabControl">

      <!-- == Tab 1: Query Builder == -->
      <TabItem Header=" Query Builder ">
        <ScrollViewer VerticalScrollBarVisibility="Auto">
          <StackPanel Margin="6">

            <!-- Date range -->
            <GroupBox Header="Interval">
              <Grid>
                <Grid.ColumnDefinitions>
                  <ColumnDefinition Width="Auto"/>
                  <ColumnDefinition Width="Auto"/>
                  <ColumnDefinition Width="Auto"/>
                  <ColumnDefinition Width="Auto"/>
                  <ColumnDefinition Width="Auto"/>
                  <ColumnDefinition Width="Auto"/>
                  <ColumnDefinition Width="Auto"/>
                  <ColumnDefinition Width="Auto"/>
                  <ColumnDefinition Width="Auto"/>
                  <ColumnDefinition Width="Auto"/>
                  <ColumnDefinition Width="Auto"/>
                  <ColumnDefinition Width="Auto"/>
                  <ColumnDefinition Width="*"/>
                </Grid.ColumnDefinitions>
                <Grid.RowDefinitions>
                  <RowDefinition Height="Auto"/>
                  <RowDefinition Height="Auto"/>
                </Grid.RowDefinitions>
                <!-- Quick presets row -->
                <Label   Grid.Row="0" Grid.Column="0" Content="Preset:"/>
                <Button  Grid.Row="0" Grid.Column="1" Name="PresetToday"     Content="Today"/>
                <Button  Grid.Row="0" Grid.Column="2" Name="PresetYesterday" Content="Yesterday"/>
                <Button  Grid.Row="0" Grid.Column="3" Name="PresetLast7"     Content="Last 7 Days"/>
                <Button  Grid.Row="0" Grid.Column="4" Name="PresetLast30"    Content="Last 30 Days"/>
                <Button  Grid.Row="0" Grid.Column="5" Name="PresetThisMonth" Content="This Month"/>
                <Button  Grid.Row="0" Grid.Column="6" Name="PresetLastMonth" Content="Last Month"/>
                <!-- Date picker row -->
                <Label       Grid.Row="1" Grid.Column="0" Content="From:"/>
                <DatePicker  Grid.Row="1" Grid.Column="1" Name="StartDatePicker" Width="130" Margin="2"/>
                <Label       Grid.Row="1" Grid.Column="2" Content="Time:"/>
                <TextBox     Grid.Row="1" Grid.Column="3" Name="StartTimeTextBox" Width="90" Margin="2" ToolTip="Local time. Formats: HH:mm or HH:mm:ss"/>
                <Label       Grid.Row="1" Grid.Column="4" Content="To:"/>
                <DatePicker  Grid.Row="1" Grid.Column="5" Name="EndDatePicker"   Width="130" Margin="2"/>
                <Label       Grid.Row="1" Grid.Column="6" Content="Time:"/>
                <TextBox     Grid.Row="1" Grid.Column="7" Name="EndTimeTextBox" Width="90" Margin="2" ToolTip="Local time. Formats: HH:mm or HH:mm:ss"/>
                <TextBlock   Grid.Row="1" Grid.Column="8" Grid.ColumnSpan="4" Margin="8,0,0,0" VerticalAlignment="Center" Foreground="Gray"
                             Text="Blank times default to 00:00:00 for start and 23:59:59 for end." TextWrapping="Wrap"/>
              </Grid>
            </GroupBox>

            <!-- Quick filter row -->
            <GroupBox Header="Quick Filters">
              <Grid>
                <Grid.ColumnDefinitions>
                  <ColumnDefinition Width="80"/>
                  <ColumnDefinition Width="140"/>
                  <ColumnDefinition Width="80"/>
                  <ColumnDefinition Width="140"/>
                  <ColumnDefinition Width="80"/>
                  <ColumnDefinition Width="140"/>
                  <ColumnDefinition Width="80"/>
                  <ColumnDefinition Width="140"/>
                  <ColumnDefinition Width="*"/>
                </Grid.ColumnDefinitions>
                <Label    Grid.Column="0" Content="Direction:"/>
                <ComboBox Grid.Column="1" Name="DirectionCombo" SelectedIndex="0">
                  <ComboBoxItem Content="(any)"/>
                  <ComboBoxItem Content="inbound"/>
                  <ComboBoxItem Content="outbound"/>
                </ComboBox>
                <Label    Grid.Column="2" Content="Media Type:"/>
                <ComboBox Grid.Column="3" Name="MediaTypeCombo" SelectedIndex="0">
                  <ComboBoxItem Content="(any)"/>
                  <ComboBoxItem Content="voice"/>
                  <ComboBoxItem Content="chat"/>
                  <ComboBoxItem Content="email"/>
                  <ComboBoxItem Content="callback"/>
                  <ComboBoxItem Content="message"/>
                  <ComboBoxItem Content="cobrowse"/>
                  <ComboBoxItem Content="video"/>
                </ComboBox>
                <Label    Grid.Column="4" Content="Order By:"/>
                <ComboBox Grid.Column="5" Name="OrderByCombo" SelectedIndex="0">
                  <ComboBoxItem Content="conversationStart"/>
                  <ComboBoxItem Content="conversationEnd"/>
                </ComboBox>
                <Label    Grid.Column="6" Content="Order:"/>
                <ComboBox Grid.Column="7" Name="OrderCombo" SelectedIndex="0">
                  <ComboBoxItem Content="asc"/>
                  <ComboBoxItem Content="desc"/>
                </ComboBox>
              </Grid>
            </GroupBox>

            <!-- Conversation filters -->
            <GroupBox Header="Conversation Filters  (one predicate per row - each becomes its own filter group)">
              <StackPanel>
                <StackPanel Name="ConvFilterPanel"/>
                <Button Name="AddConvFilterBtn" Content="+ Add Conversation Filter"
                        HorizontalAlignment="Left" Width="200" Margin="0,4,0,0"/>
              </StackPanel>
            </GroupBox>

            <!-- Segment filters -->
            <GroupBox Header="Segment Filters">
              <StackPanel>
                <StackPanel Name="SegFilterPanel"/>
                <Button Name="AddSegFilterBtn" Content="+ Add Segment Filter"
                        HorizontalAlignment="Left" Width="200" Margin="0,4,0,0"/>
              </StackPanel>
            </GroupBox>

            <!-- JSON preview -->
            <GroupBox Header="Request Preview">
              <StackPanel>
                <TextBlock Text="Endpoint" FontWeight="Bold" Margin="0,0,0,2"/>
                <TextBox Name="RequestEndpointBox" IsReadOnly="True" MaxHeight="48"
                         TextWrapping="Wrap" VerticalScrollBarVisibility="Auto"
                         FontFamily="Consolas" FontSize="11" Background="#F8F8F8"/>
                <TextBlock Text="Request Body" FontWeight="Bold" Margin="0,6,0,2"/>
                <TextBox Name="QueryPreviewBox" IsReadOnly="True" MaxHeight="180"
                         TextWrapping="Wrap" VerticalScrollBarVisibility="Auto"
                         FontFamily="Consolas" FontSize="11" Background="#F8F8F8"/>
                <WrapPanel Margin="0,4,0,0">
                  <Button Name="PreviewBtn"  Content="Preview Request"/>
                  <Button Name="ClearFiltersBtn" Content="Clear All Filters"/>
                </WrapPanel>
              </StackPanel>
            </GroupBox>

            <!-- Submit -->
            <Button Name="SubmitJobBtn" Content="Submit Async Job"
                    FontSize="13" FontWeight="Bold" Height="38"
                    Background="#005A9C" Foreground="White"
                    HorizontalContentAlignment="Center"/>

          </StackPanel>
        </ScrollViewer>
      </TabItem>

      <!-- == Tab 2: Job Monitor == -->
      <TabItem Header=" Job Monitor " Name="JobMonitorTab">
        <Grid Margin="6">
          <Grid.RowDefinitions>
            <RowDefinition Height="Auto"/>
            <RowDefinition Height="*"/>
            <RowDefinition Height="Auto"/>
          </Grid.RowDefinitions>

          <!-- Job status panel -->
          <GroupBox Grid.Row="0" Header="Current Job">
            <Grid>
              <Grid.ColumnDefinitions>
                <ColumnDefinition Width="70"/>
                <ColumnDefinition Width="280"/>
                <ColumnDefinition Width="70"/>
                <ColumnDefinition Width="100"/>
                <ColumnDefinition Width="60"/>
                <ColumnDefinition Width="50"/>
                <ColumnDefinition Width="70"/>
                <ColumnDefinition Width="80"/>
                <ColumnDefinition Width="*"/>
              </Grid.ColumnDefinitions>
              <Label   Grid.Column="0" Content="Job ID:"/>
              <TextBox Grid.Column="1" Name="JobIdBox" IsReadOnly="True" Background="#F0F0F0" FontFamily="Consolas"/>
              <Label   Grid.Column="2" Content="State:"/>
              <TextBlock Grid.Column="3" Name="JobStateLabel" VerticalAlignment="Center" FontWeight="Bold" Margin="4,0,0,0"/>
              <Label   Grid.Column="4" Content="Polls:"/>
              <TextBlock Grid.Column="5" Name="JobPollLabel" VerticalAlignment="Center" Margin="4,0,0,0"/>
              <Label   Grid.Column="6" Content="Elapsed:"/>
              <TextBlock Grid.Column="7" Name="JobElapsedLabel" VerticalAlignment="Center" Margin="4,0,0,0"/>
              <Button  Grid.Column="8" Name="CancelJobBtn" Content="Cancel / Delete Job"
                       HorizontalAlignment="Left" Margin="10,2,2,2" Background="#C0392B" Foreground="White" IsEnabled="False"/>
            </Grid>
          </GroupBox>

          <!-- Activity log -->
          <GroupBox Grid.Row="1" Header="Activity Log">
            <TextBox Name="JobLogBox" IsReadOnly="True" TextWrapping="NoWrap"
                     VerticalScrollBarVisibility="Auto" HorizontalScrollBarVisibility="Auto"
                     FontFamily="Consolas" FontSize="11" Background="#1E1E1E" Foreground="#D4D4D4"/>
          </GroupBox>

          <!-- Collect bar -->
          <Border Grid.Row="2" Background="#E8F4E8" BorderBrush="#4CAF50" BorderThickness="1" Padding="8,6" Margin="0,4,0,0">
            <Grid>
              <Grid.ColumnDefinitions>
                <ColumnDefinition Width="*"/>
                <ColumnDefinition Width="Auto"/>
              </Grid.ColumnDefinitions>
              <TextBlock Grid.Column="0" Name="CollectStatusText"
                         Text="Submit a job above. When it reaches FULFILLED, click Collect to page through results."
                         VerticalAlignment="Center" TextWrapping="Wrap"/>
              <Button Grid.Column="1" Name="CollectResultsBtn"
                      Content="Collect All Results" FontWeight="Bold"
                      Background="#27AE60" Foreground="White"
                      IsEnabled="False" Width="180" Height="34"/>
            </Grid>
          </Border>
        </Grid>
      </TabItem>

      <!-- == Tab 3: Results == -->
      <TabItem Header=" Results " Name="ResultsTab">
        <Grid Margin="6">
          <Grid.RowDefinitions>
            <RowDefinition Height="Auto"/>
            <RowDefinition Height="Auto"/>
            <RowDefinition Height="*" MinHeight="120"/>
            <RowDefinition Height="Auto"/>
            <RowDefinition Height="280" MinHeight="120"/>
          </Grid.RowDefinitions>

          <!-- Summary -->
          <Border Grid.Row="0" Background="#EBF5FB" BorderBrush="#2980B9" BorderThickness="1" Padding="6,4" Margin="0,0,0,6">
            <TextBlock Name="SummaryText" FontWeight="Bold" TextWrapping="Wrap"
                       Text="No results loaded. Submit and collect a job first."/>
          </Border>

          <!-- Toolbar -->
          <WrapPanel Grid.Row="1" Margin="0,0,0,4">
            <Button Name="ColumnSelectorBtn" Content="Column Selector..."
                    ToolTip="Choose which of the ~90 standard columns the grid shows, and add participant attribute columns."/>
            <Button Name="ResolveNamesBtn"   Content="Resolve Names"
                    ToolTip="Look up queue, wrap-up code, division, skill, and language names (needs authentication)."/>
            <Button Name="ExportCsvBtn"      Content="Export CSV"
                    ToolTip="All standard columns plus selected attribute columns, for every loaded conversation."/>
            <Button Name="ExportJsonlBtn"    Content="Export JSONL (full)"/>
            <Button Name="LoadJsonlBtn"      Content="Load from JSONL..."/>
            <Button Name="ClearResultsBtn"   Content="Clear Results"/>
            <CheckBox Name="RedactExportsCheckBox" Content="Redact exports" IsChecked="True" VerticalAlignment="Center" Margin="8,2,2,2"/>
          </WrapPanel>

          <!-- Results DataGrid -->
          <DataGrid Grid.Row="2" Name="ResultsGrid"
                    IsReadOnly="True" AutoGenerateColumns="False"
                    SelectionMode="Single" CanUserSortColumns="True"
                    GridLinesVisibility="Horizontal" AlternatingRowBackground="#F9F9F9"
                    EnableRowVirtualization="True" VirtualizingStackPanel.IsVirtualizing="True"
                    VirtualizingStackPanel.VirtualizationMode="Recycling"/>

          <GridSplitter Grid.Row="3" Height="6" HorizontalAlignment="Stretch" ResizeDirection="Rows"
                        Background="#E0E0E0" ToolTip="Drag to resize the detail panel"/>

          <!-- Detail panel (grids get their columns from the row objects at runtime) -->
          <GroupBox Grid.Row="4" Header="Conversation Detail  (select a row above)">
            <TabControl Name="DetailTabControl">
              <TabItem Header="Overview">
                <ScrollViewer HorizontalScrollBarVisibility="Disabled" VerticalScrollBarVisibility="Auto">
                  <WrapPanel Name="OverviewPanel" Margin="4" Orientation="Horizontal"/>
                </ScrollViewer>
              </TabItem>
              <TabItem Header="Participants / Sessions">
                <DataGrid Name="ParticipantsGrid" IsReadOnly="True" AutoGenerateColumns="False"
                          GridLinesVisibility="Horizontal" AlternatingRowBackground="#FAFAFA"/>
              </TabItem>
              <TabItem Header="Segment Timeline">
                <DataGrid Name="SegmentsGrid" IsReadOnly="True" AutoGenerateColumns="False"
                          GridLinesVisibility="Horizontal" AlternatingRowBackground="#FAFAFA"/>
              </TabItem>
              <TabItem Header="Metrics">
                <DataGrid Name="MetricsGrid" IsReadOnly="True" AutoGenerateColumns="False"
                          GridLinesVisibility="Horizontal" AlternatingRowBackground="#FAFAFA"/>
              </TabItem>
              <TabItem Header="Flows">
                <DataGrid Name="FlowsGrid" IsReadOnly="True" AutoGenerateColumns="False"
                          GridLinesVisibility="Horizontal" AlternatingRowBackground="#FAFAFA"/>
              </TabItem>
              <TabItem Header="Attributes">
                <DataGrid Name="AttributesGrid" IsReadOnly="True" AutoGenerateColumns="False"
                          GridLinesVisibility="Horizontal" AlternatingRowBackground="#FAFAFA"/>
              </TabItem>
              <TabItem Header="Raw JSON">
                <TextBox Name="RawJsonBox" IsReadOnly="True" TextWrapping="NoWrap"
                         HorizontalScrollBarVisibility="Auto" VerticalScrollBarVisibility="Auto"
                         FontFamily="Consolas" FontSize="10" Background="#1E1E1E" Foreground="#D4D4D4"/>
              </TabItem>
            </TabControl>
          </GroupBox>

        </Grid>
      </TabItem>

      <!-- == Tab 4: Report == -->
      <TabItem Header=" Report " Name="ReportTab">
        <Grid Margin="6">
          <Grid.RowDefinitions>
            <RowDefinition Height="Auto"/>
            <RowDefinition Height="Auto"/>
            <RowDefinition Height="Auto"/>
            <RowDefinition Height="*" MinHeight="160"/>
          </Grid.RowDefinitions>

          <DockPanel Grid.Row="0" LastChildFill="True">
            <WrapPanel DockPanel.Dock="Right" VerticalAlignment="Top">
              <Button Name="RefreshReportBtn" Content="Refresh Report"
                      ToolTip="Rebuild the report from the loaded conversations."/>
              <Button Name="ExportReportBtn" Content="Export Report (HTML)" FontWeight="Bold"
                      ToolTip="Save a self-contained HTML report plus a .json copy of the same numbers."/>
            </WrapPanel>
            <StackPanel>
              <TextBlock Name="ReportHeadlineText" FontSize="13" FontWeight="Bold" TextWrapping="Wrap"
                         Text="No report yet. Collect results or load a JSONL file."/>
              <TextBlock Name="ReportScopeText" Foreground="Gray" TextWrapping="Wrap" Margin="0,2,0,0"/>
            </StackPanel>
          </DockPanel>

          <ScrollViewer Grid.Row="1" MaxHeight="260" VerticalScrollBarVisibility="Auto" Margin="0,6,0,0">
            <StackPanel Name="ReportKpiPanel"/>
          </ScrollViewer>

          <Border Grid.Row="2" Background="#FFF8E1" BorderBrush="#E0B000" BorderThickness="1" Padding="8,4" Margin="0,6,0,0">
            <TextBlock Name="ReportObservationsText" TextWrapping="Wrap" Text="Observations appear here once a report is built."/>
          </Border>

          <TabControl Grid.Row="3" Name="ReportTablesTab" Margin="0,6,0,0"/>
        </Grid>
      </TabItem>
    </TabControl>

    <!-- Status bar -->
    <Border Grid.Row="2" Background="#F0F0F0" BorderBrush="#CCCCCC" BorderThickness="0,1,0,0" Padding="4,2">
      <TextBlock Name="StatusText" Text="Ready - authenticate and build a query to begin."/>
    </Border>

  </Grid>
</Window>
'@

# -----------------------------------------------------------------------------
# Build WPF window from XAML
# -----------------------------------------------------------------------------

$reader = [System.Xml.XmlNodeReader]::new($xaml)
$window = [System.Windows.Markup.XamlReader]::Load($reader)

# Named controls
function Get-Control { param([string]$Name) $window.FindName($Name) }
function Get-ComboValue {
    param($ComboBox)

    if ($null -eq $ComboBox) { return '' }
    if ($null -ne $ComboBox.SelectedItem -and $ComboBox.SelectedItem -is [System.Windows.Controls.ComboBoxItem]) {
        return [string]$ComboBox.SelectedItem.Content
    }
    return [string]$ComboBox.Text
}

$regionComboBox = Get-Control 'RegionComboBox'
$clientIdBox = Get-Control 'ClientIdBox'
$clientSecretBox = Get-Control 'ClientSecretBox'
$authButton = Get-Control 'AuthButton'
$authStatusLabel = Get-Control 'AuthStatusLabel'

$startDatePicker = Get-Control 'StartDatePicker'
$startTimeTextBox = Get-Control 'StartTimeTextBox'
$endDatePicker = Get-Control 'EndDatePicker'
$endTimeTextBox = Get-Control 'EndTimeTextBox'
$directionCombo = Get-Control 'DirectionCombo'
$mediaTypeCombo = Get-Control 'MediaTypeCombo'
$orderByCombo = Get-Control 'OrderByCombo'
$orderCombo = Get-Control 'OrderCombo'

$convFilterPanel = Get-Control 'ConvFilterPanel'
$segFilterPanel = Get-Control 'SegFilterPanel'
$requestEndpointBox = Get-Control 'RequestEndpointBox'
$addConvFilterBtn = Get-Control 'AddConvFilterBtn'
$addSegFilterBtn = Get-Control 'AddSegFilterBtn'
$queryPreviewBox = Get-Control 'QueryPreviewBox'
$previewBtn = Get-Control 'PreviewBtn'
$clearFiltersBtn = Get-Control 'ClearFiltersBtn'
$submitJobBtn = Get-Control 'SubmitJobBtn'

$mainTabControl = Get-Control 'MainTabControl'
$jobIdBox = Get-Control 'JobIdBox'
$jobStateLabel = Get-Control 'JobStateLabel'
$jobPollLabel = Get-Control 'JobPollLabel'
$jobElapsedLabel = Get-Control 'JobElapsedLabel'
$cancelJobBtn = Get-Control 'CancelJobBtn'
$jobLogBox = Get-Control 'JobLogBox'
$collectStatusText = Get-Control 'CollectStatusText'
$collectResultsBtn = Get-Control 'CollectResultsBtn'

$summaryText = Get-Control 'SummaryText'
$columnSelectorBtn = Get-Control 'ColumnSelectorBtn'
$resolveNamesBtn = Get-Control 'ResolveNamesBtn'
$exportCsvBtn = Get-Control 'ExportCsvBtn'
$exportJsonlBtn = Get-Control 'ExportJsonlBtn'
$loadJsonlBtn = Get-Control 'LoadJsonlBtn'
$clearResultsBtn = Get-Control 'ClearResultsBtn'
$redactExportsCheckBox = Get-Control 'RedactExportsCheckBox'
$resultsGrid = Get-Control 'ResultsGrid'
$overviewPanel = Get-Control 'OverviewPanel'
$attributesGrid = Get-Control 'AttributesGrid'
$participantsGrid = Get-Control 'ParticipantsGrid'
$segmentsGrid = Get-Control 'SegmentsGrid'
$metricsGrid = Get-Control 'MetricsGrid'
$flowsGrid = Get-Control 'FlowsGrid'
$rawJsonBox = Get-Control 'RawJsonBox'

$refreshReportBtn = Get-Control 'RefreshReportBtn'
$exportReportBtn = Get-Control 'ExportReportBtn'
$reportHeadlineText = Get-Control 'ReportHeadlineText'
$reportScopeText = Get-Control 'ReportScopeText'
$reportKpiPanel = Get-Control 'ReportKpiPanel'
$reportObservationsText = Get-Control 'ReportObservationsText'
$reportTablesTab = Get-Control 'ReportTablesTab'
$statusText = Get-Control 'StatusText'

$detailTabControl = Get-Control 'DetailTabControl'

$script:exportRedactionMode = [bool]$redactExportsCheckBox.IsChecked
$redactExportsCheckBox.Add_Checked({ $script:exportRedactionMode = $true })
$redactExportsCheckBox.Add_Unchecked({ $script:exportRedactionMode = $false })

# -----------------------------------------------------------------------------
# Helpers used from event handlers
# -----------------------------------------------------------------------------

function Set-Status { param([string]$Msg) $statusText.Text = $Msg }

function Append-JobLog {
    param([string]$Line)
    $ts = [DateTime]::Now.ToString('HH:mm:ss')
    $jobLogBox.AppendText("[$ts] $Line`n")
    $jobLogBox.ScrollToEnd()
}

function Format-UiApiFailure {
    param([System.Exception]$Exception)
    $summary = Get-UiRequestFailureSummary -Exception $Exception
    return [string]$summary.DisplayText
}

function Write-UiApiRetryLogEntry {
    param([psobject]$Entry)

    $delayText = ('{0:0.###}' -f [double]$Entry.DelaySeconds) + 's'
    $statusText = if ($null -ne $Entry.StatusCode) { "HTTP $($Entry.StatusCode)" } else { 'no HTTP status' }
    $retryAfterText = if ($null -ne $Entry.RetryAfterSeconds) {
        " Retry-After $($Entry.RetryAfterSeconds)s was honored."
    }
    else {
        ''
    }

    Append-JobLog "Transient $($Entry.Method) failure on attempt $($Entry.Attempt)/$($Entry.MaxAttempts) ($statusText). Retrying in $delayText.$retryAfterText"
}

$script:gcApiRetryLogAction = {
    param($Entry)
    Write-UiApiRetryLogEntry -Entry $Entry
}

function New-FilterRow {
    param([string]$Type, [System.Windows.Controls.StackPanel]$Panel)

    $dims = if ($Type -eq 'conversation') {
        @('mediaType', 'originatingDirection', 'queueId', 'userId', 'conversationId', 'divisionId', 'flowId', 'isEnded', 'flaggedReason')
    }
    else {
        @('purpose', 'segmentType', 'queueId', 'userId', 'flowId', 'disconnectType', 'edgeId')
    }

    $border = New-Object System.Windows.Controls.Border
    $border.BorderBrush = [System.Windows.Media.Brushes]::LightGray
    $border.BorderThickness = [System.Windows.Thickness]::new(0, 0, 0, 1)
    $border.Margin = [System.Windows.Thickness]::new(0, 1, 0, 1)

    $grid = New-Object System.Windows.Controls.Grid
    @(70, 150, 110, 1, 28) | ForEach-Object {
        $cd = New-Object System.Windows.Controls.ColumnDefinition
        if ($_ -eq 1) {
            $cd.Width = [System.Windows.GridLength]::new(1, [System.Windows.GridUnitType]::Star)
        }
        else {
            $cd.Width = [System.Windows.GridLength]::new($_)
        }
        $grid.ColumnDefinitions.Add($cd) | Out-Null
    }
    $border.Child = $grid

    $logicCb = New-Object System.Windows.Controls.ComboBox
    $logicCb.Margin = [System.Windows.Thickness]::new(1)
    @('and', 'or') | ForEach-Object { $logicCb.Items.Add($_) | Out-Null }
    $logicCb.SelectedIndex = 0
    [System.Windows.Controls.Grid]::SetColumn($logicCb, 0); $grid.Children.Add($logicCb) | Out-Null

    $dimCb = New-Object System.Windows.Controls.ComboBox
    $dimCb.Margin = [System.Windows.Thickness]::new(1); $dimCb.IsEditable = $true
    $dims | ForEach-Object { $dimCb.Items.Add($_) | Out-Null }
    $dimCb.SelectedIndex = 0
    [System.Windows.Controls.Grid]::SetColumn($dimCb, 1); $grid.Children.Add($dimCb) | Out-Null

    $opCb = New-Object System.Windows.Controls.ComboBox
    $opCb.Margin = [System.Windows.Thickness]::new(1)
    @('matches', 'notMatches', 'lt', 'lte', 'gt', 'gte') | ForEach-Object { $opCb.Items.Add($_) | Out-Null }
    $opCb.SelectedIndex = 0
    [System.Windows.Controls.Grid]::SetColumn($opCb, 2); $grid.Children.Add($opCb) | Out-Null

    $valTb = New-Object System.Windows.Controls.TextBox
    $valTb.Margin = [System.Windows.Thickness]::new(1)
    [System.Windows.Controls.Grid]::SetColumn($valTb, 3); $grid.Children.Add($valTb) | Out-Null

    $remBtn = New-Object System.Windows.Controls.Button
    $remBtn.Content = 'X'; $remBtn.Margin = [System.Windows.Thickness]::new(1); $remBtn.Padding = [System.Windows.Thickness]::new(2, 0, 2, 0)
    $remBtn.ToolTip = 'Remove filter'
    $capturedBorder = $border; $capturedPanel = $Panel
    $remBtn.Add_Click({ $capturedPanel.Children.Remove($capturedBorder) }.GetNewClosure())
    [System.Windows.Controls.Grid]::SetColumn($remBtn, 4); $grid.Children.Add($remBtn) | Out-Null

    $Panel.Children.Add($border) | Out-Null

    return [pscustomobject]@{ Border = $border; Logic = $logicCb; Dimension = $dimCb; Operator = $opCb; Value = $valTb }
}

function Collect-FilterRows {
    param([System.Windows.Controls.StackPanel]$Panel)
    $rows = [System.Collections.Generic.List[pscustomobject]]::new()
    foreach ($child in @($Panel.Children)) {
        if ($child -isnot [System.Windows.Controls.Border]) { continue }
        $g = $child.Child
        if ($null -eq $g) { continue }
        $items = @($g.Children)
        $rows.Add([pscustomobject]@{
                logic     = if ($items[0].SelectedItem) { [string]$items[0].SelectedItem } else { 'and' }
                dimension = if ($items[1].Text) { [string]$items[1].Text } else { '' }
                operator  = if ($items[2].SelectedItem) { [string]$items[2].SelectedItem } else { 'matches' }
                value     = [string]$items[3].Text
            }) | Out-Null
    }
    return @($rows | Where-Object { -not [string]::IsNullOrWhiteSpace($_.dimension) -and -not [string]::IsNullOrWhiteSpace($_.value) })
}

function Resolve-SelectedDateValue {
    param(
        [AllowNull()]
        [object]$PrimaryDate,
        [AllowNull()]
        [object]$FallbackDate
    )

    foreach ($candidate in @($PrimaryDate, $FallbackDate)) {
        if ($null -eq $candidate) {
            continue
        }

        if ($candidate -is [DateTime]) {
            return $candidate.Date
        }

        if ($candidate -is [Nullable[DateTime]] -and $candidate.HasValue) {
            return $candidate.GetValueOrDefault().Date
        }

        $parsed = [DateTime]::MinValue
        if ([DateTime]::TryParse([string]$candidate, [ref]$parsed)) {
            return $parsed.Date
        }
    }

    return [DateTime]::Today
}

function Resolve-TimeOfDayValue {
    param(
        [string]$Text,
        [TimeSpan]$DefaultValue,
        [string]$Label
    )

    if ([string]::IsNullOrWhiteSpace($Text)) {
        return $DefaultValue
    }

    $parsed = [TimeSpan]::Zero
    foreach ($format in @('hh\:mm', 'hh\:mm\:ss', 'h\:mm', 'h\:mm\:ss')) {
        if ([TimeSpan]::TryParseExact($Text.Trim(), $format, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$parsed)) {
            return $parsed
        }
    }

    if ([TimeSpan]::TryParse($Text.Trim(), [System.Globalization.CultureInfo]::InvariantCulture, [ref]$parsed)) {
        return $parsed
    }

    throw "$Label must be in HH:mm or HH:mm:ss format."
}

function Resolve-IntervalSelection {
    $startDate = Resolve-SelectedDateValue -PrimaryDate $startDatePicker.SelectedDate -FallbackDate $endDatePicker.SelectedDate
    $endDate = Resolve-SelectedDateValue -PrimaryDate $endDatePicker.SelectedDate -FallbackDate $startDatePicker.SelectedDate

    $startTime = Resolve-TimeOfDayValue -Text ([string]$startTimeTextBox.Text) -DefaultValue ([TimeSpan]::Zero) -Label 'Start time'
    $endTime = Resolve-TimeOfDayValue -Text ([string]$endTimeTextBox.Text) -DefaultValue ([TimeSpan]::new(23, 59, 59)) -Label 'End time'

    $startLocal = $startDate.Date.Add($startTime)
    $endLocal = $endDate.Date.Add($endTime)
    if ($endLocal -le $startLocal) {
        throw "End date/time must be after start date/time."
    }

    $startUtc = [DateTime]::SpecifyKind($startLocal, [System.DateTimeKind]::Local).ToUniversalTime()
    $endUtc = [DateTime]::SpecifyKind($endLocal, [System.DateTimeKind]::Local).ToUniversalTime()

    return [pscustomobject]@{
        StartDate  = $startDate
        EndDate    = $endDate
        StartTime  = $startTime
        EndTime    = $endTime
        StartLocal = $startLocal
        EndLocal   = $endLocal
        StartUtc   = $startUtc
        EndUtc     = $endUtc
        Interval   = "$($startUtc.ToString('yyyy-MM-ddTHH:mm:ss.fffZ'))/$($endUtc.ToString('yyyy-MM-ddTHH:mm:ss.fffZ'))"
    }
}

function Set-IntervalControlDefaults {
    # Accepts an already-resolved interval so Build-QueryBody and Set-IntervalControlDefaults
    # always operate on the exact same resolution rather than calling Resolve-IntervalSelection twice.
    param(
        [AllowNull()]
        [pscustomobject]$Resolved = $null
    )
    if ($null -eq $Resolved) { $Resolved = Resolve-IntervalSelection }

    if (-not $startDatePicker.SelectedDate) { $startDatePicker.SelectedDate = $Resolved.StartDate }
    if (-not $endDatePicker.SelectedDate) { $endDatePicker.SelectedDate = $Resolved.EndDate }
    if ([string]::IsNullOrWhiteSpace([string]$startTimeTextBox.Text)) { $startTimeTextBox.Text = $Resolved.StartTime.ToString('hh\:mm\:ss') }
    if ([string]::IsNullOrWhiteSpace([string]$endTimeTextBox.Text)) { $endTimeTextBox.Text = $Resolved.EndTime.ToString('hh\:mm\:ss') }
}

function Test-MapContainsKey {
    param(
        [AllowNull()]
        [object]$Map,
        [string]$Key
    )

    if ($null -eq $Map -or [string]::IsNullOrWhiteSpace($Key)) {
        return $false
    }

    if ($Map -is [System.Collections.Specialized.OrderedDictionary]) {
        return $Map.Contains($Key)
    }

    if ($Map -is [System.Collections.IDictionary]) {
        if ($Map.Contains($Key)) { return $true }
        if ($Map -is [hashtable] -and $Map.ContainsKey($Key)) { return $true }
    }

    return $false
}

function Test-RegionValue {
    # Returns $true only if the value looks like a valid Genesys Cloud API region hostname.
    # Accepted forms: 'usw2.pure.cloud', 'mypurecloud.com', 'mypurecloud.com.au', etc.
    # Rejected: empty, whitespace-only, contains spaces, no dot, or invalid hostname chars.
    param([string]$Region)
    if ([string]::IsNullOrWhiteSpace($Region)) { return $false }
    $r = $Region.Trim()
    # Must contain at least one dot and consist only of valid hostname characters
    if ($r -notlike '*.*') { return $false }
    if ($r -match '\s') { return $false }
    if ($r -notmatch '^[a-zA-Z0-9]([a-zA-Z0-9\-\.]*[a-zA-Z0-9])?$') { return $false }
    return $true
}

function Build-QueryBody {
    $intervalSelection = Resolve-IntervalSelection
    Set-IntervalControlDefaults -Resolved $intervalSelection
    $body = [ordered]@{
        interval = $intervalSelection.Interval
        order    = Get-ComboValue $orderCombo
        orderBy  = Get-ComboValue $orderByCombo
    }

    # Quick filter: direction
    $dir = Get-ComboValue $directionCombo
    if (-not [string]::IsNullOrWhiteSpace($dir) -and $dir -ne '(any)') {
        $body['conversationFilters'] = @(@{
                type       = 'and'
                predicates = @(@{ dimension = 'originatingDirection'; value = $dir })
            })
    }

    # Quick filter: media type -> segment filter (purpose=agent + mediaType from session)
    $mt = Get-ComboValue $mediaTypeCombo
    if (-not [string]::IsNullOrWhiteSpace($mt) -and $mt -ne '(any)') {
        if (Test-MapContainsKey -Map $body -Key 'segmentFilters') {
            $existing = [System.Collections.Generic.List[object]]::new([object[]]@($body['segmentFilters']))
        } else {
            $existing = [System.Collections.Generic.List[object]]::new()
        }
        $existing.Add(@{
                type       = 'and'
                predicates = @(@{ dimension = 'mediaType'; value = $mt })
            }) | Out-Null
        $body['segmentFilters'] = @($existing)
    }

    # Custom conversation filters
    $convRows = Collect-FilterRows -Panel $convFilterPanel
    if ($convRows.Count -gt 0) {
        if (Test-MapContainsKey -Map $body -Key 'conversationFilters') {
            $existingConv = [System.Collections.Generic.List[object]]::new([object[]]@($body['conversationFilters']))
        } else {
            $existingConv = [System.Collections.Generic.List[object]]::new()
        }
        foreach ($r in $convRows) {
            $pred = [ordered]@{ dimension = $r.dimension; value = $r.value }
            if ($r.operator -ne 'matches') { $pred['operator'] = $r.operator }
            $existingConv.Add([ordered]@{ type = $r.logic; predicates = @($pred) }) | Out-Null
        }
        $body['conversationFilters'] = @($existingConv)
    }

    # Custom segment filters
    $segRows = Collect-FilterRows -Panel $segFilterPanel
    if ($segRows.Count -gt 0) {
        if (Test-MapContainsKey -Map $body -Key 'segmentFilters') {
            $existingSeg = [System.Collections.Generic.List[object]]::new([object[]]@($body['segmentFilters']))
        } else {
            $existingSeg = [System.Collections.Generic.List[object]]::new()
        }
        foreach ($r in $segRows) {
            $pred = [ordered]@{ dimension = $r.dimension; value = $r.value }
            if ($r.operator -ne 'matches') { $pred['operator'] = $r.operator }
            $existingSeg.Add([ordered]@{ type = $r.logic; predicates = @($pred) }) | Out-Null
        }
        $body['segmentFilters'] = @($existingSeg)
    }

    return $body
}

function Build-QueryRequestPreview {
    # Validates and resolves the region from the UI control, then builds both the
    # endpoint URI and the request body.  Also syncs $script:baseUri so the submit
    # path always targets the same endpoint that was shown in the preview.
    $body = Build-QueryBody

    $region = ([string]$regionComboBox.Text).Trim()
    if ([string]::IsNullOrWhiteSpace($region)) { $region = $DefaultRegion }

    if (-not (Test-RegionValue -Region $region)) {
        throw "Invalid region '$region'. Region must be a valid hostname (e.g. 'usw2.pure.cloud' or 'mypurecloud.com')."
    }

    # Keep the API base URI in sync so submit uses the same region shown in preview
    $script:baseUri = "https://api.$region"

    return [pscustomobject]@{
        EndpointUri = "$($script:baseUri)/api/v2/analytics/conversations/details/jobs"
        Body        = $body
        BodyJson    = ($body | ConvertTo-Json -Depth 10)
    }
}

# -----------------------------------------------------------------------------
# Results grid population
# -----------------------------------------------------------------------------

function Set-GridRows {
    # Binds rows to a DataGrid, generating one text column per property. Column headers
    # get tooltips from the column dictionary when the name is a known report column.
    param(
        [System.Windows.Controls.DataGrid]$Grid,
        [AllowNull()][object[]]$Rows,
        [string[]]$Columns,
        [hashtable]$HeaderMap
    )
    $Grid.ItemsSource = $null
    $Grid.Columns.Clear()
    if ($null -eq $Rows -or $Rows.Count -eq 0) { return }
    $list = $Rows
    $names = if ($Columns) { $Columns } else { @($list[0].PSObject.Properties.Name) }
    foreach ($name in $names) {
        $header = New-Object System.Windows.Controls.TextBlock
        $header.Text = if ($null -ne $HeaderMap -and $HeaderMap.ContainsKey($name)) { [string]$HeaderMap[$name] } else { $name }
        $tip = Get-ColumnDescription -Name $name
        if ($tip) { $header.ToolTip = $tip }
        $column = New-Object System.Windows.Controls.DataGridTextColumn
        $column.Header = $header
        $column.SortMemberPath = $name
        $column.Binding = New-Object System.Windows.Data.Binding($name)
        if ($name -like '*Id' -and $name -ne 'ConversationId') { $column.FontFamily = New-Object System.Windows.Media.FontFamily('Consolas') }
        $Grid.Columns.Add($column) | Out-Null
    }
    $Grid.ItemsSource = $list
}

function New-InfoTile {
    param([string]$Label, [string]$Value, [string]$Caption, [string]$ToolTip, [double]$MinWidth = 0)
    $border = New-Object System.Windows.Controls.Border
    $border.Background = [System.Windows.Media.Brushes]::WhiteSmoke
    $border.BorderBrush = [System.Windows.Media.Brushes]::LightGray
    $border.BorderThickness = [System.Windows.Thickness]::new(1)
    $border.Margin = [System.Windows.Thickness]::new(4, 2, 4, 2)
    $border.Padding = [System.Windows.Thickness]::new(6, 3, 6, 3)
    $border.CornerRadius = [System.Windows.CornerRadius]::new(3)
    if ($MinWidth -gt 0) { $border.MinWidth = $MinWidth }
    $border.MaxWidth = 420
    if ($ToolTip) { $border.ToolTip = $ToolTip }

    $stack = New-Object System.Windows.Controls.StackPanel
    $labelBlock = New-Object System.Windows.Controls.TextBlock
    $labelBlock.Text = $Label
    $labelBlock.FontSize = 10
    $labelBlock.Foreground = [System.Windows.Media.Brushes]::Gray
    $valueBlock = New-Object System.Windows.Controls.TextBlock
    $valueBlock.Text = if ([string]::IsNullOrEmpty($Value)) { '-' } else { $Value }
    $valueBlock.FontWeight = [System.Windows.FontWeights]::SemiBold
    $valueBlock.TextWrapping = 'Wrap'
    $stack.Children.Add($labelBlock) | Out-Null
    $stack.Children.Add($valueBlock) | Out-Null
    if ($Caption) {
        $captionBlock = New-Object System.Windows.Controls.TextBlock
        $captionBlock.Text = $Caption
        $captionBlock.FontSize = 10
        $captionBlock.Foreground = [System.Windows.Media.Brushes]::Gray
        $captionBlock.TextWrapping = 'Wrap'
        $stack.Children.Add($captionBlock) | Out-Null
    }
    $border.Child = $stack
    return $border
}

function Update-ConversationProfiles {
    # Builds (or reuses) the cached profile for every loaded conversation, with progress.
    $snapshot = $script:allConversations.ToArray()
    $total = $snapshot.Length
    $profiles = [System.Collections.Generic.List[object]]::new($total)
    $i = 0
    foreach ($conv in $snapshot) {
        $profiles.Add((Get-CachedConversationProfile -Conv $conv)) | Out-Null
        $i++
        if ($i % 500 -eq 0) {
            Set-Status "Analyzing conversations... $i of $total"
            [System.Windows.Forms.Application]::DoEvents()
        }
    }
    return $profiles
}

function Get-GridRows {
    # Flat rows for the grid, cached until attribute columns or resolved names change.
    $attrCols = @($script:selectedAttrCols)
    $cacheKey = '{0}|{1}|{2}' -f $script:lookupVersion, $script:allConversations.Count, ($attrCols -join [char]31)
    if ($null -ne $script:gridRowCache -and $script:gridRowCacheKey -eq $cacheKey) { return $script:gridRowCache }

    $snapshot = $script:allConversations.ToArray()
    $limit = [Math]::Min($snapshot.Length, $script:maxGridRows)
    $rows = [System.Collections.Generic.List[object]]::new($limit)
    for ($i = 0; $i -lt $limit; $i++) {
        $conv = $snapshot[$i]
        $rows.Add((ConvertTo-FlatRow -ConversationProfile (Get-CachedConversationProfile -Conv $conv) -Conversation $conv -AttrCols $attrCols -Lookups $script:lookups -ForGrid)) | Out-Null
        if (($i + 1) % 500 -eq 0) {
            Set-Status "Building grid rows... $($i + 1) of $limit"
            [System.Windows.Forms.Application]::DoEvents()
        }
    }
    $script:gridRowCache = $rows
    $script:gridRowCacheKey = $cacheKey
    return $rows
}

function Show-Results {
    $total = $script:allConversations.Count

    # Warn before rendering if the dataset is large enough to cause visible UI lag
    if ($total -gt 10000 -and $null -eq $script:gridRowCache) {
        $answer = [System.Windows.MessageBox]::Show(
            "This dataset contains $total conversations. Analyzing and rendering may take a minute and the UI will be unresponsive during that time.`n`nContinue?",
            'Large Dataset', 'YesNo', 'Warning')
        if ($answer -ne 'Yes') {
            Set-Status "Render cancelled. $total conversations remain in memory - exports are still available."
            return
        }
    }

    $profiles = Update-ConversationProfiles
    $rows = Get-GridRows
    $attrCols = @($script:selectedAttrCols)

    # Visible standard columns in dictionary order, then attribute columns (bound as Attr0..N)
    $allStandard = Get-FlatRowColumnNames
    $columns = [System.Collections.Generic.List[string]]::new()
    $headers = @{}
    foreach ($name in $allStandard) { if ($script:visibleStdCols -contains $name) { $columns.Add($name) | Out-Null } }
    for ($i = 0; $i -lt $attrCols.Count; $i++) { $columns.Add("Attr$i") | Out-Null; $headers["Attr$i"] = "A:$($attrCols[$i])" }
    Set-GridRows -Grid $resultsGrid -Rows $rows -Columns @($columns) -HeaderMap $headers

    # Summary line from the cached profiles (single pass, no per-row pipelines)
    $inbound = 0; $outbound = 0; $offered = 0; $answered = 0; $abandoned = 0; $handleMs = 0.0; $handled = 0
    $mediaCounts = @{}
    foreach ($cp in $profiles) {
        if ($cp.Direction -eq 'inbound') { $inbound++ } elseif ($cp.Direction -eq 'outbound') { $outbound++ }
        if ($cp.Offered) { $offered++ }
        if ($cp.Answered) { $answered++ }
        if ($cp.Abandoned) { $abandoned++ }
        $value = $cp.MetricSums['tHandle']
        if ($null -ne $value) { $handleMs += $value; $handled++ }
        $mt = if ([string]::IsNullOrWhiteSpace($cp.MediaType)) { '(blank)' } else { $cp.MediaType }
        if ($mediaCounts.ContainsKey($mt)) { $mediaCounts[$mt]++ } else { $mediaCounts[$mt] = 1 }
    }
    $byMedia = @($mediaCounts.GetEnumerator() | Sort-Object Value -Descending | ForEach-Object { "$($_.Key): $($_.Value)" })
    $abandonText = if ($offered -gt 0) { '  Abandoned: {0} ({1:0.0}%)' -f $abandoned, (100.0 * $abandoned / $offered) } else { '' }
    $ahtText = if ($handled -gt 0) { '  |  AHT: {0}' -f (Format-SecondsDisplay ([Math]::Round($handleMs / $handled / 1000.0, 1))) } else { '' }
    $namesNote = if ($script:lookupsLoaded) { '' } else { '  |  Names not resolved (IDs shown) - click Resolve Names.' }
    $displayNotice = if ($total -gt $rows.Count) {
        "  |  Grid showing first $($rows.Count) of $total rows. Report and exports include all loaded conversations."
    }
    else { '' }

    $summaryText.Text = "Loaded $total conversations  |  Inbound: $inbound  Outbound: $outbound  |  Offered: $offered  Answered: $answered$abandonText$ahtText  |  $($byMedia -join '  |  ')$displayNotice$namesNote"

    Update-ReportView -Profiles $profiles
    Set-Status "Results loaded: $total conversations."
}

function Show-ConversationDetail {
    param([object]$Conv)

    $conversationProfile = Get-CachedConversationProfile -Conv $Conv

    # -- Overview tiles
    $overviewPanel.Children.Clear()
    $fields = Get-ConversationOverviewFields -ConversationProfile $conversationProfile -Lookups $script:lookups
    foreach ($field in $fields.GetEnumerator()) {
        $overviewPanel.Children.Add((New-InfoTile -Label $field.Key -Value ([string]$field.Value) -MinWidth 120)) | Out-Null
    }

    Set-GridRows -Grid $participantsGrid -Rows (Get-ConversationSessionRows -Conversation $Conv -Lookups $script:lookups)
    Set-GridRows -Grid $segmentsGrid -Rows (Get-ConversationSegmentRows -Conversation $Conv -Lookups $script:lookups)
    Set-GridRows -Grid $metricsGrid -Rows (Get-ConversationMetricRows -Conversation $Conv)
    Set-GridRows -Grid $flowsGrid -Rows (Get-ConversationFlowRows -Conversation $Conv)
    Set-GridRows -Grid $attributesGrid -Rows (Get-ConversationAttributeRows -Conversation $Conv)

    # -- Raw JSON
    $rawJsonBox.Text = $Conv | ConvertTo-Json -Depth 20
}

function Clear-ReportView {
    $reportHeadlineText.Text = 'No report yet. Collect results or load a JSONL file.'
    $reportScopeText.Text = ''
    $reportKpiPanel.Children.Clear()
    $reportObservationsText.Text = 'Observations appear here once a report is built.'
    $reportTablesTab.Items.Clear()
}

function Update-ReportView {
    param([object[]]$Profiles, [switch]$Force)

    if ($script:allConversations.Count -eq 0) { Clear-ReportView; return }
    if ($null -eq $script:currentReport -or $Force) {
        if ($null -eq $Profiles) { $Profiles = @(Update-ConversationProfiles) }
        Set-Status 'Building report...'
        [System.Windows.Forms.Application]::DoEvents()
        $script:currentReport = Get-ConversationReport -Profiles $Profiles -Lookups $script:lookups -Source $script:dataSource -QueryInterval $script:dataQueryInterval
    }
    $report = $script:currentReport

    $reportHeadlineText.Text = $report.Headline
    $scope = [System.Collections.Generic.List[string]]::new()
    $scope.Add("Data window: $($report.WindowStartLocal) to $($report.WindowEndLocal) ($($report.TimeZone))") | Out-Null
    if ($report.QueryInterval) { $scope.Add("Query interval (UTC): $($report.QueryInterval)") | Out-Null }
    if ($report.Source) { $scope.Add("Source: $($report.Source)") | Out-Null }
    $scope.Add("Generated: $($report.GeneratedLocal)") | Out-Null
    $scope.Add($report.Units) | Out-Null
    $reportScopeText.Text = $scope -join '   |   '

    # KPI tiles, one wrap row per section
    $reportKpiPanel.Children.Clear()
    foreach ($section in @($report.Kpis | ForEach-Object Section | Select-Object -Unique)) {
        $heading = New-Object System.Windows.Controls.TextBlock
        $heading.Text = $section.ToUpperInvariant()
        $heading.FontSize = 10
        $heading.FontWeight = [System.Windows.FontWeights]::Bold
        $heading.Foreground = [System.Windows.Media.Brushes]::SteelBlue
        $heading.Margin = [System.Windows.Thickness]::new(4, 4, 0, 0)
        $reportKpiPanel.Children.Add($heading) | Out-Null
        $wrap = New-Object System.Windows.Controls.WrapPanel
        foreach ($kpi in @($report.Kpis | Where-Object { $_.Section -eq $section })) {
            $wrap.Children.Add((New-InfoTile -Label $kpi.Metric -Value $kpi.Display -Caption $kpi.Detail -MinWidth 150)) | Out-Null
        }
        $reportKpiPanel.Children.Add($wrap) | Out-Null
    }

    $observations = @($report.Observations)
    $reportObservationsText.Text = if ($observations.Count -gt 0) {
        'Observations (reference thresholds - compare against your own targets):' + [Environment]::NewLine + (($observations | ForEach-Object { "  - $_" }) -join [Environment]::NewLine)
    }
    else { 'Observations: no reference thresholds were exceeded.' }

    # One tab per breakdown table
    $selectedHeader = if ($null -ne $reportTablesTab.SelectedItem) { [string]$reportTablesTab.SelectedItem.Header } else { '' }
    $reportTablesTab.Items.Clear()
    foreach ($title in $report.Tables.Keys) {
        $table = $report.Tables[$title]
        $dock = New-Object System.Windows.Controls.DockPanel
        $description = New-Object System.Windows.Controls.TextBlock
        $description.Text = $table.Description
        $description.Foreground = [System.Windows.Media.Brushes]::Gray
        $description.TextWrapping = 'Wrap'
        $description.Margin = [System.Windows.Thickness]::new(2, 2, 2, 4)
        [System.Windows.Controls.DockPanel]::SetDock($description, 'Top')
        $dock.Children.Add($description) | Out-Null

        $grid = New-Object System.Windows.Controls.DataGrid
        $grid.IsReadOnly = $true
        $grid.AutoGenerateColumns = $false
        $grid.CanUserSortColumns = $true
        $grid.GridLinesVisibility = 'Horizontal'
        $grid.AlternatingRowBackground = New-Object System.Windows.Media.SolidColorBrush([System.Windows.Media.Color]::FromRgb(0xF9, 0xF9, 0xF9))
        $headers = @{}
        $rows = @($table.Rows)
        if ($rows.Count -gt 0) {
            foreach ($name in $rows[0].PSObject.Properties.Name) { $headers[$name] = ConvertTo-ReportHeaderText $name }
        }
        Set-GridRows -Grid $grid -Rows $rows -HeaderMap $headers
        $dock.Children.Add($grid) | Out-Null

        $tab = New-Object System.Windows.Controls.TabItem
        $tab.Header = "$title ($($rows.Count))"
        $tab.Content = $dock
        $reportTablesTab.Items.Add($tab) | Out-Null
        if ($selectedHeader -and $selectedHeader.StartsWith("$title (")) { $reportTablesTab.SelectedItem = $tab }
    }
    if ($null -eq $reportTablesTab.SelectedItem -and $reportTablesTab.Items.Count -gt 0) { $reportTablesTab.SelectedIndex = 0 }
}

function Export-ReportFile {
    if ($script:allConversations.Count -eq 0) {
        [System.Windows.MessageBox]::Show('No results to report on.', 'Export Report', 'OK', 'Information') | Out-Null
        return
    }
    if ($null -eq $script:currentReport) { Update-ReportView }

    $dlg = New-Object System.Windows.Forms.SaveFileDialog
    $dlg.Filter = 'HTML report (*.html)|*.html'
    $dlg.FileName = "conversation-report-$(Get-Date -Format 'yyyyMMdd-HHmmss').html"
    if ($dlg.ShowDialog() -ne 'OK') { return }

    try {
        $htmlPath = $dlg.FileName
        $jsonPath = [System.IO.Path]::ChangeExtension($htmlPath, '.json')
        $utf8 = New-Object System.Text.UTF8Encoding($false)
        [System.IO.File]::WriteAllText($htmlPath, (ConvertTo-ConversationReportHtml -Report $script:currentReport), $utf8)
        [System.IO.File]::WriteAllText($jsonPath, (ConvertTo-ConversationReportJson -Report $script:currentReport), $utf8)
        Set-Status "Report exported to $htmlPath"
        $open = [System.Windows.MessageBox]::Show("Report saved:`n$htmlPath`n$jsonPath`n`nOpen the HTML report now?", 'Export Complete', 'YesNo', 'Information')
        if ($open -eq 'Yes') { Start-Process $htmlPath }
    }
    catch {
        [System.Windows.MessageBox]::Show("Report export failed:`n$($_.Exception.Message)", 'Export Error', 'OK', 'Error') | Out-Null
    }
}

function Update-ReferenceLookups {
    # Loads id -> name maps for the reference data that analytics records only carry as IDs.
    # Each type fails independently (e.g. missing permission) and falls back to raw IDs.
    param([switch]$Force)

    if ([string]::IsNullOrWhiteSpace($script:accessToken)) {
        Append-JobLog 'Name lookup skipped: not authenticated. IDs are shown instead of names.'
        return $false
    }
    if ($script:lookupsLoaded -and -not $Force) { return $true }

    $sources = @(
        @{ Kind = 'queues'; Label = 'queues'; Path = '/api/v2/routing/queues' }
        @{ Kind = 'wrapupCodes'; Label = 'wrap-up codes'; Path = '/api/v2/routing/wrapupcodes' }
        @{ Kind = 'divisions'; Label = 'divisions'; Path = '/api/v2/authorization/divisions' }
        @{ Kind = 'skills'; Label = 'skills'; Path = '/api/v2/routing/skills' }
        @{ Kind = 'languages'; Label = 'languages'; Path = '/api/v2/routing/languages' }
    )
    $loadedAny = $false
    foreach ($source in $sources) {
        $map = @{}
        try {
            $pageNumber = 1
            $hasMore = $true
            while ($hasMore -and $pageNumber -le $script:maxLookupPages) {
                Set-Status "Resolving names: $($source.Label) (page $pageNumber)..."
                [System.Windows.Forms.Application]::DoEvents()
                $response = Invoke-GcApiRequest -Method 'GET' -Path $source.Path -QueryParams @{ pageSize = '100'; pageNumber = [string]$pageNumber }
                $entities = @($response.entities)
                foreach ($entity in $entities) {
                    if ($null -ne $entity -and -not [string]::IsNullOrWhiteSpace([string]$entity.id)) { $map[[string]$entity.id] = [string]$entity.name }
                }
                $hasMore = if ($null -ne $response.pageCount) { $pageNumber -lt [int]$response.pageCount } else { $entities.Count -ge 100 }
                $pageNumber++
            }
            $script:lookups[$source.Kind] = $map
            $loadedAny = $true
            Append-JobLog "Name lookup: loaded $($map.Count) $($source.Label)."
        }
        catch {
            $failureText = Format-UiApiFailure -Exception $_.Exception
            Append-JobLog "Name lookup for $($source.Label) failed; IDs will be shown instead. $failureText"
        }
    }

    # Marked loaded even on partial failure so every collect does not retry; Resolve Names forces a retry.
    $script:lookupsLoaded = $true
    $script:lookupVersion++
    Reset-AnalysisCaches
    Set-Status 'Name lookup complete.'
    return $loadedAny
}

# -----------------------------------------------------------------------------
# DispatcherTimer (job polling - runs on UI thread, no runspace needed)
# -----------------------------------------------------------------------------

$script:pollTimer = New-Object System.Windows.Threading.DispatcherTimer
$script:pollTimer.Interval = [TimeSpan]::FromSeconds(3)

$script:pollTimer.Add_Tick({
        # -- Check guardrails before making the API call --
        $elapsed = [DateTime]::UtcNow - $script:jobSubmitTime
        $jobElapsedLabel.Text = $elapsed.ToString('mm\:ss')

        if ($script:pollCount -ge $script:maxPollCount) {
            $script:pollTimer.Stop()
            $cancelJobBtn.IsEnabled = $false
            $jobStateLabel.Text = 'STOPPED'
            $jobStateLabel.Foreground = [System.Windows.Media.Brushes]::DarkRed
            Append-JobLog "Polling stopped: reached max poll count ($($script:maxPollCount))."
            $collectStatusText.Text = "Polling stopped after $($script:maxPollCount) attempts. Cancel the job or try collecting manually if it completed."
            Set-Status 'Polling stopped - max poll count reached.'
            return
        }

        if ($elapsed.TotalMinutes -ge $script:maxPollTimeoutMinutes) {
            $script:pollTimer.Stop()
            $cancelJobBtn.IsEnabled = $false
            $jobStateLabel.Text = 'TIMEOUT'
            $jobStateLabel.Foreground = [System.Windows.Media.Brushes]::DarkRed
            Append-JobLog "Polling stopped: timeout after $($script:maxPollTimeoutMinutes) minutes."
            $collectStatusText.Text = "Polling timed out after $($script:maxPollTimeoutMinutes) min. Cancel the job or try collecting manually if it completed."
            Set-Status 'Polling stopped - timeout.'
            return
        }

        try {
            $status = Get-AnalyticsJobStatus -JobId $script:currentJobId
            $state = [string]$status.state

            $script:pollCount++
            $script:consecutivePollErrors = 0
            $jobStateLabel.Text = $state
            $jobPollLabel.Text = [string]$script:pollCount

            Append-JobLog "Poll $($script:pollCount): state=$state"

            if ($state -in @('FULFILLED', 'FAILED', 'CANCELLED')) {
                $script:pollTimer.Stop()
                $cancelJobBtn.IsEnabled = $false

                if ($state -eq 'FULFILLED') {
                    $jobStateLabel.Foreground = [System.Windows.Media.Brushes]::DarkGreen
                    $collectResultsBtn.IsEnabled = $true
                    $collectStatusText.Text = "Job FULFILLED! Click 'Collect All Results' to page through and load all conversations."
                    Append-JobLog "Job complete. Ready to collect results."
                    Set-Status "Job $($script:currentJobId) fulfilled - click Collect."
                }
                else {
                    $jobStateLabel.Foreground = [System.Windows.Media.Brushes]::DarkRed
                    $collectStatusText.Text = "Job ended in state: $state. Submit a new job."
                    Append-JobLog "Job ended with non-success state: $state"
                    Set-Status "Job $state - check log."
                }
            }
            else {
                $jobStateLabel.Foreground = [System.Windows.Media.Brushes]::DarkOrange
            }
        }
        catch {
            $script:consecutivePollErrors++
            $failureText = Format-UiApiFailure -Exception $_.Exception
            Append-JobLog "Poll error ($($script:consecutivePollErrors)/$($script:maxConsecutivePollErrors)): $failureText"

            if ($script:consecutivePollErrors -ge $script:maxConsecutivePollErrors) {
                $script:pollTimer.Stop()
                $jobStateLabel.Text = 'ERROR'
                $jobStateLabel.Foreground = [System.Windows.Media.Brushes]::DarkRed
                $collectResultsBtn.IsEnabled = $false
                $collectStatusText.Text = "Polling stopped after $($script:maxConsecutivePollErrors) consecutive errors. Review the activity log."
                Set-Status 'Polling stopped - too many consecutive errors.'
            }
        }
    })

# -----------------------------------------------------------------------------
# Event handlers
# -----------------------------------------------------------------------------

# -- Auth ----------------------------------------------------------------------

$authButton.Add_Click({
        $region = ([string]$regionComboBox.Text).Trim()
        if ([string]::IsNullOrWhiteSpace($region)) { $region = $DefaultRegion }

        if (-not (Test-RegionValue -Region $region)) {
            [System.Windows.MessageBox]::Show(
                "Invalid region '$region'.`nRegion must be a valid hostname such as 'usw2.pure.cloud' or 'mypurecloud.com'.",
                'Invalid Region', 'OK', 'Warning') | Out-Null
            return
        }

        $script:baseUri = "https://api.$region"

        $clientId = [string]$clientIdBox.Text
        $clientSecret = [string]$clientSecretBox.Password

        if ([string]::IsNullOrWhiteSpace($clientId) -or [string]::IsNullOrWhiteSpace($clientSecret)) {
            [System.Windows.MessageBox]::Show('Enter Client ID and Secret.', 'Authentication', 'OK', 'Warning') | Out-Null
            return
        }

        $authButton.IsEnabled = $false
        Set-Status 'Authenticating...'
        $authStatusLabel.Text = 'Authenticating...'
        $authStatusLabel.Foreground = [System.Windows.Media.Brushes]::DarkOrange

        try {
            $pair = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("${clientId}:${clientSecret}"))
            $authResult = Invoke-RestMethod -Uri "https://login.$region/oauth/token" -Method POST `
                -Headers @{ Authorization = "Basic $pair" } `
                -Body @{ grant_type = 'client_credentials' } -ErrorAction Stop

            $script:accessToken = $authResult.access_token
            $script:headers = @{ Authorization = "Bearer $($script:accessToken)" }

            # A new token may belong to a different org or region: drop cached names.
            $script:lookups = New-ConversationLookupTable
            $script:lookupsLoaded = $false
            $script:lookupVersion++
            Reset-AnalysisCaches

            $authStatusLabel.Text = 'Authenticated'
            $authStatusLabel.Foreground = [System.Windows.Media.Brushes]::DarkGreen
            Save-GenesysEnvConfig -Region $region -ClientId $clientId
            Set-Status "Authenticated - $region."
            Append-JobLog "Authenticated to $region."
        }
        catch {
            $authStatusLabel.Text = 'Failed'
            $authStatusLabel.Foreground = [System.Windows.Media.Brushes]::DarkRed
            Set-Status "Authentication failed."
            [System.Windows.MessageBox]::Show("Authentication failed:`n$($_.Exception.Message)", 'Auth Error', 'OK', 'Error') | Out-Null
        }
        finally {
            $authButton.IsEnabled = $true
        }
    })

# -- Date presets --------------------------------------------------------------

function Set-DatePreset {
    # Presets always apply the canonical full-day range: 00:00:00 start, 23:59:59 end.
    # Times are set unconditionally so each preset produces a deterministic interval
    # regardless of any value previously typed into the time boxes.
    param([DateTime]$Start, [DateTime]$End)
    $startDatePicker.SelectedDate = $Start
    $endDatePicker.SelectedDate = $End
    $startTimeTextBox.Text = '00:00:00'
    $endTimeTextBox.Text = '23:59:59'
}

(Get-Control 'PresetToday').Add_Click({
        $t = [DateTime]::Today; Set-DatePreset -Start $t -End $t
    })
(Get-Control 'PresetYesterday').Add_Click({
        $y = [DateTime]::Today.AddDays(-1); Set-DatePreset -Start $y -End $y
    })
(Get-Control 'PresetLast7').Add_Click({
        Set-DatePreset -Start ([DateTime]::Today.AddDays(-6)) -End ([DateTime]::Today)
    })
(Get-Control 'PresetLast30').Add_Click({
        Set-DatePreset -Start ([DateTime]::Today.AddDays(-29)) -End ([DateTime]::Today)
    })
(Get-Control 'PresetThisMonth').Add_Click({
        $now = [DateTime]::Today
        $s = [DateTime]::new($now.Year, $now.Month, 1)
        Set-DatePreset -Start $s -End $now
    })
(Get-Control 'PresetLastMonth').Add_Click({
        $now = [DateTime]::Today
        $s = [DateTime]::new($now.Year, $now.Month, 1).AddMonths(-1)
        $e = [DateTime]::new($now.Year, $now.Month, 1).AddDays(-1)
        Set-DatePreset -Start $s -End $e
    })

# -- Filter rows ---------------------------------------------------------------

$addConvFilterBtn.Add_Click({ New-FilterRow -Type 'conversation' -Panel $convFilterPanel | Out-Null })
$addSegFilterBtn.Add_Click({ New-FilterRow -Type 'segment' -Panel $segFilterPanel  | Out-Null })

$clearFiltersBtn.Add_Click({
        $convFilterPanel.Children.Clear()
        $segFilterPanel.Children.Clear()
        $directionCombo.SelectedIndex = 0
        $mediaTypeCombo.SelectedIndex = 0
        $requestEndpointBox.Text = ''
        $queryPreviewBox.Text = ''
    })

# -- Preview JSON --------------------------------------------------------------

$previewBtn.Add_Click({
        try {
            $preview = Build-QueryRequestPreview
            $requestEndpointBox.Text = $preview.EndpointUri
            $queryPreviewBox.Text = $preview.BodyJson
            Set-Status "Preview updated for $($preview.EndpointUri)"
        }
        catch {
            $err = $_
            $line = if ($err.InvocationInfo -and $err.InvocationInfo.ScriptLineNumber) {
                $err.InvocationInfo.ScriptLineNumber
            }
            else {
                'unknown'
            }

            $cmd = if ($err.InvocationInfo -and $err.InvocationInfo.Line) {
                $err.InvocationInfo.Line.Trim()
            }
            else {
                '<no line text>'
            }

            $pos = if ($err.InvocationInfo -and $err.InvocationInfo.PositionMessage) {
                $err.InvocationInfo.PositionMessage
            }
            else {
                '<no position info>'
            }

            $stack = if ($err.ScriptStackTrace) {
                $err.ScriptStackTrace
            }
            else {
                '<no script stack>'
            }

            $requestEndpointBox.Text = ''
            $queryPreviewBox.Text = @"
Error: $($err.Exception.Message)

Line: $line
Command: $cmd

Position:
$pos

Stack:
$stack
"@

            Set-Status "Preview failed at line $line"
        }
    })

# -- Submit job ----------------------------------------------------------------

$submitJobBtn.Add_Click({
        if ([string]::IsNullOrWhiteSpace($script:accessToken)) {
            [System.Windows.MessageBox]::Show('Please authenticate first.', 'Not Authenticated', 'OK', 'Warning') | Out-Null
            return
        }

        try {
            $preview = Build-QueryRequestPreview
            $body = $preview.Body
            $jsonBody = $preview.BodyJson

            $requestEndpointBox.Text = $preview.EndpointUri
            $queryPreviewBox.Text = $jsonBody

            Append-JobLog "Submitting job..."
            Append-JobLog "Endpoint: $($preview.EndpointUri)"
            Append-JobLog "Body: $jsonBody"
            Set-Status 'Submitting job...'

            $result = Submit-AnalyticsJob -JsonBody $jsonBody
            $jobId = [string]$result.jobId

            if ([string]::IsNullOrWhiteSpace($jobId)) { throw "No jobId in response." }

            $script:currentJobId = $jobId
            $script:currentJobInterval = [string]$body['interval']
            $script:pollCount = 0
            $script:consecutivePollErrors = 0
            $script:jobSubmitTime = [DateTime]::UtcNow

            $jobIdBox.Text = $jobId
            $jobStateLabel.Text = [string]$result.state
            $jobStateLabel.Foreground = [System.Windows.Media.Brushes]::DarkOrange
            $jobPollLabel.Text = '0'
            $jobElapsedLabel.Text = '00:00'
            $cancelJobBtn.IsEnabled = $true
            $collectResultsBtn.IsEnabled = $false
            $collectStatusText.Text = 'Job submitted. Polling for completion...'

            Append-JobLog "Job submitted: $jobId  (initial state: $($result.state))"
            Set-Status "Job $jobId submitted - polling..."

            # Switch to Job Monitor tab
            $mainTabControl.SelectedIndex = 1

            $script:pollTimer.Start()
        }
        catch {
            $failureText = Format-UiApiFailure -Exception $_.Exception
            [System.Windows.MessageBox]::Show("Submit failed:`n$failureText", 'Submit Error', 'OK', 'Error') | Out-Null
            Set-Status "Job submit failed."
            Append-JobLog "Submit error: $failureText"
        }
    })

# -- Cancel job ----------------------------------------------------------------

$cancelJobBtn.Add_Click({
        if ([string]::IsNullOrWhiteSpace($script:currentJobId)) { return }
        $r = [System.Windows.MessageBox]::Show(
            "Delete/cancel job $($script:currentJobId)?", 'Confirm Cancel', 'YesNo', 'Question')
        if ($r -ne 'Yes') { return }

        $script:pollTimer.Stop()
        $deleteSucceeded = Remove-AnalyticsJob -JobId $script:currentJobId
        $collectResultsBtn.IsEnabled = $false
        if ($deleteSucceeded) {
            Append-JobLog "Job $($script:currentJobId) cancelled/deleted."
            $jobStateLabel.Text = 'CANCELLED'
            $jobStateLabel.Foreground = [System.Windows.Media.Brushes]::DarkRed
            $cancelJobBtn.IsEnabled = $false
            $collectStatusText.Text = 'Job cancelled. Submit a new job.'
            Set-Status 'Job cancelled.'
        }
        else {
            $jobStateLabel.Text = 'DELETE FAILED'
            $jobStateLabel.Foreground = [System.Windows.Media.Brushes]::DarkRed
            $cancelJobBtn.IsEnabled = $true
            $collectStatusText.Text = 'Delete request failed. The remote job may still exist; review the activity log.'
            Set-Status 'Job delete failed - see log.'
            [System.Windows.MessageBox]::Show("Delete/cancel failed. The local poller was stopped, but the remote job may still exist.`nSee the activity log for details.", 'Cancel Error', 'OK', 'Warning') | Out-Null
        }
    })

# -- Collect results -----------------------------------------------------------

$collectResultsBtn.Add_Click({
        $collectResultsBtn.IsEnabled = $false
        Clear-ConversationStore
        $script:seenCursors = [System.Collections.Generic.HashSet[string]]::new()
        $cursor = $null
        $page = 0

        Append-JobLog "Collecting results from job $($script:currentJobId)..."
        Append-JobLog "  Guardrails: max $($script:maxPageCount) pages, cursor loop detection enabled."
        Set-Status 'Collecting results...'

        try {
            do {
                $page++

                # -- Max page guard --
                if ($page -gt $script:maxPageCount) {
                    Append-JobLog "Collection stopped: reached max page count ($($script:maxPageCount))."
                    [System.Windows.MessageBox]::Show(
                        "Collection stopped after $($script:maxPageCount) pages ($($script:allConversations.Count) conversations collected).`nThis is a safety limit. Results collected so far are available in the Results tab.",
                        'Page Limit Reached', 'OK', 'Warning') | Out-Null
                    break
                }

                Append-JobLog "  Page $page - cursor: $(if ($cursor) { $cursor.Substring(0, [Math]::Min(20,$cursor.Length)) + '...' } else { '(first)' })"

                $result = Get-AnalyticsJobResults -JobId $script:currentJobId -PageSize 1000 -Cursor $cursor
                $batch = @($result.conversations)
                Add-Conversations -Conversations $batch
                $cursor = [string]$result.cursor
                Append-JobLog "  Got $($batch.Count) - total so far: $($script:allConversations.Count)"
                Set-Status "Collecting... $($script:allConversations.Count) conversations so far (page $page)."

                # -- Cursor loop detection --
                if (-not [string]::IsNullOrWhiteSpace($cursor)) {
                    if (-not $script:seenCursors.Add($cursor)) {
                        Append-JobLog "Collection stopped: cursor loop detected (cursor repeated on page $page)."
                        [System.Windows.MessageBox]::Show(
                            "Collection stopped: the API returned a cursor that was already seen, indicating a loop.`n$($script:allConversations.Count) conversations collected so far are available in the Results tab.",
                            'Cursor Loop Detected', 'OK', 'Warning') | Out-Null
                        break
                    }
                }

                # Let the UI breathe between pages
                [System.Windows.Forms.Application]::DoEvents()
            } while (-not [string]::IsNullOrWhiteSpace($cursor))

            Append-JobLog "Collection complete. $($script:allConversations.Count) total conversations across $page pages."
            Set-Status "Collection complete: $($script:allConversations.Count) conversations."

            $script:dataSource = "Analytics job $($script:currentJobId)"
            $script:dataQueryInterval = $script:currentJobInterval
            if (-not $script:lookupsLoaded) { Update-ReferenceLookups | Out-Null }
            Show-Results
            $mainTabControl.SelectedIndex = 2
        }
        catch {
            $failureText = Format-UiApiFailure -Exception $_.Exception
            Append-JobLog "Collection error on page $($page): $failureText"
            Set-Status 'Collection error - see log.'
            [System.Windows.MessageBox]::Show("Collection failed on page $($page):`n$failureText", 'Error', 'OK', 'Error') | Out-Null

            # Show partial results if any were collected before the error
            if ($script:allConversations.Count -gt 0) {
                Append-JobLog "Showing $($script:allConversations.Count) partial results collected before failure."
                $script:dataSource = "Analytics job $($script:currentJobId) (partial: failed on page $page)"
                $script:dataQueryInterval = $script:currentJobInterval
                Show-Results
                $mainTabControl.SelectedIndex = 2
            }
        }
        finally {
            $script:seenCursors = $null
            $collectResultsBtn.IsEnabled = $true
        }
    })

# -- Results grid selection ----------------------------------------------------

$resultsGrid.Add_SelectionChanged({
        $row = $resultsGrid.SelectedItem
        if ($null -eq $row) { return }
        $convId = [string]$row.ConversationId
        $conv = Get-ConversationById -ConversationId $convId
        if ($null -eq $conv) { return }
        Show-ConversationDetail -Conv $conv
    })

# -- Column selector -----------------------------------------------------------

$columnSelectorBtn.Add_Click({
        # Plain scriptblock handlers (no GetNewClosure): while ShowDialog blocks, WPF events run
        # as child scopes of this handler, so they see its locals and the real $script: scope.
        $standardColumns = @(Get-FlatRowColumnNames)
        $attributeKeys = @(Get-ConversationAttributeKeys -Conversations $script:allConversations.ToArray())

        $popup = New-Object System.Windows.Window
        $popup.Title = 'Select Columns'
        $popup.Width = 560; $popup.Height = 640
        $popup.WindowStartupLocation = 'CenterOwner'; $popup.Owner = $window

        $outerGrid = New-Object System.Windows.Controls.Grid
        foreach ($height in @('Auto', '*', 'Auto')) {
            $rowDef = New-Object System.Windows.Controls.RowDefinition
            $rowDef.Height = if ($height -eq '*') { [System.Windows.GridLength]::new(1, 'Star') } else { [System.Windows.GridLength]::Auto }
            $outerGrid.RowDefinitions.Add($rowDef)
        }
        $popup.Content = $outerGrid

        $searchBox = New-Object System.Windows.Controls.TextBox
        $searchBox.Margin = [System.Windows.Thickness]::new(8, 8, 8, 4)
        $searchBox.ToolTip = 'Filter columns by name or description'
        [System.Windows.Controls.Grid]::SetRow($searchBox, 0); $outerGrid.Children.Add($searchBox) | Out-Null

        $tabs = New-Object System.Windows.Controls.TabControl
        $tabs.Margin = [System.Windows.Thickness]::new(8, 0, 8, 0)
        [System.Windows.Controls.Grid]::SetRow($tabs, 1); $outerGrid.Children.Add($tabs) | Out-Null

        $stdBoxes = [System.Collections.Generic.List[System.Windows.Controls.CheckBox]]::new()
        $attrBoxes = [System.Collections.Generic.List[System.Windows.Controls.CheckBox]]::new()

        # -- Standard columns tab
        $stdDock = New-Object System.Windows.Controls.DockPanel
        $stdButtons = New-Object System.Windows.Controls.WrapPanel
        [System.Windows.Controls.DockPanel]::SetDock($stdButtons, 'Top')
        $stdDock.Children.Add($stdButtons) | Out-Null
        $stdScroll = New-Object System.Windows.Controls.ScrollViewer; $stdScroll.VerticalScrollBarVisibility = 'Auto'
        $stdList = New-Object System.Windows.Controls.StackPanel; $stdList.Margin = [System.Windows.Thickness]::new(6)
        $stdScroll.Content = $stdList
        $stdDock.Children.Add($stdScroll) | Out-Null
        foreach ($name in $standardColumns) {
            $label = New-Object System.Windows.Controls.TextBlock
            $label.Inlines.Add((New-Object System.Windows.Documents.Run($name))) | Out-Null
            $descriptionRun = New-Object System.Windows.Documents.Run("  $(Get-ColumnDescription -Name $name)")
            $descriptionRun.Foreground = [System.Windows.Media.Brushes]::Gray
            $label.Inlines.Add($descriptionRun) | Out-Null
            $cb = New-Object System.Windows.Controls.CheckBox
            $cb.Content = $label; $cb.Tag = $name
            $cb.Margin = [System.Windows.Thickness]::new(0, 1, 0, 1)
            $cb.IsChecked = $script:visibleStdCols -contains $name
            $stdList.Children.Add($cb) | Out-Null
            $stdBoxes.Add($cb) | Out-Null
        }
        foreach ($spec in @(@('Defaults', 'defaults'), @('Select all shown', 'all'), @('Clear all shown', 'none'))) {
            $btn = New-Object System.Windows.Controls.Button
            $btn.Content = $spec[0]; $btn.Tag = $spec[1]
            $btn.Add_Click({
                    param($buttonSender)
                    $mode = [string]$buttonSender.Tag
                    $defaults = @(Get-DefaultGridColumnNames)
                    foreach ($box in $stdBoxes) {
                        if ($mode -eq 'defaults') { $box.IsChecked = $defaults -contains [string]$box.Tag }
                        elseif ($box.Visibility -eq 'Visible') { $box.IsChecked = ($mode -eq 'all') }
                    }
                })
            $stdButtons.Children.Add($btn) | Out-Null
        }
        $stdTab = New-Object System.Windows.Controls.TabItem
        $stdTab.Header = "Standard columns ($($standardColumns.Count))"
        $stdTab.Content = $stdDock
        $tabs.Items.Add($stdTab) | Out-Null

        # -- Attribute columns tab
        $attrScroll = New-Object System.Windows.Controls.ScrollViewer; $attrScroll.VerticalScrollBarVisibility = 'Auto'
        $attrList = New-Object System.Windows.Controls.StackPanel; $attrList.Margin = [System.Windows.Thickness]::new(6)
        $attrScroll.Content = $attrList
        if ($attributeKeys.Count -eq 0) {
            $none = New-Object System.Windows.Controls.TextBlock
            $none.Text = 'No participant attributes found. Load results first.'
            $none.Foreground = [System.Windows.Media.Brushes]::Gray
            $attrList.Children.Add($none) | Out-Null
        }
        foreach ($key in $attributeKeys) {
            $cb = New-Object System.Windows.Controls.CheckBox
            $cb.Content = $key; $cb.Tag = $key
            $cb.Margin = [System.Windows.Thickness]::new(0, 1, 0, 1)
            $cb.IsChecked = $script:selectedAttrCols -contains $key
            $attrList.Children.Add($cb) | Out-Null
            $attrBoxes.Add($cb) | Out-Null
        }
        $attrTab = New-Object System.Windows.Controls.TabItem
        $attrTab.Header = "Attributes ($($attributeKeys.Count))"
        $attrTab.Content = $attrScroll
        $tabs.Items.Add($attrTab) | Out-Null

        # Filter by hiding non-matching boxes (keeps checked state intact)
        $searchBox.Add_TextChanged({
                $filter = $searchBox.Text
                foreach ($box in @($stdBoxes) + @($attrBoxes)) {
                    $haystack = [string]$box.Tag
                    if ($box.Content -is [System.Windows.Controls.TextBlock]) { $haystack = $haystack + ' ' + (Get-ColumnDescription -Name ([string]$box.Tag)) }
                    $box.Visibility = if ([string]::IsNullOrWhiteSpace($filter) -or $haystack -like "*$filter*") { 'Visible' } else { 'Collapsed' }
                }
            })

        $btnRow = New-Object System.Windows.Controls.StackPanel
        $btnRow.Orientation = 'Horizontal'; $btnRow.HorizontalAlignment = 'Right'
        $btnRow.Margin = [System.Windows.Thickness]::new(8)
        [System.Windows.Controls.Grid]::SetRow($btnRow, 2); $outerGrid.Children.Add($btnRow) | Out-Null

        $applyBtn = New-Object System.Windows.Controls.Button; $applyBtn.Content = 'Apply & Refresh'; $applyBtn.Width = 120
        $applyBtn.Add_Click({
                $script:visibleStdCols.Clear()
                foreach ($box in $stdBoxes) { if ($box.IsChecked) { $script:visibleStdCols.Add([string]$box.Tag) | Out-Null } }
                $script:selectedAttrCols.Clear()
                foreach ($box in $attrBoxes) { if ($box.IsChecked) { $script:selectedAttrCols.Add([string]$box.Tag) | Out-Null } }
                $popup.Close()
                if ($script:allConversations.Count -gt 0) { Show-Results }
            })
        $btnRow.Children.Add($applyBtn) | Out-Null

        $cancelBtn = New-Object System.Windows.Controls.Button; $cancelBtn.Content = 'Cancel'; $cancelBtn.Width = 70; $cancelBtn.Margin = [System.Windows.Thickness]::new(4, 0, 0, 0)
        $cancelBtn.Add_Click({ $popup.Close() })
        $btnRow.Children.Add($cancelBtn) | Out-Null

        $popup.ShowDialog() | Out-Null
    })
# -- Export CSV ----------------------------------------------------------------

$exportCsvBtn.Add_Click({
        if ($script:allConversations.Count -eq 0) {
            [System.Windows.MessageBox]::Show('No results to export.', 'Export', 'OK', 'Information') | Out-Null; return
        }
        $dlg = New-Object System.Windows.Forms.SaveFileDialog
        $dlg.Filter = 'CSV files (*.csv)|*.csv'
        $dlg.FileName = "conversations-$(Get-Date -Format 'yyyyMMdd-HHmmss').csv"
        if ($dlg.ShowDialog() -ne 'OK') { return }

        try {
            # Every standard column (not just the visible ones) plus selected attribute columns.
            $attrCols = @($script:selectedAttrCols)
            $sensitiveColumns = @((Get-FlatRowColumnNames) + @($attrCols | ForEach-Object { "A:$_" }) | Where-Object { Test-SensitiveKey -Key $_ })
            $exported = 0
            $script:allConversations.ToArray() |
                ForEach-Object {
                    $row = ConvertTo-FlatRow -ConversationProfile (Get-CachedConversationProfile -Conv $_) -Conversation $_ -AttrCols $attrCols -Lookups $script:lookups
                    if ($script:exportRedactionMode) {
                        foreach ($columnName in $sensitiveColumns) {
                            $row.$columnName = Protect-ScalarValue -Value $row.$columnName -Key $columnName
                        }
                    }
                    $exported++
                    if ($exported % 1000 -eq 0) { Set-Status "Exporting CSV... $exported rows"; [System.Windows.Forms.Application]::DoEvents() }
                    $row
                } |
                Export-Csv -Path $dlg.FileName -NoTypeInformation -Encoding UTF8
            Set-Status "Exported $($script:allConversations.Count) rows to $($dlg.FileName)"
            [System.Windows.MessageBox]::Show("Exported $($script:allConversations.Count) conversations to:`n$($dlg.FileName)", 'Export Complete', 'OK', 'Information') | Out-Null
        }
        catch {
            [System.Windows.MessageBox]::Show("Export failed:`n$($_.Exception.Message)", 'Export Error', 'OK', 'Error') | Out-Null
        }
    })

# -- Export JSONL --------------------------------------------------------------

$exportJsonlBtn.Add_Click({
        if ($script:allConversations.Count -eq 0) {
            [System.Windows.MessageBox]::Show('No results to export.', 'Export', 'OK', 'Information') | Out-Null; return
        }
        $dlg = New-Object System.Windows.Forms.SaveFileDialog
        $dlg.Filter = 'JSONL files (*.jsonl)|*.jsonl|JSON files (*.json)|*.json'
        $dlg.FileName = "conversations-$(Get-Date -Format 'yyyyMMdd-HHmmss').jsonl"
        if ($dlg.ShowDialog() -ne 'OK') { return }

        $writer = $null
        try {
            $writer = [System.IO.StreamWriter]::new($dlg.FileName, $false, [System.Text.Encoding]::UTF8)
            foreach ($conv in @($script:allConversations)) {
                $writer.WriteLine(((Get-ExportConversation -Conversation $conv) | ConvertTo-Json -Depth 20 -Compress))
            }
            Set-Status "Exported $($script:allConversations.Count) conversations (JSONL) to $($dlg.FileName)"
            [System.Windows.MessageBox]::Show("Exported $($script:allConversations.Count) conversations to:`n$($dlg.FileName)", 'Export Complete', 'OK', 'Information') | Out-Null
        }
        catch {
            [System.Windows.MessageBox]::Show("Export failed:`n$($_.Exception.Message)", 'Export Error', 'OK', 'Error') | Out-Null
        }
        finally {
            if ($null -ne $writer) { $writer.Dispose() }
        }
    })

# -- Load from JSONL -----------------------------------------------------------

$loadJsonlBtn.Add_Click({
        $dlg = New-Object System.Windows.Forms.OpenFileDialog
        $dlg.Filter = 'JSONL files (*.jsonl)|*.jsonl|JSON files (*.json)|*.json|All files (*.*)|*.*'
        $dlg.Title = 'Load Conversation JSONL'
        if ($dlg.ShowDialog() -ne 'OK') { return }

        try {
            Clear-ConversationStore
            Set-Status "Loading $($dlg.FileName)..."

            $progressAction = {
                param([hashtable]$Info)
                Set-Status "Loading... $($Info.Count) conversations read (line $($Info.LineNumber))."
                [System.Windows.Forms.Application]::DoEvents()
            }.GetNewClosure()

            $loadResult = Read-ConversationsFromFile -Path $dlg.FileName -OnProgress $progressAction
            Add-Conversations -Conversations $loadResult.Conversations

            # Report any per-line parse errors
            if ($loadResult.ErrorCount -gt 0) {
                $preview = ($loadResult.Errors | Select-Object -First 5) -join "`n"
                $moreNote = if ($loadResult.ErrorCount -gt 5) { "`n... and $($loadResult.ErrorCount - 5) more." } else { '' }
                [System.Windows.MessageBox]::Show(
                    "$($loadResult.ErrorCount) lines could not be parsed and were skipped:`n`n$preview$moreNote",
                    'Partial Load Warnings', 'OK', 'Warning') | Out-Null
            }

            $script:dataSource = "File $([System.IO.Path]::GetFileName($dlg.FileName))"
            $script:dataQueryInterval = ''
            if (-not $script:lookupsLoaded -and -not [string]::IsNullOrWhiteSpace($script:accessToken)) { Update-ReferenceLookups | Out-Null }
            Show-Results
            $mainTabControl.SelectedIndex = 2

            $errorSuffix = if ($loadResult.ErrorCount -gt 0) { " ($($loadResult.ErrorCount) lines skipped)" } else { '' }
            Set-Status "Loaded $($script:allConversations.Count) conversations from file.$errorSuffix"
        }
        catch {
            [System.Windows.MessageBox]::Show("Load failed:`n$($_.Exception.Message)", 'Load Error', 'OK', 'Error') | Out-Null
            Set-Status 'Load failed.'
        }
    })

# -- Clear results -------------------------------------------------------------

$clearResultsBtn.Add_Click({
        Clear-ConversationStore
        $resultsGrid.ItemsSource = $null
        $resultsGrid.Columns.Clear()
        $summaryText.Text = 'Results cleared.'
        $overviewPanel.Children.Clear()
        foreach ($detailGrid in @($attributesGrid, $participantsGrid, $segmentsGrid, $metricsGrid, $flowsGrid)) { Set-GridRows -Grid $detailGrid -Rows $null }
        $rawJsonBox.Text = ''
        $script:dataSource = ''
        $script:dataQueryInterval = ''
        Clear-ReportView
        Set-Status 'Results cleared.'
    })

# -- Resolve names / report ------------------------------------------------------

$resolveNamesBtn.Add_Click({
        if ([string]::IsNullOrWhiteSpace($script:accessToken)) {
            [System.Windows.MessageBox]::Show('Authenticate first - names are looked up from your Genesys Cloud org.', 'Resolve Names', 'OK', 'Information') | Out-Null
            return
        }
        $resolveNamesBtn.IsEnabled = $false
        try {
            $loaded = Update-ReferenceLookups -Force
            if ($script:allConversations.Count -gt 0) { Show-Results }
            if (-not $loaded) {
                [System.Windows.MessageBox]::Show('No reference names could be loaded. See the Job Monitor activity log for the API errors (often a missing permission).', 'Resolve Names', 'OK', 'Warning') | Out-Null
            }
        }
        finally { $resolveNamesBtn.IsEnabled = $true }
    })

$refreshReportBtn.Add_Click({
        if ($script:allConversations.Count -eq 0) {
            [System.Windows.MessageBox]::Show('No results loaded.', 'Report', 'OK', 'Information') | Out-Null
            return
        }
        Update-ReportView -Force
        Set-Status 'Report refreshed.'
    })

$exportReportBtn.Add_Click({ Export-ReportFile })

# -----------------------------------------------------------------------------
# Startup: load persisted config + auto-auth
# -----------------------------------------------------------------------------

$startDatePicker.SelectedDate = [DateTime]::Today
$endDatePicker.SelectedDate = [DateTime]::Today
$startTimeTextBox.Text = '00:00:00'
$endTimeTextBox.Text = '23:59:59'
Set-IntervalControlDefaults

$cfg = Read-GenesysEnvConfig
if ($null -ne $cfg) {
    $cfgRegion = Get-ConfigString -ConfigObject $cfg -PropertyNames @('region')
    if (-not [string]::IsNullOrWhiteSpace($cfgRegion)) { $regionComboBox.Text = $cfgRegion }

    $cfgClientId = Get-ConfigString -ConfigObject $cfg -PropertyNames @('clientId', 'client_id')
    if (-not [string]::IsNullOrWhiteSpace($cfgClientId)) { $clientIdBox.Text = $cfgClientId }
}

# Env vars override config
if (-not [string]::IsNullOrWhiteSpace($env:GENESYS_CLIENT_ID)) { $clientIdBox.Text = $env:GENESYS_CLIENT_ID }
if (-not [string]::IsNullOrWhiteSpace($env:GENESYS_CLIENT_SECRET)) { $clientSecretBox.Password = $env:GENESYS_CLIENT_SECRET }

if (-not [string]::IsNullOrWhiteSpace([string]$clientIdBox.Text) -and -not [string]::IsNullOrWhiteSpace([string]$clientSecretBox.Password)) {
    $authButton.RaiseEvent([System.Windows.RoutedEventArgs]::new([System.Windows.Controls.Button]::ClickEvent))
}

# -----------------------------------------------------------------------------
# Show
# -----------------------------------------------------------------------------

$window.ShowDialog() | Out-Null
