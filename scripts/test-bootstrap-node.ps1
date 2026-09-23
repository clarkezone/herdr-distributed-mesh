#requires -Version 7.4
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
. (Join-Path $PSScriptRoot 'bootstrap-node.ps1')

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
}

function Assert-Equal($Expected, $Actual, [string]$Message) {
    if ($Expected -cne $Actual) { throw "$Message Expected '$Expected', got '$Actual'." }
}

$testRoot = Join-Path ([IO.Path]::GetTempPath()) "herdr-bootstrap-test-$([Guid]::NewGuid().ToString('N'))"
$script:mockCalls = [Collections.Generic.List[object]]::new()
$script:mockJobs = [Collections.Generic.List[object]]::new()
$script:mockMode = ''
$script:cancelSource = $null
$script:stoppedJobs = 0
$script:removedJobs = 0
$script:tests = 0
$script:latestFailure = $null
$script:runner = $null
$script:runnerMode = ''
$script:runnerStartCalls = 0
$script:runnerQueries = [Collections.Generic.List[string]]::new()
$script:runnerObservationQueries = 0
$script:taskToolsUnavailable = $false
$script:remoteAction = ''
$script:vanishingHerdr = ''
$script:sshUnavailable = $false

# The OOB and scheduler cmdlets are faked. The real orchestration, fixed remote program,
# streaming ZIP validation, ACL/path checks, upload and rename run on local fixtures.
function Invoke-Command {
    [CmdletBinding()]
    param($Session, [scriptblock]$ScriptBlock, [object[]]$ArgumentList, [switch]$AsJob)
    Assert-Equal 'fixture-session' $Session 'Unexpected fake session.'
    Assert-True $AsJob.IsPresent 'Remote invocation must use a cancellable job.'
    Assert-Equal $script:HerdrBootstrapProgram.ToString() $ScriptBlock.ToString() 'Remote script was not fixed.'
    Assert-Equal 2 $ArgumentList.Count 'Unexpected remote arguments.'
    $action = $ArgumentList[0]
    Assert-True ($action -in @('prepare', 'upload', 'commit', 'start')) 'Unbounded remote operation.'
    $script:remoteAction = $action
    $request = ConvertFrom-Json $ArgumentList[1] -AsHashtable
    Assert-True (-not $request.ContainsKey('ArchivePath')) 'Local archive path leaked into remote request.'
    $script:mockCalls.Add([pscustomobject]@{ Action = $action; Request = $request })
    if ($script:mockMode -eq 'prepare-fail') { throw 'untrusted remote error SECRET-MARKER' }
    if ($script:mockMode -eq 'unknown-error') { throw 'BOOTSTRAP_SECRET_MARKER SECRET-MARKER' }
    if ($script:mockMode -eq 'upload-fail' -and $action -eq 'upload') { throw 'SECRET-MARKER' }
    if ($script:mockMode -eq 'commit-fail' -and $action -eq 'commit') { throw 'SECRET-MARKER' }
    if ($script:mockMode -eq 'cancel-upload' -and $action -eq 'upload') { $script:cancelSource.Cancel() }
    if ($script:mockMode -eq 'lock-source' -and $action -eq 'prepare') {
        $blocked = $false
        try {
            $file = [IO.File]::Open($script:archive, [IO.FileMode]::Open, [IO.FileAccess]::Write, [IO.FileShare]::ReadWrite)
            $file.Dispose()
        } catch [IO.IOException] { $blocked = $true }
        Assert-True $blocked 'Source was writable between validation and upload.'
    }
    if ($script:mockMode -eq 'tamper-upload' -and $action -eq 'upload' -and $request.Offset -eq 0) {
        $bytes = [Convert]::FromBase64String($request.Data)
        $bytes[100] = $bytes[100] -bxor 1
        $request.Data = [Convert]::ToBase64String($bytes)
    }
    if ($script:mockMode -eq 'bad-offset' -and $action -eq 'upload' -and $request.Offset -gt 0) {
        $request.Offset = 0
    }
    if ($script:mockMode -eq 'big-chunk' -and $action -eq 'upload') {
        $request.Data = 'A' * (192KB / 3 * 4 + 4)
    }
    if ($script:mockMode -eq 'invalid-base64' -and $action -eq 'upload') { $request.Data = '!!!!' }
    if ($script:mockMode -eq 'publish-race' -and $action -eq 'commit') {
        [IO.Directory]::CreateDirectory($request.InstallDirectory) | Out-Null
        [IO.File]::WriteAllText((Join-Path $request.InstallDirectory 'existing.txt'), 'do-not-overwrite')
    }
    if (($script:mockMode -eq 'task-change-before-commit' -and $action -eq 'commit') -or
        ($script:mockMode -eq 'task-change-before-start' -and $action -eq 'start')) {
        $script:runner.Actions[0].Arguments = 'SECRET-MARKER'
    }
    if ($script:mockMode -eq 'race-running' -and $action -eq 'start') { $script:runner.State = 'Running' }
    if ($script:mockMode -eq 'tamper-installed' -and $action -eq 'start') {
        [IO.File]::WriteAllText((Join-Path $request.InstallDirectory 'herdr-mesh.exe'), 'not the verified binary')
    }
    if ($script:mockMode -eq 'runner-short-budget' -and $action -eq 'start') {
        $request.RemainingMilliseconds = 200
    }
    if (($script:mockMode -eq 'herdr-missing-before-commit' -and $action -eq 'commit') -or
        ($script:mockMode -eq 'herdr-missing-before-start' -and $action -eq 'start')) {
        Assert-True ($request.HerdrExecutable.StartsWith("$testRoot\", [StringComparison]::OrdinalIgnoreCase)) 'Herdr fixture escaped test root.'
        [IO.File]::Delete($request.HerdrExecutable)
    }
    $result = $null
    $errorText = ''
    $running = $script:mockMode -eq 'timeout' -or ($script:mockMode -eq 'cancel-upload' -and $action -eq 'upload')
    if (-not $running) {
        try {
            $result = & $ScriptBlock $action (ConvertTo-Json $request -Compress)
            if ($script:mockMode -eq 'commit-lost' -and $action -eq 'commit') {
                throw 'SECRET-MARKER commit acknowledgement lost'
            }
            if ($script:mockMode -eq 'start-lost' -and $action -eq 'start') { throw 'SECRET-MARKER start acknowledgement lost' }
            if (($script:mockMode -eq 'cancel-before-start' -and $action -eq 'commit') -or
                ($script:mockMode -eq 'cancel-after-start' -and $action -eq 'start')) { $script:cancelSource.Cancel() }
            if ($script:mockMode -eq 'start-timeout' -and $action -eq 'start') { $running = $true }
            if ($script:mockMode -eq 'start-bad-reply' -and $action -eq 'start') { $result.Connectivity = 'ready' }
            if ($script:mockMode -eq 'start-extra-property' -and $action -eq 'start') {
                $result | Add-Member -NotePropertyName UntrustedExtra -NotePropertyValue 'SECRET-MARKER'
            }
            if ($script:mockMode -eq 'bad-reply' -and $action -eq 'commit') { $result.Startup = 'running' }
            if ($script:mockMode -eq 'bad-arguments' -and $action -eq 'commit') { $result.NodeArguments += 'SECRET-MARKER' }
            if ($script:mockMode -eq 'bad-herdr-reply' -and $action -eq 'commit') {
                $result.NodeArguments[6] = Join-Path $testRoot 'other-herdr.exe'
            }
            if ($script:mockMode -eq 'extra-property' -and $action -eq 'commit') {
                $result | Add-Member -NotePropertyName UntrustedExtra -NotePropertyValue 'SECRET-MARKER'
            }
            if ($script:mockMode -eq 'extra-output') { $result = @($result, 'untrusted output') }
        } catch {
            $errorText = $_.Exception.Message
            $cause = $_.Exception.GetBaseException()
            if ($cause -is [IO.IOException] -or $cause -is [UnauthorizedAccessException]) {
                Write-Warning "Disposable fixture filesystem failure during ${action}: $($cause.GetType().Name), HRESULT $($cause.HResult)."
            }
        }
    }
    $state = if ($running) { 'Running' } elseif ($errorText) { 'Failed' } else { 'Completed' }
    $job = [pscustomobject]@{
        State = $state
        Finished = [Threading.ManualResetEvent]::new(-not $running)
        Result = $result
        ErrorText = $errorText
    }
    $script:mockJobs.Add($job)
    return $job
}

function Receive-Job {
    [CmdletBinding()]
    param($Job)
    if ($Job.ErrorText) { throw $Job.ErrorText }
    return $Job.Result
}

function Stop-Job {
    [CmdletBinding()]
    param($Job)
    $script:stoppedJobs++
    $Job.State = 'Stopped'
}

function Remove-Job {
    [CmdletBinding()]
    param($Job)
    $script:removedJobs++
    $Job.Finished.Dispose()
}

function Get-Command {
    [CmdletBinding()]
    param([string[]]$Name, [string]$CommandType)
    if ($Name.Count -eq 1 -and $Name[0] -eq 'ssh') {
        if ($script:sshUnavailable) { throw 'SECRET-MARKER missing ssh' }
        return [pscustomobject]@{ Name = 'ssh'; CommandType = 'Application' }
    }
    if ($script:taskToolsUnavailable -and 'Get-ScheduledTask' -in $Name) { throw 'SECRET-MARKER missing tools' }
    Microsoft.PowerShell.Core\Get-Command @PSBoundParameters
}

function Get-ScheduledTask {
    [CmdletBinding()]
    param([string]$TaskName, [string]$TaskPath)
    Assert-Equal 'Mesh Node' $TaskName 'Task query must name exactly the fixture task.'
    Assert-Equal '\Fixture\' $TaskPath 'Task query must not enumerate other folders.'
    $script:runnerQueries.Add($script:remoteAction)
    if ($script:runnerMode -eq 'unavailable') { throw 'SECRET-MARKER denied or absent task' }
    if ($null -eq $script:runner) { return }
    if (($script:runnerMode -eq 'herdr-missing-before-publication' -and $script:remoteAction -eq 'commit' -and
            @($script:runnerQueries | Where-Object { $_ -eq 'commit' }).Count -eq 2) -or
        ($script:runnerMode -eq 'herdr-missing-before-dispatch' -and $script:remoteAction -eq 'start')) {
        Assert-True ($script:vanishingHerdr.StartsWith("$testRoot\", [StringComparison]::OrdinalIgnoreCase)) 'Herdr fixture escaped test root.'
        [IO.File]::Delete($script:vanishingHerdr)
    }
    if ($script:runnerMode -eq 'change-before-publication' -and
        @($script:runnerQueries | Where-Object { $_ -eq 'commit' }).Count -eq 2) {
        $script:runner.Actions[0].Execute = 'SECRET-MARKER.exe'
    }
    if ($script:remoteAction -eq 'start') {
        $script:runnerObservationQueries++
        if ($script:runnerMode -eq 'eventually-running' -and $script:runnerObservationQueries -ge 3) {
            $script:runner.State = 'Running'
        }
        if ($script:runnerMode -eq 'running-exits' -and $script:runnerObservationQueries -ge 2) {
            $script:runner.State = 'Ready'
        }
    }
    # A new snapshot on every query, not a mutable reference held by the caller.
    return $script:runner | ConvertTo-Json -Depth 8 | ConvertFrom-Json
}

function Start-ScheduledTask {
    [CmdletBinding()]
    param($InputObject)
    $script:runnerStartCalls++
    Assert-Equal 'Mesh Node' $InputObject.TaskName 'Start must use the validated task object.'
    Assert-Equal '\Fixture\' $InputObject.TaskPath 'Start selected the wrong folder.'
    Assert-Equal 'IgnoreNew' $InputObject.Settings.MultipleInstances 'Start bypassed duplicate policy.'
    Assert-True ([IO.File]::Exists($InputObject.Actions[0].Execute)) 'Start occurred before binary publication.'
    if ($script:runnerMode -eq 'start-fail') { throw 'SECRET-MARKER start rejected' }
    $script:runner.State = 'Running'
    if ($script:runnerMode -eq 'start-ack-lost') { throw 'SECRET-MARKER scheduler acknowledgement lost' }
    if ($script:runnerMode -eq 'never-running') { $script:runner.State = 'Ready' }
    if ($script:runnerMode -eq 'eventually-running') { $script:runner.State = 'Queued' }
    if ($script:runnerMode -eq 'change-after-start') { $script:runner.Actions[0].Arguments = 'SECRET-MARKER' }
}

function New-FixtureArchive {
    param(
        [string]$Name,
        [string[]]$Entries = @('herdr-mesh.exe'),
        [byte[]]$Payload = $script:payload,
        [int]$Attributes = 0
    )
    $path = Join-Path $testRoot $Name
    $file = [IO.File]::Open($path, [IO.FileMode]::CreateNew)
    $zip = [IO.Compression.ZipArchive]::new($file, [IO.Compression.ZipArchiveMode]::Create, $true)
    try {
        foreach ($name in $Entries) {
            $entry = $zip.CreateEntry($name, [IO.Compression.CompressionLevel]::NoCompression)
            $entry.ExternalAttributes = $Attributes
            $stream = $entry.Open()
            try { $stream.Write($Payload, 0, $Payload.Length) } finally { $stream.Dispose() }
        }
    } finally {
        $zip.Dispose()
        $file.Dispose()
    }
    return $path
}

function New-Options([string]$Name) {
    $script:mockCalls.Clear()
    $script:mockJobs.Clear()
    $script:mockMode = ''
    $script:runner = $null
    $script:runnerMode = ''
    $script:runnerStartCalls = 0
    $script:runnerQueries.Clear()
    $script:runnerObservationQueries = 0
    $script:taskToolsUnavailable = $false
    $script:vanishingHerdr = ''
    return @{
        Session = 'fixture-session'
        ArchivePath = $script:archive
        ExpectedSHA256 = (Get-FileHash -LiteralPath $script:archive -Algorithm SHA256).Hash
        Architecture = $script:architecture
        InstallDirectory = Join-Path $testRoot $Name
        StateDirectory = $script:state
        Server = 'coordinator.example.ts.net:50052'
        TimeoutSeconds = 30
    }
}

function New-RunnerOptions([string]$Name, [string]$HerdrExecutable) {
    $options = New-Options $Name
    if ($PSBoundParameters.ContainsKey('HerdrExecutable')) { $options.HerdrExecutable = $HerdrExecutable }
    $options.Start = $true
    $options.ExistingTaskName = '\Fixture\Mesh Node'
    $script:runner = [pscustomobject]@{
        TaskName = 'Mesh Node'
        TaskPath = '\Fixture\'
        State = 'Ready'
        Actions = @([pscustomobject]@{
            CimClass = [pscustomobject]@{ CimClassName = 'MSFT_TaskExecAction' }
            Execute = Join-Path $options.InstallDirectory 'herdr-mesh.exe'
            Arguments = 'node -server "' + $options.Server + '" -state-dir "' + $options.StateDirectory + '"'
            WorkingDirectory = ''
        })
        Settings = [pscustomobject]@{
            MultipleInstances = 'IgnoreNew'
            Enabled = $true
            AllowDemandStart = $true
            ExecutionTimeLimit = 'PT0S'
        }
        Principal = [pscustomobject]@{ LogonType = 'Password' }
    }
    if ($HerdrExecutable) { $script:runner.Actions[0].Arguments += ' -herdr-executable "' + $HerdrExecutable + '"' }
    return $options
}

function Assert-Fails {
    param([hashtable]$Options, [string]$Code, [string]$Outcome, [string]$Phase)
    $script:latestFailure = $null
    try { $null = Invoke-HerdrBootstrapCore @Options }
    catch { $script:latestFailure = $_.Exception }
    Assert-True ($null -ne $script:latestFailure) "Expected failure $Code."
    Assert-True ($script:latestFailure.Message.Contains($Code)) "Fixture $([IO.Path]::GetFileName($Options.InstallDirectory)): expected $Code; got $($script:latestFailure.Message)"
    Assert-True (-not $script:latestFailure.ToString().Contains('SECRET-MARKER')) 'Raw remote diagnostics leaked.'
    Assert-Equal $Outcome $script:latestFailure.Data['BootstrapOutcome'] 'Incorrect lifecycle uncertainty.'
    if ($Phase) { Assert-Equal $Phase $script:latestFailure.Data['BootstrapPhase'] 'Incorrect failure phase.' }
    $script:tests++
}

try {
    Assert-True ([OperatingSystem]::IsWindows()) 'These focused filesystem/ACL fixtures require Windows.'
    [IO.Directory]::CreateDirectory($testRoot) | Out-Null
    $acl = [Security.AccessControl.DirectorySecurity]::new()
    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User
    $acl.SetOwner($sid)
    $acl.SetAccessRuleProtection($true, $false)
    foreach ($identity in @($sid, [Security.Principal.SecurityIdentifier]::new('S-1-5-18'))) {
        $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($identity, 'FullControl',
            'ContainerInherit, ObjectInherit', 'None', 'Allow'))
    }
    Set-Acl -LiteralPath $testRoot -AclObject $acl
    $script:state = Join-Path $testRoot 'persistent-state'
    [IO.Directory]::CreateDirectory($script:state) | Out-Null
    [IO.Directory]::CreateDirectory((Join-Path $script:state 'nested')) | Out-Null
    $sentinel = Join-Path $script:state 'nested\fixture-state.dat'
    [IO.File]::WriteAllText($sentinel, 'disposable identity/journal stand-in; not a real enrollment')
    $beforeHash = (Get-FileHash -LiteralPath $sentinel).Hash
    $beforeWrite = [IO.File]::GetLastWriteTimeUtc($sentinel)
    $beforeACL = (Get-Acl -LiteralPath $script:state).Sddl
    $beforeFiles = @(Get-ChildItem -LiteralPath $script:state -Recurse | ForEach-Object FullName) -join "`n"
    $script:architecture = if ([Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString() -eq 'Arm64') { 'arm64' } else { 'amd64' }
    # Deliberately non-runnable fixture: only the checked PE header is meaningful.
    $script:payload = [byte[]]::new(600KB)
    $random = [Security.Cryptography.RandomNumberGenerator]::Create()
    try { $random.GetBytes($script:payload) } finally { $random.Dispose() }
    $script:payload[0] = 0x4D
    $script:payload[1] = 0x5A
    [BitConverter]::GetBytes([int]128).CopyTo($script:payload, 60)
    [BitConverter]::GetBytes([uint32]0x4550).CopyTo($script:payload, 128)
    $machine = if ($script:architecture -eq 'arm64') { 0xAA64 } else { 0x8664 }
    [BitConverter]::GetBytes([uint16]$machine).CopyTo($script:payload, 132)
    [BitConverter]::GetBytes([uint16]0x20B).CopyTo($script:payload, 152)
    $script:archive = New-FixtureArchive "release [literal] ' `$ ;.zip"

    $options = New-Options 'release one'
    $result = Invoke-HerdrBootstrapCore @options
    Assert-Equal 'staged-not-started' $result.Status 'Must not claim a running node.'
    Assert-Equal 'not-attempted' $result.Startup 'Unexpected startup.'
    Assert-Equal 'not-checked' $result.Enrollment 'Must not claim enrollment.'
    Assert-Equal 'not-checked' $result.Connectivity 'Must not claim connectivity.'
    Assert-True $result.RunnerRequired 'Runner prerequisite missing.'
    Assert-Equal 5 $result.NodeArguments.Count 'Only fixed node arguments are supported.'
    Assert-Equal 'node|-server|coordinator.example.ts.net:50052|-state-dir|persistent-state' `
        (($result.NodeArguments[0..3] + 'persistent-state') -join '|') 'Arguments changed.'
    Assert-Equal $script:state $result.NodeArguments[4] 'State path must remain a single argument.'
    Assert-Equal 3 @(Get-ChildItem -LiteralPath $options.InstallDirectory).Count 'Unexpected installed artifacts.'
    $installedBinary = Join-Path $options.InstallDirectory 'herdr-mesh.exe'
    Assert-Equal $result.BinarySHA256 (Get-FileHash -LiteralPath $installedBinary).Hash.ToLowerInvariant() 'Extracted hash mismatch.'
    $persisted = Get-Content -LiteralPath (Join-Path $options.InstallDirectory 'bootstrap-receipt.json') -Raw | ConvertFrom-Json
    Assert-Equal $result.OperationId $persisted.OperationId 'Receipt was not persisted.'
    Assert-Equal $result.Status $persisted.Status 'Persisted lifecycle was not truthful.'
    $uploads = @($script:mockCalls | Where-Object Action -eq 'upload')
    Assert-True ($uploads.Count -ge 3) 'Fixture did not test multiple chunks.'
    foreach ($call in $uploads) {
        Assert-True ([Convert]::FromBase64String($call.Request.Data).Length -le 192KB) 'Chunk exceeded cap.'
    }
    Assert-True (-not [IO.Directory]::Exists((Join-Path $testRoot ".herdr-bootstrap-$($result.OperationId)"))) 'Staging was not renamed.'
    $script:tests++

    $existingOptions = $options.Clone()
    Assert-Fails $existingOptions 'BOOTSTRAP_EXISTS' 'indeterminate' 'prepare'
    Assert-Equal $result.BinarySHA256 (Get-FileHash -LiteralPath $installedBinary).Hash.ToLowerInvariant() 'Existing install was changed.'
    $options = New-Options 'side-by-side'
    $script:mockMode = 'lock-source'
    $null = Invoke-HerdrBootstrapCore @options
    Assert-True ([IO.File]::Exists($installedBinary)) 'Side-by-side install removed previous version.'
    $script:tests++

    # Exercise the exact ZIP packaging command used by build-release.ps1.
    $releaseSource = Join-Path $testRoot 'release-source'
    [IO.Directory]::CreateDirectory($releaseSource) | Out-Null
    $sourceBinary = Join-Path $releaseSource 'herdr-mesh.exe'
    [IO.File]::WriteAllBytes($sourceBinary, $script:payload)
    $releaseArchive = Join-Path $testRoot 'compress-archive.zip'
    Compress-Archive -LiteralPath $sourceBinary -DestinationPath $releaseArchive
    $options = New-Options 'release-script-format'
    $options.ArchivePath = $releaseArchive
    $options.ExpectedSHA256 = (Get-FileHash -LiteralPath $releaseArchive).Hash
    $script:mockMode = 'extra-property'
    $projected = Invoke-HerdrBootstrapCore @options
    Assert-True (-not ($projected | ConvertTo-Json -Depth 4).Contains('SECRET-MARKER')) 'Unexpected remote properties leaked.'
    Assert-Equal (Get-FileHash -LiteralPath $sourceBinary).Hash.ToLowerInvariant() $projected.BinarySHA256 'Release compressor compatibility failed.'
    $script:tests++

    foreach ($hash in @('', 'abc', ('0' * 63), ('0' * 65), (('a' * 63) + ';'))) {
        $options = New-Options "bad-hash-$($script:tests)"
        $options.ExpectedSHA256 = $hash
        Assert-Fails $options 'BOOTSTRAP_INPUT' 'not-installed' 'validation'
        Assert-Equal 0 $script:mockCalls.Count 'Invalid input contacted remote.'
    }
    $options = New-Options 'hash-mismatch'
    $options.ExpectedSHA256 = '0' * 64
    Assert-Fails $options 'BOOTSTRAP_HASH' 'not-installed' 'validation'
    Assert-Equal 0 $script:mockCalls.Count 'Bad checksum reached transport.'
    $options = New-Options 'missing-archive'
    $options.ArchivePath = Join-Path $testRoot 'missing.zip'
    Assert-Fails $options 'BOOTSTRAP_IO' 'not-installed' 'validation'
    $options = New-Options 'network-archive'
    $options.ArchivePath = '\\must-not-contact\share\release.zip'
    Assert-Fails $options 'BOOTSTRAP_LOCAL_ARCHIVE' 'not-installed' 'validation'
    Assert-Equal 0 $script:mockCalls.Count 'Network archive attempted OOB work.'

    $badArchives = @{
        'traversal.zip' = @('..\herdr-mesh.exe')
        'traversal-slash.zip' = @('../herdr-mesh.exe')
        'absolute.zip' = @('C:\herdr-mesh.exe')
        'nested.zip' = @('folder/herdr-mesh.exe')
        'case.zip' = @('HERDR-MESH.EXE')
        'ads.zip' = @('herdr-mesh.exe:payload')
        'extra.zip' = @('herdr-mesh.exe', 'extra.txt')
        'duplicate.zip' = @('herdr-mesh.exe', 'herdr-mesh.exe')
        'unix.zip' = @('herdr-mesh')
    }
    foreach ($name in $badArchives.Keys) {
        $path = New-FixtureArchive $name -Entries $badArchives[$name]
        $options = New-Options "layout-$($script:tests)"
        $options.ArchivePath = $path
        $options.ExpectedSHA256 = (Get-FileHash -LiteralPath $path).Hash
        Assert-Fails $options 'BOOTSTRAP_ARCHIVE_LAYOUT' 'not-installed' 'validation'
        Assert-Equal 0 $script:mockCalls.Count 'Invalid layout reached transport.'
    }
    foreach ($attributes in @((0xA000 -shl 16), 0x400, 0x10)) {
        $path = New-FixtureArchive "link-$($script:tests).zip" -Attributes $attributes
        $options = New-Options "link-$($script:tests)"
        $options.ArchivePath = $path
        $options.ExpectedSHA256 = (Get-FileHash -LiteralPath $path).Hash
        Assert-Fails $options 'BOOTSTRAP_ARCHIVE_LAYOUT' 'not-installed' 'validation'
    }
    $path = New-FixtureArchive 'non-pe.zip' -Payload ([byte[]]::new(300))
    $options = New-Options 'non-pe'
    $options.ArchivePath = $path
    $options.ExpectedSHA256 = (Get-FileHash -LiteralPath $path).Hash
    Assert-Fails $options 'BOOTSTRAP_BINARY_FORMAT' 'not-installed' 'validation'
    $options = New-Options 'wrong-arch'
    $options.Architecture = if ($script:architecture -eq 'amd64') { 'arm64' } else { 'amd64' }
    Assert-Fails $options 'BOOTSTRAP_ARCHITECTURE' 'not-installed' 'validation'
    $otherPayload = $script:payload.Clone()
    $otherMachine = if ($machine -eq 0x8664) { 0xAA64 } else { 0x8664 }
    [BitConverter]::GetBytes([uint16]$otherMachine).CopyTo($otherPayload, 132)
    $path = New-FixtureArchive 'other-native-architecture.zip' -Payload $otherPayload
    $options = New-Options 'remote-architecture'
    $options.Architecture = if ($script:architecture -eq 'amd64') { 'arm64' } else { 'amd64' }
    $options.ArchivePath = $path
    $options.ExpectedSHA256 = (Get-FileHash -LiteralPath $path).Hash
    Assert-Fails $options 'BOOTSTRAP_ARCHITECTURE' 'indeterminate' 'prepare'
    $path = Join-Path $testRoot 'malformed.zip'
    [IO.File]::WriteAllText($path, 'this is not a ZIP')
    $options = New-Options 'malformed'
    $options.ArchivePath = $path
    $options.ExpectedSHA256 = (Get-FileHash -LiteralPath $path).Hash
    Assert-Fails $options 'BOOTSTRAP_ARCHIVE_FORMAT' 'not-installed' 'validation'
    $path = Join-Path $testRoot 'oversize.zip'
    $file = [IO.File]::Open($path, [IO.FileMode]::CreateNew)
    try { $file.SetLength(256MB + 1) } finally { $file.Dispose() }
    $options = New-Options 'oversize'
    $options.ArchivePath = $path
    Assert-Fails $options 'BOOTSTRAP_ARCHIVE_SIZE' 'not-installed' 'validation'
    # Change the central-directory expanded length without allocating a bomb.
    $bombBytes = [IO.File]::ReadAllBytes($script:archive)
    $centralOffset = [BitConverter]::ToUInt32($bombBytes, $bombBytes.Length - 6)
    [BitConverter]::GetBytes([uint32](512MB + 1)).CopyTo($bombBytes, $centralOffset + 24)
    $path = Join-Path $testRoot 'expanded-limit.zip'
    [IO.File]::WriteAllBytes($path, $bombBytes)
    $options = New-Options 'expanded-limit'
    $options.ArchivePath = $path
    $options.ExpectedSHA256 = (Get-FileHash -LiteralPath $path).Hash
    Assert-Fails $options 'BOOTSTRAP_BINARY_SIZE' 'not-installed' 'validation'
    foreach ($kind in @('central-size', 'trailing-data', 'multi-disk')) {
        $bytes = [IO.File]::ReadAllBytes($script:archive)
        if ($kind -eq 'central-size') {
            [BitConverter]::GetBytes([uint32](64KB + 1)).CopyTo($bytes, $bytes.Length - 10)
        } elseif ($kind -eq 'multi-disk') {
            [BitConverter]::GetBytes([uint16]1).CopyTo($bytes, $bytes.Length - 18)
        } else {
            $bytes = [byte[]]($bytes + [byte]0)
        }
        $path = Join-Path $testRoot "$kind.zip"
        [IO.File]::WriteAllBytes($path, $bytes)
        $options = New-Options $kind
        $options.ArchivePath = $path
        $options.ExpectedSHA256 = (Get-FileHash -LiteralPath $path).Hash
        Assert-Fails $options 'BOOTSTRAP_ARCHIVE_FORMAT' 'not-installed' 'validation'
    }

    foreach ($badPath in @('C:\', 'relative', '\\server\share\node', '\\?\C:\node', 'C:\node\..\other',
            'C:\node\.', 'C:\node\', 'C:\node:ads', 'C:\NUL', 'C:\COM1.txt', 'C:\node.',
            'C:\node ', 'C:\node;whoami', 'C:\$(whoami)', 'C:\node`test', "C:\node'quote", "C:\node`nnext")) {
        $options = New-Options "path-$($script:tests)"
        $options.InstallDirectory = $badPath
        Assert-Fails $options 'BOOTSTRAP_PATH' 'indeterminate' 'prepare'
        Assert-Equal 1 $script:mockCalls.Count 'Invalid path progressed beyond preflight.'
    }
    foreach ($badServer in @('host;whoami:50052', '$(whoami):50052', '-host:22', 'host:0',
            'host:65536', 'host:22 extra', "host`n:22", "host:22`n", 'https://host:22', 'host..name:22', 'host-:22', 'host:22;')) {
        $options = New-Options "server-$($script:tests)"
        $options.Server = $badServer
        Assert-Fails $options 'BOOTSTRAP_SERVER' 'not-installed' 'validation'
        Assert-Equal 0 $script:mockCalls.Count 'Invalid server reached transport.'
    }
    foreach ($pair in @(
            @($script:state.ToUpperInvariant(), $script:state),
            @((Join-Path $script:state 'child'), $script:state),
            @($testRoot, $script:state))) {
        $options = New-Options "overlap-$($script:tests)"
        $options.InstallDirectory = $pair[0]
        $options.StateDirectory = $pair[1]
        Assert-Fails $options 'BOOTSTRAP_STATE_OVERLAP' 'indeterminate' 'prepare'
    }
    $options = New-Options 'missing-state'
    $options.StateDirectory = Join-Path $testRoot 'absent-state'
    Assert-Fails $options 'BOOTSTRAP_DIRECTORY_REQUIRED' 'indeterminate' 'prepare'
    $options = New-Options 'missing-parent'
    $options.InstallDirectory = Join-Path $testRoot 'absent-parent\install'
    Assert-Fails $options 'BOOTSTRAP_DIRECTORY_REQUIRED' 'indeterminate' 'prepare'
    $options = New-Options 'existing-file'
    [IO.File]::WriteAllText($options.InstallDirectory, 'preserve')
    Assert-Fails $options 'BOOTSTRAP_EXISTS' 'indeterminate' 'prepare'
    Assert-Equal 'preserve' ([IO.File]::ReadAllText($options.InstallDirectory)) 'Existing file changed.'
    $junction = Join-Path $testRoot 'junction'
    New-Item -ItemType Junction -Path $junction -Target $script:state | Out-Null
    $options = New-Options 'junction-refused'
    $options.InstallDirectory = Join-Path $junction 'install'
    Assert-Fails $options 'BOOTSTRAP_REPARSE' 'indeterminate' 'prepare'
    # Delete only the link, never recursively follow it into the state fixture.
    [IO.Directory]::Delete($junction)
    $publicDirectory = Join-Path $testRoot 'not-private'
    [IO.Directory]::CreateDirectory($publicDirectory) | Out-Null
    $publicACL = Get-Acl -LiteralPath $publicDirectory
    $publicACL.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new(
        [Security.Principal.SecurityIdentifier]::new('S-1-1-0'), 'ReadAndExecute', 'Allow'))
    Set-Acl -LiteralPath $publicDirectory -AclObject $publicACL
    $options = New-Options 'private-refused'
    $options.StateDirectory = $publicDirectory
    Assert-Fails $options 'BOOTSTRAP_PRIVATE_DIRECTORY' 'indeterminate' 'prepare'
    $inheritedDirectory = Join-Path $testRoot 'inheritable-public'
    [IO.Directory]::CreateDirectory($inheritedDirectory) | Out-Null
    $inheritedACL = Get-Acl -LiteralPath $inheritedDirectory
    $inheritedACL.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new(
        [Security.Principal.SecurityIdentifier]::new('S-1-1-0'), 'ReadAndExecute',
        'ContainerInherit, ObjectInherit', 'InheritOnly', 'Allow'))
    Set-Acl -LiteralPath $inheritedDirectory -AclObject $inheritedACL
    $options = New-Options 'private-descendants-refused'
    $options.StateDirectory = $inheritedDirectory
    Assert-Fails $options 'BOOTSTRAP_PRIVATE_DIRECTORY' 'indeterminate' 'prepare'

    foreach ($mode in @('prepare-fail', 'unknown-error', 'upload-fail', 'commit-fail', 'tamper-upload', 'bad-offset',
            'big-chunk', 'invalid-base64', 'publish-race', 'extra-output')) {
        $options = New-Options $mode
        $script:mockMode = $mode
        $code = switch ($mode) {
            'tamper-upload' { 'BOOTSTRAP_HASH' }
            'bad-offset' { 'BOOTSTRAP_OFFSET' }
            'big-chunk' { 'BOOTSTRAP_CHUNK' }
            'invalid-base64' { 'BOOTSTRAP_CHUNK' }
            'publish-race' { 'BOOTSTRAP_EXISTS' }
            'extra-output' { 'BOOTSTRAP_TRANSPORT' }
            default { 'BOOTSTRAP_IO' }
        }
        Assert-Fails $options $code 'indeterminate'
        Assert-True (-not $script:latestFailure.ToString().Contains('BOOTSTRAP_SECRET_MARKER')) 'Unknown remote error code leaked.'
        if ($mode -eq 'publish-race') {
            Assert-Equal 'do-not-overwrite' ([IO.File]::ReadAllText((Join-Path $options.InstallDirectory 'existing.txt'))) 'Race overwrote an install.'
        } else {
            Assert-True (-not [IO.Directory]::Exists($options.InstallDirectory)) "Failure $mode published an install."
        }
        if ($mode -in @('upload-fail', 'commit-fail', 'tamper-upload')) {
            $stage = Join-Path $testRoot ".herdr-bootstrap-$($script:latestFailure.Data['OperationId'])"
            Assert-True ([IO.Directory]::Exists($stage)) 'Failed staging evidence was deleted.'
        }
    }
    foreach ($mode in @('commit-lost', 'bad-reply', 'bad-arguments')) {
        $options = New-Options $mode
        $script:mockMode = $mode
        $code = if ($mode -eq 'commit-lost') { 'BOOTSTRAP_IO' } else { 'BOOTSTRAP_TRANSPORT' }
        Assert-Fails $options $code 'indeterminate' 'commit'
        Assert-True ([IO.File]::Exists((Join-Path $options.InstallDirectory 'bootstrap-receipt.json'))) 'Lost acknowledgement did not exercise completed publication.'
    }
    $options = New-Options 'no-fake-start'
    $options.Start = $true
    Assert-Fails $options 'BOOTSTRAP_RUNNER_REQUIRED' 'not-installed' 'validation'
    Assert-Equal 0 $script:mockCalls.Count 'Unsupported start request performed remote work.'
    $script:cancelSource = [Threading.CancellationTokenSource]::new()
    $script:cancelSource.Cancel()
    $options = New-Options 'cancel-before'
    $options.CancellationToken = $script:cancelSource.Token
    Assert-Fails $options 'BOOTSTRAP_CANCELLED' 'not-installed' 'validation'
    Assert-Equal 0 $script:mockCalls.Count 'Pre-cancelled call reached transport.'
    $script:cancelSource.Dispose()
    $script:cancelSource = [Threading.CancellationTokenSource]::new()
    $options = New-Options 'cancel-upload'
    $options.CancellationToken = $script:cancelSource.Token
    $script:mockMode = 'cancel-upload'
    $stoppedBefore = $script:stoppedJobs
    Assert-Fails $options 'BOOTSTRAP_CANCELLED' 'indeterminate' 'upload'
    Assert-Equal ($stoppedBefore + 1) $script:stoppedJobs 'Cancelled remoting job was not stopped.'
    $options = New-Options 'timeout'
    $options.TimeoutSeconds = 1
    $script:mockMode = 'timeout'
    $stoppedBefore = $script:stoppedJobs
    $watch = [Diagnostics.Stopwatch]::StartNew()
    Assert-Fails $options 'BOOTSTRAP_TIMEOUT' 'indeterminate' 'prepare'
    Assert-True ($watch.Elapsed.TotalSeconds -lt 5) 'Fake transport deadline was not bounded.'
    Assert-Equal ($stoppedBefore + 1) $script:stoppedJobs 'Timed out job was not stopped.'

    foreach ($badName in @('Mesh Node', '\Fixture\*', '\Fixture\Node?', '\Fixture\Node[1]',
            '\Fixture\..\Node', '\Fixture\Node;', '\Fixture\$(Node)', "\Fixture\Node`n",
            '\Fixture\Node ', '\Fixture\Node.', '\Fixture\', '\\Fixture\Node')) {
        $options = New-RunnerOptions "task-name-$($script:tests)"
        $options.ExistingTaskName = $badName
        Assert-Fails $options 'BOOTSTRAP_TASK_NAME' 'not-installed' 'validation'
        Assert-Equal 0 $script:mockCalls.Count 'Invalid task name reached the transport.'
    }
    $options = New-Options 'task-name-without-start'
    $options.ExistingTaskName = '\Fixture\Mesh Node'
    Assert-Fails $options 'BOOTSTRAP_RUNNER_REQUIRED' 'not-installed' 'validation'
    $options = New-Options 'stage-without-task-tools'
    $script:taskToolsUnavailable = $true
    $null = Invoke-HerdrBootstrapCore @options
    Assert-Equal 0 $script:runnerQueries.Count 'Staging-only queried a task.'
    Assert-Equal 0 $script:runnerStartCalls 'Staging-only started a task.'
    $script:tests++

    foreach ($case in @('missing', 'unavailable', 'tools', 'zero-actions', 'multiple-actions', 'wrapper',
            'com-handler', 'arguments', 'server', 'state', 'working-directory', 'parallel', 'queue',
            'stop-existing', 'disabled', 'no-demand', 'time-limit', 'interactive', 'unknown-state')) {
        $options = New-RunnerOptions "task-invalid-$case"
        $code = 'BOOTSTRAP_TASK_ACTION'
        switch ($case) {
            'missing' { $script:runner = $null; $code = 'BOOTSTRAP_TASK_UNAVAILABLE' }
            'unavailable' { $script:runnerMode = 'unavailable'; $code = 'BOOTSTRAP_TASK_UNAVAILABLE' }
            'tools' { $script:taskToolsUnavailable = $true; $code = 'BOOTSTRAP_TASK_TOOLS' }
            'zero-actions' { $script:runner.Actions = @() }
            'multiple-actions' { $script:runner.Actions = @($script:runner.Actions[0], $script:runner.Actions[0]) }
            'wrapper' { $script:runner.Actions[0].Execute = 'C:\Windows\System32\cmd.exe' }
            'com-handler' { $script:runner.Actions[0].CimClass.CimClassName = 'MSFT_TaskComHandlerAction' }
            'arguments' { $script:runner.Actions[0].Arguments += ' -secret SECRET-MARKER' }
            'server' { $script:runner.Actions[0].Arguments = $script:runner.Actions[0].Arguments.Replace($options.Server, 'other:50052') }
            'state' { $script:runner.Actions[0].Arguments = $script:runner.Actions[0].Arguments.Replace($script:state, "$($script:state)-other") }
            'working-directory' { $script:runner.Actions[0].WorkingDirectory = 'C:\Windows' }
            'parallel' { $script:runner.Settings.MultipleInstances = 'Parallel'; $code = 'BOOTSTRAP_TASK_INSTANCES' }
            'queue' { $script:runner.Settings.MultipleInstances = 'Queue'; $code = 'BOOTSTRAP_TASK_INSTANCES' }
            'stop-existing' { $script:runner.Settings.MultipleInstances = 'StopExisting'; $code = 'BOOTSTRAP_TASK_INSTANCES' }
            'disabled' { $script:runner.Settings.Enabled = $false; $code = 'BOOTSTRAP_TASK_SETTINGS' }
            'no-demand' { $script:runner.Settings.AllowDemandStart = $false; $code = 'BOOTSTRAP_TASK_SETTINGS' }
            'time-limit' { $script:runner.Settings.ExecutionTimeLimit = 'PT72H'; $code = 'BOOTSTRAP_TASK_SETTINGS' }
            'interactive' { $script:runner.Principal.LogonType = 'Interactive'; $code = 'BOOTSTRAP_TASK_SETTINGS' }
            'unknown-state' { $script:runner.State = 'Unknown'; $code = 'BOOTSTRAP_TASK_STATE' }
        }
        $beforeTree = @(Get-ChildItem -LiteralPath $testRoot -Force | ForEach-Object FullName) -join "`n"
        Assert-Fails $options $code 'indeterminate' 'prepare'
        Assert-Equal $beforeTree (@(Get-ChildItem -LiteralPath $testRoot -Force | ForEach-Object FullName) -join "`n") 'Runner preflight failure mutated the filesystem.'
        Assert-Equal 0 $script:runnerStartCalls 'Runner preflight failure attempted startup.'
        Assert-Equal 1 $script:mockCalls.Count 'Runner preflight failure continued uploading.'
    }

    foreach ($logon in @('Password', 'S4U', 'ServiceAccount')) {
        $options = New-RunnerOptions "task-success-$logon"
        $script:runner.Principal.LogonType = $logon
        $script:runner.Actions[0].WorkingDirectory = $options.InstallDirectory
        $script:runnerMode = 'eventually-running'
        $script:mockMode = 'start-extra-property'
        $began = [DateTimeOffset]::UtcNow
        $started = Invoke-HerdrBootstrapCore @options
        Assert-Equal 'runner-running-mesh-unverified' $started.Status 'Task state was conflated with mesh readiness.'
        Assert-Equal 'requested-once' $started.Startup 'Unexpected startup disposition.'
        Assert-Equal 'Running' $started.RunnerState 'Fresh running state was not returned.'
        Assert-Equal 'not-checked' $started.Enrollment 'Startup claimed enrollment.'
        Assert-Equal 'not-checked' $started.Connectivity 'Startup claimed connectivity.'
        Assert-Equal $false $started.RunnerRequired 'Existing supported runner was not acknowledged.'
        Assert-Equal 1 $script:runnerStartCalls 'Task start was retried.'
        Assert-Equal 1 @($script:mockCalls | Where-Object Action -eq 'start').Count 'OOB start was retried.'
        Assert-True ($script:runnerObservationQueries -ge 3) 'Observation reused the pre-start task snapshot.'
        Assert-True ([DateTimeOffset]::Parse($started.RunnerObservedAtUtc) -ge $began) 'Runner observation was stale.'
        Assert-True (-not ($started | ConvertTo-Json -Depth 4).Contains('SECRET-MARKER')) 'Unexpected runner response properties leaked.'
        $persisted = Get-Content -LiteralPath (Join-Path $options.InstallDirectory 'bootstrap-receipt.json') -Raw | ConvertFrom-Json
        Assert-Equal 'staged-not-started' $persisted.Status 'Transient runner status rewrote the install receipt.'
        Assert-Equal $persisted.OperationId $started.InstallOperationId 'Startup lost installation provenance.'
        Assert-True ($script:runnerQueries[0] -ceq 'prepare') 'Task was not validated before staging.'
        Assert-True (@($script:runnerQueries | Where-Object { $_ -ceq 'commit' }).Count -ge 2) 'Task was not revalidated before publication.'
        $script:tests++
    }

    foreach ($case in @('already-running', 'ready-existing', 'queued-existing')) {
        $options = New-Options $case
        $staged = Invoke-HerdrBootstrapCore @options
        $installedFile = Join-Path $options.InstallDirectory 'herdr-mesh.exe'
        $installedWriteTime = [IO.File]::GetLastWriteTimeUtc($installedFile)
        $options = New-RunnerOptions $case
        $expectedCalls = 0
        $disposition = 'reused-running'
        if ($case -eq 'already-running') { $script:runner.State = 'Running' }
        if ($case -eq 'ready-existing') { $expectedCalls = 1; $disposition = 'requested-once' }
        if ($case -eq 'queued-existing') {
            $script:runner.State = 'Queued'
            $script:runnerMode = 'eventually-running'
            $disposition = 'observed-pending'
        }
        $started = Invoke-HerdrBootstrapCore @options
        Assert-Equal 'runner-running-mesh-unverified' $started.Status 'Existing install did not support runner startup.'
        Assert-Equal $disposition $started.Startup 'Existing runner was not reused appropriately.'
        Assert-Equal $expectedCalls $script:runnerStartCalls 'Existing runner received a duplicate start.'
        Assert-Equal 2 $script:mockCalls.Count 'Existing install was uploaded or republished.'
        Assert-Equal 'prepare,start' (($script:mockCalls | ForEach-Object Action) -join ',') 'Existing install used a mutation path.'
        Assert-Equal $staged.OperationId $started.InstallOperationId 'Existing install provenance changed.'
        Assert-Equal $installedWriteTime ([IO.File]::GetLastWriteTimeUtc($installedFile)) 'Reusing an install rewrote its binary.'
        $script:tests++
    }
    foreach ($case in @('binary', 'receipt')) {
        $options = New-Options "reuse-mismatch-$case"
        $null = Invoke-HerdrBootstrapCore @options
        if ($case -eq 'binary') {
            [IO.File]::WriteAllText((Join-Path $options.InstallDirectory 'herdr-mesh.exe'), 'not the installed release')
        } else {
            $path = Join-Path $options.InstallDirectory 'bootstrap-receipt.json'
            $record = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json
            $record.NodeArguments[2] = 'other:50052'
            [IO.File]::WriteAllText($path, ($record | ConvertTo-Json -Depth 4))
        }
        $options = New-RunnerOptions "reuse-mismatch-$case"
        Assert-Fails $options 'BOOTSTRAP_INSTALLED_MISMATCH' 'indeterminate' 'prepare'
        Assert-Equal 0 $script:runnerStartCalls 'Mismatched existing install was started.'
    }

    foreach ($case in @('task-change-before-commit', 'change-before-publication')) {
        $options = New-RunnerOptions $case
        if ($case -eq 'change-before-publication') { $script:runnerMode = $case } else { $script:mockMode = $case }
        Assert-Fails $options 'BOOTSTRAP_TASK_ACTION' 'indeterminate' 'commit'
        Assert-True (-not [IO.Directory]::Exists($options.InstallDirectory)) 'Changed runner configuration allowed publication.'
        Assert-Equal 0 $script:runnerStartCalls 'Changed runner configuration was started.'
    }
    $options = New-RunnerOptions 'race-running'
    $script:mockMode = 'race-running'
    $started = Invoke-HerdrBootstrapCore @options
    Assert-Equal 'reused-running' $started.Startup 'A matching task that started concurrently was not reused.'
    Assert-Equal 0 $script:runnerStartCalls 'Concurrent matching runner received another start.'
    $script:tests++

    foreach ($case in @('start-fail', 'start-ack-lost', 'start-lost', 'start-bad-reply',
            'task-change-before-start', 'change-after-start', 'tamper-installed', 'never-running', 'running-exits', 'observation-window')) {
        $options = New-RunnerOptions $case
        $expectedStarts = 1
        $code = 'BOOTSTRAP_TASK_START'
        switch ($case) {
            'start-fail' { $script:runnerMode = $case }
            'start-ack-lost' { $script:runnerMode = $case }
            'start-lost' { $script:mockMode = $case; $code = 'BOOTSTRAP_IO' }
            'start-bad-reply' { $script:mockMode = $case; $code = 'BOOTSTRAP_TRANSPORT' }
            'task-change-before-start' { $script:mockMode = $case; $code = 'BOOTSTRAP_TASK_ACTION'; $expectedStarts = 0 }
            'change-after-start' { $script:runnerMode = $case; $code = 'BOOTSTRAP_TASK_ACTION' }
            'tamper-installed' { $script:mockMode = $case; $code = 'BOOTSTRAP_INSTALLED_MISMATCH'; $expectedStarts = 0 }
            'never-running' { $script:runnerMode = $case; $script:mockMode = 'runner-short-budget'; $code = 'BOOTSTRAP_TIMEOUT' }
            'observation-window' { $script:runnerMode = 'never-running'; $code = 'BOOTSTRAP_TASK_OBSERVATION' }
            'running-exits' {
                $script:runner.State = 'Running'
                $script:runnerMode = $case
                $script:mockMode = 'runner-short-budget'
                $code = 'BOOTSTRAP_TIMEOUT'
                $expectedStarts = 0
            }
        }
        $observationWatch = [Diagnostics.Stopwatch]::StartNew()
        Assert-Fails $options $code 'start-unknown' 'start'
        if ($case -eq 'observation-window') {
            Assert-True ($observationWatch.Elapsed.TotalSeconds -ge 10 -and $observationWatch.Elapsed.TotalSeconds -lt 20) `
                'Observation did not use its ten-second bound independently of the overall timeout.'
        }
        Assert-Equal 'start-unknown' $script:latestFailure.Data['BootstrapResult'].Status 'Unknown startup did not expose a typed status.'
        Assert-Equal 'not-checked' $script:latestFailure.Data['BootstrapResult'].Connectivity 'Unknown startup claimed mesh connectivity.'
        Assert-Equal $expectedStarts $script:runnerStartCalls 'Unknown start was automatically retried.'
        Assert-Equal 1 @($script:mockCalls | Where-Object Action -eq 'start').Count 'Unknown OOB start was retried.'
        Assert-True ([IO.File]::Exists((Join-Path $options.InstallDirectory 'bootstrap-receipt.json'))) 'Start uncertainty removed a verified install.'
        Assert-True (-not $script:latestFailure.Message.Contains('Node startup was not attempted')) 'Unknown start was reported as never attempted.'
    }
    foreach ($case in @('cancel-before-start', 'cancel-after-start', 'start-timeout')) {
        if ($script:cancelSource) { $script:cancelSource.Dispose() }
        $script:cancelSource = [Threading.CancellationTokenSource]::new()
        if ($case -eq 'start-timeout') {
            # Test a lost start acknowledgement, not filesystem work racing a
            # one-second deadline on a busy shared build machine.
            $stagingOptions = New-Options $case
            $null = Invoke-HerdrBootstrapCore @stagingOptions
        }
        $options = New-RunnerOptions $case
        $options.CancellationToken = $script:cancelSource.Token
        $script:mockMode = $case
        $code = 'BOOTSTRAP_CANCELLED'
        $outcome = 'start-unknown'
        $phase = 'start'
        $expectedStarts = 1
        if ($case -eq 'cancel-before-start') { $outcome = 'indeterminate'; $phase = 'commit'; $expectedStarts = 0 }
        if ($case -eq 'start-timeout') { $options.TimeoutSeconds = 5; $code = 'BOOTSTRAP_TIMEOUT' }
        Assert-Fails $options $code $outcome $phase
        Assert-Equal $expectedStarts $script:runnerStartCalls 'Cancellation/deadline caused a new task start.'
        Assert-Equal $expectedStarts @($script:mockCalls | Where-Object Action -eq 'start').Count 'Cancellation/deadline replayed an OOB start.'
    }

    $herdrDirectory = Join-Path $testRoot 'Herdr tools'
    [IO.Directory]::CreateDirectory($herdrDirectory) | Out-Null
    $herdrExecutable = Join-Path $herdrDirectory 'herdr.exe'
    $otherHerdr = Join-Path $herdrDirectory 'other.exe'
    # Existence/configuration fixtures only; neither executable may be run.
    [IO.File]::WriteAllText($herdrExecutable, 'disposable Herdr executable stand-in')
    [IO.File]::WriteAllText($otherHerdr, 'disposable alternate Herdr executable stand-in')
    $herdrHash = (Get-FileHash -LiteralPath $herdrExecutable).Hash
    $options = New-Options 'headless-staged'
    $options.HerdrExecutable = $herdrExecutable
    $configured = Invoke-HerdrBootstrapCore @options
    $expectedArguments = @('node', '-server', $options.Server, '-state-dir', $script:state, '-herdr-executable', $herdrExecutable)
    Assert-Equal 7 $configured.NodeArguments.Count 'Headless configuration must append exactly two arguments.'
    Assert-Equal ($expectedArguments -join '|') ($configured.NodeArguments -join '|') 'Headless argument array differs from intended fixed flags.'
    Assert-Equal 'staged-not-started' $configured.Status 'Configuring Herdr claimed startup.'
    Assert-Equal 'not-checked' $configured.Connectivity 'Configuring Herdr claimed connectivity.'
    Assert-Equal 0 $script:runnerQueries.Count 'Headless staging queried a live runner.'
    $headlessReceiptPath = Join-Path $options.InstallDirectory 'bootstrap-receipt.json'
    $headlessRecord = Get-Content -LiteralPath $headlessReceiptPath -Raw | ConvertFrom-Json
    Assert-Equal ($expectedArguments -join '|') ($headlessRecord.NodeArguments -join '|') 'Receipt omitted headless configuration.'
    $script:tests++

    $options = New-RunnerOptions 'headless-staged' -HerdrExecutable $herdrExecutable
    $script:runner.State = 'Running'
    $receiptHash = (Get-FileHash -LiteralPath $headlessReceiptPath).Hash
    $reused = Invoke-HerdrBootstrapCore @options
    Assert-Equal 'runner-running-mesh-unverified' $reused.Status 'Matching configured runner was not reusable.'
    Assert-Equal 'reused-running' $reused.Startup 'Running headless runner was restarted.'
    Assert-Equal $configured.OperationId $reused.InstallOperationId 'Headless reuse lost install provenance.'
    Assert-Equal ($expectedArguments -join '|') ($reused.NodeArguments -join '|') 'Headless reuse changed configuration.'
    Assert-Equal $receiptHash (Get-FileHash -LiteralPath $headlessReceiptPath).Hash 'Headless reuse rewrote the receipt.'
    Assert-Equal 'prepare,start' (($script:mockCalls | ForEach-Object Action) -join ',') 'Headless reuse republished an install.'
    Assert-Equal 0 $script:runnerStartCalls 'Headless reuse dispatched a duplicate start.'
    $script:tests++
    $options = New-RunnerOptions 'headless-start' -HerdrExecutable $herdrExecutable
    $started = Invoke-HerdrBootstrapCore @options
    Assert-Equal 'runner-running-mesh-unverified' $started.Status 'Headless startup claimed or lost the runner boundary.'
    Assert-Equal 'not-checked' $started.Enrollment 'Headless startup claimed enrollment.'
    Assert-Equal 1 $script:runnerStartCalls 'Configured headless startup was not one-shot.'
    Assert-Equal 7 $started.NodeArguments.Count 'Headless startup lost typed arguments.'
    Assert-True ($script:runner.Actions[0].Arguments.EndsWith(' -herdr-executable "' + $herdrExecutable + '"')) `
        'Task arguments did not keep the spaced Herdr path as one quoted value.'
    $script:tests++

    foreach ($badPath in @('', ' ', 'herdr.exe', 'C:herdr.exe', '\\server\share\herdr.exe',
            '\\?\C:\herdr.exe', 'C:\tools\..\herdr.exe', 'C:\tools\herdr.exe:stream',
            'C:\tools\herdr.exe;', 'C:\tools\$(herdr).exe', 'C:\tools\herdr`name.exe',
            "C:\tools\herdr'name.exe", 'C:\tools\herdr"name.exe', "C:\tools\herdr.exe`n",
            'C:\tools\herdr.exe ', 'C:\tools\herdr.exe.', 'C:\CON.exe',
            ('C:\' + ('a' * 81) + '.exe'), ('C:\' + ('a\' * 100) + 'herdr.exe'))) {
        $options = New-Options "herdr-invalid-$($script:tests)"
        $options.HerdrExecutable = $badPath
        $code = 'BOOTSTRAP_PATH'
        $outcome = 'indeterminate'
        $phase = 'prepare'
        if ([string]::IsNullOrWhiteSpace($badPath)) {
            $code = 'BOOTSTRAP_HERDR_EXECUTABLE'
            $outcome = 'not-installed'
            $phase = 'validation'
        }
        $beforeTree = @(Get-ChildItem -LiteralPath $testRoot -Force | ForEach-Object FullName) -join "`n"
        Assert-Fails $options $code $outcome $phase
        Assert-Equal $beforeTree (@(Get-ChildItem -LiteralPath $testRoot -Force | ForEach-Object FullName) -join "`n") 'Invalid Herdr path mutated staging.'
        Assert-Equal 0 $script:runnerStartCalls 'Invalid Herdr path attempted startup.'
    }
    $directoryExe = Join-Path $herdrDirectory 'directory.exe'
    [IO.Directory]::CreateDirectory($directoryExe) | Out-Null
    $wrongExtension = Join-Path $herdrDirectory 'herdr.ps1'
    [IO.File]::WriteAllText($wrongExtension, 'not executable')
    foreach ($path in @((Join-Path $herdrDirectory 'missing.exe'), $directoryExe, $wrongExtension)) {
        foreach ($withStart in @($false, $true)) {
            $options = if ($withStart) {
                New-RunnerOptions "herdr-missing-$($script:tests)" -HerdrExecutable $path
            } else {
                $stagingOptions = New-Options "herdr-missing-$($script:tests)"
                $stagingOptions.HerdrExecutable = $path
                $stagingOptions
            }
            $beforeTree = @(Get-ChildItem -LiteralPath $testRoot -Force | ForEach-Object FullName) -join "`n"
            Assert-Fails $options 'BOOTSTRAP_HERDR_EXECUTABLE' 'indeterminate' 'prepare'
            Assert-Equal $beforeTree (@(Get-ChildItem -LiteralPath $testRoot -Force | ForEach-Object FullName) -join "`n") 'Missing Herdr prerequisite mutated staging.'
            Assert-Equal 0 $script:runnerQueries.Count 'Missing Herdr prerequisite reached the scheduler.'
            Assert-Equal 0 $script:runnerStartCalls 'Missing Herdr prerequisite started a runner.'
        }
    }
    foreach ($leaf in @('herdr-link', 'herdr-link.exe')) {
        $junction = Join-Path $testRoot $leaf
        New-Item -ItemType Junction -Path $junction -Target $herdrDirectory | Out-Null
        try {
            $path = if ($leaf.EndsWith('.exe')) { $junction } else { Join-Path $junction 'herdr.exe' }
            $options = New-RunnerOptions "herdr-reparse-$($script:tests)" -HerdrExecutable $path
            Assert-Fails $options 'BOOTSTRAP_REPARSE' 'indeterminate' 'prepare'
            Assert-Equal 0 $script:runnerStartCalls 'Reparse Herdr path authorized a start.'
            Assert-True (-not [IO.Directory]::Exists($options.InstallDirectory)) 'Reparse Herdr path published an install.'
        } finally { [IO.Directory]::Delete($junction) }
    }
    foreach ($case in @('missing-flag', 'wrong-path', 'extra-flag', 'omitted-option')) {
        $options = New-RunnerOptions "herdr-task-$case" -HerdrExecutable $herdrExecutable
        switch ($case) {
            'missing-flag' { $script:runner.Actions[0].Arguments = 'node -server "' + $options.Server + '" -state-dir "' + $script:state + '"' }
            'wrong-path' { $script:runner.Actions[0].Arguments = $script:runner.Actions[0].Arguments.Replace($herdrExecutable, $otherHerdr) }
            'extra-flag' { $script:runner.Actions[0].Arguments += ' -extra SECRET-MARKER' }
            'omitted-option' { $options.Remove('HerdrExecutable') }
        }
        Assert-Fails $options 'BOOTSTRAP_TASK_ACTION' 'indeterminate' 'prepare'
        Assert-Equal 0 $script:runnerStartCalls 'Mismatched Herdr task arguments were started.'
        Assert-True (-not [IO.Directory]::Exists($options.InstallDirectory)) 'Mismatched Herdr task arguments authorized publication.'
    }
    foreach ($case in @('added', 'removed', 'changed', 'tampered')) {
        $options = New-Options "herdr-receipt-$case"
        if ($case -ne 'added') { $options.HerdrExecutable = $herdrExecutable }
        $null = Invoke-HerdrBootstrapCore @options
        $path = Join-Path $options.InstallDirectory 'bootstrap-receipt.json'
        if ($case -eq 'tampered') {
            $record = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json
            $record.NodeArguments[6] = $otherHerdr
            [IO.File]::WriteAllText($path, ($record | ConvertTo-Json -Depth 4))
        }
        $expectedHerdr = if ($case -eq 'changed') { $otherHerdr } else { $herdrExecutable }
        $options = if ($case -eq 'removed') {
            New-RunnerOptions "herdr-receipt-$case"
        } else {
            New-RunnerOptions "herdr-receipt-$case" -HerdrExecutable $expectedHerdr
        }
        $beforeReceipt = (Get-FileHash -LiteralPath $path).Hash
        Assert-Fails $options 'BOOTSTRAP_INSTALLED_MISMATCH' 'indeterminate' 'prepare'
        Assert-Equal $beforeReceipt (Get-FileHash -LiteralPath $path).Hash 'Herdr mismatch rewrote an existing receipt.'
        Assert-Equal 0 $script:runnerStartCalls 'Herdr receipt mismatch started a task.'
    }
    $options = New-RunnerOptions 'herdr-bad-reply' -HerdrExecutable $herdrExecutable
    $script:mockMode = 'bad-herdr-reply'
    Assert-Fails $options 'BOOTSTRAP_TRANSPORT' 'indeterminate' 'commit'
    Assert-Equal 0 $script:runnerStartCalls 'Incorrect Herdr receipt reply allowed startup.'
    foreach ($case in @('herdr-missing-before-commit', 'herdr-missing-before-start',
            'herdr-missing-before-publication', 'herdr-missing-before-dispatch')) {
        $temporaryHerdr = Join-Path $herdrDirectory "$case.exe"
        [IO.File]::WriteAllText($temporaryHerdr, 'disposable vanishing executable')
        $options = New-RunnerOptions $case -HerdrExecutable $temporaryHerdr
        if ($case -in @('herdr-missing-before-publication', 'herdr-missing-before-dispatch')) {
            $script:runnerMode = $case
            $script:vanishingHerdr = $temporaryHerdr
        } else { $script:mockMode = $case }
        $phase = if ($case -in @('herdr-missing-before-commit', 'herdr-missing-before-publication')) { 'commit' } else { 'start' }
        $outcome = if ($phase -eq 'commit') { 'indeterminate' } else { 'start-unknown' }
        Assert-Fails $options 'BOOTSTRAP_HERDR_EXECUTABLE' $outcome $phase
        Assert-Equal 0 $script:runnerStartCalls 'A disappearing Herdr executable reached task start.'
        if ($phase -eq 'commit') {
            Assert-True (-not [IO.Directory]::Exists($options.InstallDirectory)) 'A missing Herdr executable allowed publication.'
        }
    }
    $options = New-Options 'transport-only-compatibility'
    $transportOnly = Invoke-HerdrBootstrapCore @options
    Assert-Equal 5 $transportOnly.NodeArguments.Count 'Absent Herdr option changed legacy transport-only arguments.'
    Assert-Equal 'staged-not-started' $transportOnly.Status 'Transport-only staging changed lifecycle.'
    $options = New-RunnerOptions 'transport-only-compatibility'
    $script:runner.State = 'Running'
    $legacyReuse = Invoke-HerdrBootstrapCore @options
    Assert-Equal $transportOnly.OperationId $legacyReuse.InstallOperationId 'Legacy receipt without Herdr fields was not reusable.'
    Assert-Equal 5 $legacyReuse.NodeArguments.Count 'Legacy receipt grew implicit Herdr arguments.'
    Assert-Equal 0 $script:runnerStartCalls 'Legacy running task was restarted.'
    Assert-Equal $herdrHash (Get-FileHash -LiteralPath $herdrExecutable).Hash 'Bootstrap modified the supplied Herdr binary.'
    $script:tests++

    Assert-Equal $beforeHash (Get-FileHash -LiteralPath $sentinel).Hash 'State bytes changed.'
    Assert-Equal $beforeWrite ([IO.File]::GetLastWriteTimeUtc($sentinel)) 'State timestamp changed.'
    Assert-Equal $beforeACL (Get-Acl -LiteralPath $script:state).Sddl 'State ACL changed.'
    Assert-Equal $beforeFiles (@(Get-ChildItem -LiteralPath $script:state -Recurse | ForEach-Object FullName) -join "`n") 'State tree changed.'
    $script:tests++

    $failedDirectly = $false
    try { & (Join-Path $PSScriptRoot 'bootstrap-node.ps1') }
    catch { $failedDirectly = $_.Exception.Message.Contains('Dot-source') }
    Assert-True $failedDirectly 'Direct script execution silently succeeded without installing.'
    $script:tests++
    $public = Get-Command Invoke-HerdrNodeBootstrap
    Assert-Equal ([System.Management.Automation.Runspaces.PSSession]) $public.Parameters['Session'].ParameterType 'Public entrypoint must require a real PSSession.'
    $nullSessionFailed = $false
    try {
        Invoke-HerdrNodeBootstrap -Session $null -ArchivePath $script:archive `
            -ExpectedSHA256 ('0' * 64) -Architecture $script:architecture `
            -InstallDirectory (Join-Path $testRoot 'public-null') -StateDirectory $script:state -Server 'host:22'
    } catch { $nullSessionFailed = $true }
    Assert-True $nullSessionFailed 'Public command accepted a missing session.'
    $script:tests++

    $tokens = $null
    $parseErrors = $null
    $ast = [Management.Automation.Language.Parser]::ParseFile(
        (Join-Path $PSScriptRoot '..\src\internal\bootstrap\assets\bootstrap-node.ps1'), [ref]$tokens, [ref]$parseErrors)
    Assert-Equal 0 $parseErrors.Count 'Bootstrap script did not parse.'
    $commands = $ast.FindAll({ param($node) $node -is [Management.Automation.Language.CommandAst] }, $true)
    foreach ($command in $commands) {
        Assert-True ($command.GetCommandName() -notin @('New-PSSession', 'Enter-PSSession', 'Start-Process',
            'Invoke-Expression', 'ssh', 'scp', 'Start-Service', 'New-Service', 'New-LocalUser', 'Install-Module',
            'New-ScheduledTask', 'Register-ScheduledTask', 'Set-ScheduledTask', 'Unregister-ScheduledTask',
            'Stop-ScheduledTask', 'Disable-ScheduledTask', 'Enable-ScheduledTask')) `
            'Bootstrap gained a connection, arbitrary shell, installer or startup command.'
    }
    Assert-Equal 1 @($commands | Where-Object { $_.GetCommandName() -ceq 'Start-ScheduledTask' }).Count 'There must be exactly one scheduler start call site.'
    $script:tests++

    . (Join-Path $PSScriptRoot '..\src\internal\bootstrap\assets\entry.ps1')
    $script:bridgeConfig = Join-Path $testRoot 'fixture-ssh-config'
    $script:bridgeCalls = 0
    $script:bridgeCloses = 0
    $script:bridgeMode = ''
    $script:bridgeParameters = $null
    $script:bridgeResult = $transportOnly
    function Get-HerdrBootstrapSSHConfig { return $script:bridgeConfig }
    function New-PSSession {
        [CmdletBinding()]
        param([string]$HostName, [switch]$SSHTransport, [string]$Subsystem, [int]$ConnectingTimeout, [hashtable]$Options)
        $script:bridgeCalls++
        Assert-Equal 'prepared-node' $HostName 'SSH received a host expression instead of a literal alias.'
        Assert-True $SSHTransport.IsPresent 'SSH transport was not selected.'
        Assert-Equal 'powershell' $Subsystem 'Unexpected remote command/subsystem.'
        Assert-True ($ConnectingTimeout -gt 0 -and $ConnectingTimeout -le 30000) 'SSH connection timeout was not bounded.'
        foreach ($name in @('BatchMode', 'StrictHostKeyChecking')) { Assert-Equal 'yes' $Options[$name] 'SSH trust/prompt policy changed.' }
        foreach ($name in @('PasswordAuthentication', 'KbdInteractiveAuthentication', 'ForwardAgent',
                'ForwardX11', 'PermitLocalCommand', 'UpdateHostKeys', 'VerifyHostKeyDNS')) {
            Assert-Equal 'no' $Options[$name] 'SSH credentials/trust policy changed.'
        }
        foreach ($name in @('ProxyCommand', 'ProxyJump', 'KnownHostsCommand', 'RemoteCommand', 'ControlPath')) {
            Assert-Equal 'none' $Options[$name] 'SSH command or multiplexing boundary changed.'
        }
        Assert-Equal '1' $Options.ConnectionAttempts 'SSH may retry a connection.'
        Assert-Equal '0' $Options.NumberOfPasswordPrompts 'SSH may ask for credentials.'
        Assert-Equal 'never' $env:SSH_ASKPASS_REQUIRE 'Askpass was not disabled.'
        Assert-True (-not [Environment]::GetEnvironmentVariable('BOOTSTRAP_FIXTURE_SECRET')) 'Secret environment reached SSH.'
        Assert-True (-not [Environment]::GetEnvironmentVariable('TS_AUTHKEY_NODE')) 'Enrollment environment reached SSH.'
        if ($script:bridgeMode -eq 'connect-fail') { throw 'SECRET-MARKER connection failed' }
        return [pscustomobject]@{ State = 'Opened'; Availability = 'Available' }
    }
    function Remove-PSSession {
        [CmdletBinding()]
        param($Session)
        $script:bridgeCloses++
        if ($script:bridgeMode -eq 'close-fail') { throw 'SECRET-MARKER disconnect failed' }
    }
    function Invoke-HerdrNodeBootstrap {
        param($Session, [string]$ArchivePath, [string]$ExpectedSHA256, [string]$Architecture,
            [string]$InstallDirectory, [string]$StateDirectory, [string]$Server, [int]$TimeoutSeconds,
            [string]$HerdrExecutable, [switch]$Start, [string]$ExistingTaskName)
        $script:bridgeParameters = $PSBoundParameters
        Assert-True ($TimeoutSeconds -gt 0 -and $TimeoutSeconds -lt 30) 'Connection time was not included in operation deadline.'
        if ($script:bridgeMode -eq 'core-fail') {
            $failure = [InvalidOperationException]::new('BOOTSTRAP_TASK_START SECRET-MARKER')
            $failure.Data['BootstrapOutcome'] = 'start-unknown'
            $failure.Data['BootstrapPhase'] = 'start'
            $failure.Data['OperationId'] = '12345678901234567890123456789012'
            throw $failure
        }
        return $script:bridgeResult
    }
    $optionsPath = Join-Path $testRoot 'bridge-options.json'
    foreach ($case in @('stage', 'start', 'bad-hash', 'missing-alias', 'include-config', 'match-config',
            'sendenv-config', 'setenv-config', 'missing-ssh', 'connect-fail', 'core-fail', 'close-fail')) {
        $script:bridgeCalls = 0
        $script:bridgeCloses = 0
        $script:bridgeMode = $case
        $script:bridgeParameters = $null
        $script:sshUnavailable = $case -eq 'missing-ssh'
        $start = $case -in @('start', 'core-fail', 'close-fail')
        $script:bridgeResult = if ($start) { $reused } else { $transportOnly }
        $options = @{
            SSHHost = 'prepared-node'; ArchivePath = $script:archive
            ExpectedSHA256 = (Get-FileHash -LiteralPath $script:archive).Hash
            Architecture = $script:architecture; InstallDirectory = $script:bridgeResult.InstallDirectory
            StateDirectory = $script:state; Server = 'coordinator.example.ts.net:50052'; TimeoutSeconds = 30
            HerdrExecutable = ''; Start = $start; ExistingTaskName = ''
        }
        if ($start) { $options.HerdrExecutable = $herdrExecutable; $options.ExistingTaskName = '\Fixture\Mesh Node' }
        if ($case -eq 'bad-hash') { $options.ExpectedSHA256 = '0' * 64 }
        $config = "Host prepared-node`n  HostName fixture.invalid`n"
        if ($case -eq 'missing-alias') { $config = "Host other`n" }
        if ($case -eq 'include-config') { $config += "Include SECRET-MARKER`n" }
        if ($case -eq 'match-config') { $config += "Match exec SECRET-MARKER`n" }
        if ($case -eq 'sendenv-config') { $config += "SendEnv SECRET-MARKER`n" }
        if ($case -eq 'setenv-config') { $config += "SetEnv SECRET-MARKER=value`n" }
        [IO.File]::WriteAllText($script:bridgeConfig, $config)
        [IO.File]::WriteAllText($optionsPath, ($options | ConvertTo-Json -Compress))
        $env:BOOTSTRAP_FIXTURE_SECRET = 'SECRET-MARKER'
        $env:TS_AUTHKEY_NODE = 'SECRET-MARKER'
        $reply = Invoke-HerdrBootstrapEntry -OptionsPath $optionsPath
        Assert-True (-not ($reply | ConvertTo-Json -Depth 8).Contains('SECRET-MARKER')) 'Bridge leaked raw credentials/diagnostics.'
        Assert-True ($script:bridgeCalls -le 1) 'Bridge retried SSH.'
        if ($case -in @('stage', 'start')) {
            Assert-True $reply.ok 'Bridge failed a valid typed request.'
            Assert-Equal $script:bridgeResult.Status $reply.result.status 'Bridge changed lifecycle.'
            Assert-Equal 1 $script:bridgeCalls 'Bridge did not create its own connection.'
            Assert-Equal 1 $script:bridgeCloses 'Bridge did not close its own connection.'
            Assert-Equal $options.ArchivePath $script:bridgeParameters.ArchivePath 'Archive escaped structured parameters.'
            if ($start) { Assert-Equal $herdrExecutable $script:bridgeParameters.HerdrExecutable 'Typed Herdr option was dropped.' }
        } else {
            Assert-True (-not $reply.ok) 'Bridge returned false success.'
            $expectedCode = switch ($case) {
                'bad-hash' { 'BOOTSTRAP_HASH' }
                'missing-ssh' { 'ssh_missing' }
                'connect-fail' { 'ssh_connection' }
                'core-fail' { 'BOOTSTRAP_TASK_START' }
                'close-fail' { 'process_failed' }
                default { 'ssh_config' }
            }
            Assert-Equal $expectedCode $reply.error.code 'Bridge did not return an actionable error category.'
            $expectedOutcome = if ($start) { 'start-unknown' } else { 'not-installed' }
            Assert-Equal $expectedOutcome $reply.error.status 'Bridge lost lifecycle uncertainty.'
            if ($case -notin @('connect-fail', 'core-fail', 'close-fail')) {
                Assert-Equal 0 $script:bridgeCalls 'Invalid local prerequisites attempted SSH.'
            }
        }
        $script:tests++
    }
    Write-Output "PASS: $($script:tests) bootstrap checks; disposable Windows fixtures, fake SSH/PSSession transport and fake Scheduled Tasks only."
} catch {
    if ($script:mockCalls.Count -gt 0) {
        $last = $script:mockCalls[$script:mockCalls.Count - 1]
        Write-Warning "Failed disposable fixture: $([IO.Path]::GetFileName($last.Request.InstallDirectory)), action $($last.Action)."
    }
    throw
} finally {
    if ($script:cancelSource) { $script:cancelSource.Dispose() }
    # This uniquely named directory was created above and contains only this test's fixtures.
    if ([IO.Directory]::Exists($testRoot)) { Remove-Item -LiteralPath $testRoot -Recurse -Force }
}
