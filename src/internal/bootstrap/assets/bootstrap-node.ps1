#requires -Version 7.4

# Embedded implementation. The operator entrypoint is herdr-mesh bootstrap.
if ($MyInvocation.InvocationName -ne '.') {
    throw 'Dot-source this embedded implementation only for developer fixtures; use herdr-mesh bootstrap for operator workflows.'
}

$script:HerdrBootstrapProgram = {
    param(
        [ValidateSet('validate', 'prepare', 'upload', 'commit', 'start')]
        [string]$Action,
        [string]$RequestJson,
        [Threading.CancellationToken]$CancellationToken = [Threading.CancellationToken]::None
    )

    $ErrorActionPreference = 'Stop'
    Set-StrictMode -Version Latest
    $request = ConvertFrom-Json -InputObject $RequestJson -AsHashtable
    $clock = [Diagnostics.Stopwatch]::StartNew()
    $maxArchive = 256MB
    $maxBinary = 512MB
    $chunkSize = 192KB

    function Assert-Budget {
        if ($CancellationToken.IsCancellationRequested) { throw 'BOOTSTRAP_CANCELLED' }
        if ($clock.Elapsed.TotalMilliseconds -ge $request.RemainingMilliseconds) { throw 'BOOTSTRAP_TIMEOUT' }
    }

    function Get-LocalDirectoryPath([string]$Value) {
        if ($Value.Length -gt 200 -or $Value -cnotmatch '^[A-Za-z]:\\[^\\]+(?:\\[^\\]+)*\z') {
            throw 'BOOTSTRAP_PATH'
        }
        foreach ($part in $Value.Substring(3).Split('\')) {
            if ($part -cnotmatch '^[A-Za-z0-9_][A-Za-z0-9 ._-]{0,79}\z' -or
                $part.EndsWith('.') -or $part.EndsWith(' ') -or
                $part -match '^(CON|PRN|AUX|NUL|COM[0-9]|LPT[0-9])(?:\.|$)') {
                throw 'BOOTSTRAP_PATH'
            }
        }
        return [IO.Path]::GetFullPath($Value)
    }

    function Assert-NoReparse([string]$Path) {
        $ancestors = [Collections.Generic.Stack[string]]::new()
        $item = $Path
        while ($item) {
            $ancestors.Push($item)
            $item = [IO.Path]::GetDirectoryName($item)
        }
        while ($ancestors.Count -gt 0) {
            $item = $ancestors.Pop()
            # GetAttributes also sees dangling links; File.Exists alone does not.
            try { $attributes = [IO.File]::GetAttributes($item) }
            catch [IO.FileNotFoundException] { $attributes = $null }
            catch [IO.DirectoryNotFoundException] { $attributes = $null }
            if ($null -ne $attributes -and ($attributes -band [IO.FileAttributes]::ReparsePoint)) {
                throw 'BOOTSTRAP_REPARSE'
            }
        }
    }

    function Assert-PrivateDirectory([string]$Path) {
        if (-not [IO.Directory]::Exists($Path)) { throw 'BOOTSTRAP_DIRECTORY_REQUIRED' }
        Assert-NoReparse $Path
        $acl = Get-Acl -LiteralPath $Path
        $descriptor = [Security.AccessControl.RawSecurityDescriptor]::new($acl.GetSecurityDescriptorBinaryForm(), 0)
        if ($null -eq $descriptor.DiscretionaryAcl) { throw 'BOOTSTRAP_PRIVATE_DIRECTORY' }
        $operator = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
        $allowed = @($operator, 'S-1-5-18', 'S-1-5-32-544')
        if ($acl.GetOwner([Security.Principal.SecurityIdentifier]).Value -notin $allowed) {
            throw 'BOOTSTRAP_PRIVATE_DIRECTORY'
        }
        foreach ($rule in $acl.GetAccessRules($true, $true, [Security.Principal.SecurityIdentifier])) {
            if ($rule.AccessControlType -eq [Security.AccessControl.AccessControlType]::Allow -and
                $rule.IdentityReference.Value -notin $allowed) {
                if ($rule.IdentityReference.Value -eq 'S-1-3-0' -and
                    ($rule.PropagationFlags -band [Security.AccessControl.PropagationFlags]::InheritOnly) -ne 0) {
                    continue
                }
                throw 'BOOTSTRAP_PRIVATE_DIRECTORY'
            }
        }
    }

    function Get-ArchiveInfo([string]$Path, [string]$ExtractTo = '') {
        $file = $null
        $zip = $null
        $entryStream = $null
        $output = $null
        $hash = [Security.Cryptography.IncrementalHash]::CreateHash([Security.Cryptography.HashAlgorithmName]::SHA256)
        try {
            $file = [IO.File]::Open($Path, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
            if ($file.Length -le 0 -or $file.Length -gt $maxArchive) { throw 'BOOTSTRAP_ARCHIVE_SIZE' }
            $buffer = [byte[]]::new(64KB)
            while (($count = $file.Read($buffer, 0, $buffer.Length)) -gt 0) {
                Assert-Budget
                $hash.AppendData($buffer, 0, $count)
            }
            $archiveHash = [Convert]::ToHexString($hash.GetHashAndReset()).ToLowerInvariant()
            if ($archiveHash -cne $request.ExpectedSHA256.ToLowerInvariant()) { throw 'BOOTSTRAP_HASH' }
            # Bound central-directory parsing before ZipArchive allocates entry objects.
            # The release format never needs multi-disk or ZIP64 metadata.
            $tail = [byte[]]::new([Math]::Min($file.Length, 65557))
            $file.Position = $file.Length - $tail.Length
            $file.ReadExactly($tail, 0, $tail.Length)
            $end = -1
            for ($i = $tail.Length - 22; $i -ge 0; $i--) {
                if ([BitConverter]::ToUInt32($tail, $i) -eq 0x06054B50 -and
                    $i + 22 + [BitConverter]::ToUInt16($tail, $i + 20) -eq $tail.Length) {
                    $end = $i
                    break
                }
            }
            if ($end -lt 0 -or [BitConverter]::ToUInt16($tail, $end + 4) -ne 0 -or
                [BitConverter]::ToUInt16($tail, $end + 6) -ne 0) { throw 'BOOTSTRAP_ARCHIVE_FORMAT' }
            if ([BitConverter]::ToUInt16($tail, $end + 8) -ne 1 -or
                [BitConverter]::ToUInt16($tail, $end + 10) -ne 1) { throw 'BOOTSTRAP_ARCHIVE_LAYOUT' }
            [long]$centralSize = [BitConverter]::ToUInt32($tail, $end + 12)
            [long]$centralOffset = [BitConverter]::ToUInt32($tail, $end + 16)
            if ($centralSize -lt 46 -or $centralSize -gt 64KB -or
                $centralOffset + $centralSize -ne $file.Length - $tail.Length + $end) {
                throw 'BOOTSTRAP_ARCHIVE_FORMAT'
            }
            $file.Position = 0
            try { $zip = [IO.Compression.ZipArchive]::new($file, [IO.Compression.ZipArchiveMode]::Read, $true) }
            catch { throw 'BOOTSTRAP_ARCHIVE_FORMAT' }
            # build-release.ps1 produces precisely one root-level executable.
            if ($zip.Entries.Count -ne 1) { throw 'BOOTSTRAP_ARCHIVE_LAYOUT' }
            $entry = $zip.Entries[0]
            $kind = ($entry.ExternalAttributes -shr 16) -band 0xF000
            if ($entry.FullName -cne 'herdr-mesh.exe' -or $kind -notin @(0, 0x8000) -or
                ($entry.ExternalAttributes -band 0x410) -ne 0) {
                throw 'BOOTSTRAP_ARCHIVE_LAYOUT'
            }
            if ($entry.Length -lt 256 -or $entry.Length -gt $maxBinary) { throw 'BOOTSTRAP_BINARY_SIZE' }
            if ($ExtractTo) {
                $output = [IO.File]::Open($ExtractTo, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
            }
            $entryStream = $entry.Open()
            $header = [byte[]]::new(4096)
            [long]$total = 0
            while (($count = $entryStream.Read($buffer, 0, $buffer.Length)) -gt 0) {
                Assert-Budget
                $total += $count
                if ($total -gt $maxBinary -or $total -gt $entry.Length) { throw 'BOOTSTRAP_BINARY_SIZE' }
                $headerOffset = $total - $count
                if ($headerOffset -lt $header.Length) {
                    [Array]::Copy($buffer, 0, $header, $headerOffset, [Math]::Min($count, $header.Length - $headerOffset))
                }
                $hash.AppendData($buffer, 0, $count)
                if ($output) { $output.Write($buffer, 0, $count) }
            }
            if ($total -ne $entry.Length) { throw 'BOOTSTRAP_BINARY_SIZE' }
            $pe = [BitConverter]::ToInt32($header, 60)
            if ($header[0] -ne 0x4D -or $header[1] -ne 0x5A -or $pe -lt 64 -or
                $pe -gt [Math]::Min($total, $header.Length) - 26 -or
                [BitConverter]::ToUInt32($header, $pe) -ne 0x4550 -or
                [BitConverter]::ToUInt16($header, $pe + 24) -ne 0x20B) {
                throw 'BOOTSTRAP_BINARY_FORMAT'
            }
            $machine = [BitConverter]::ToUInt16($header, $pe + 4)
            $expectedMachine = if ($request.Architecture -eq 'amd64') { 0x8664 } else { 0xAA64 }
            if ($machine -ne $expectedMachine) { throw 'BOOTSTRAP_ARCHITECTURE' }
            if ($output) { $output.Flush($true) }
            Assert-Budget
            return [pscustomobject]@{
                ArchiveSHA256 = $archiveHash
                ArchiveBytes = $file.Length
                BinarySHA256 = [Convert]::ToHexString($hash.GetHashAndReset()).ToLowerInvariant()
                BinaryBytes = $total
            }
        } finally {
            if ($output) { $output.Dispose() }
            if ($entryStream) { $entryStream.Dispose() }
            if ($zip) { $zip.Dispose() }
            if ($file) { $file.Dispose() }
            $hash.Dispose()
        }
    }

    function Assert-HerdrExecutable {
        if (-not $herdrExecutable) { return }
        Assert-Budget
        if ([IO.Path]::GetExtension($herdrExecutable) -ine '.exe') { throw 'BOOTSTRAP_HERDR_EXECUTABLE' }
        if ([IO.DriveInfo]::new([IO.Path]::GetPathRoot($herdrExecutable)).DriveType -ne [IO.DriveType]::Fixed) {
            throw 'BOOTSTRAP_LOCAL_DISK'
        }
        Assert-NoReparse $herdrExecutable
        if (-not [IO.File]::Exists($herdrExecutable)) { throw 'BOOTSTRAP_HERDR_EXECUTABLE' }
    }

    function Get-ExistingRunner {
        Assert-Budget
        $fullName = $request.ExistingTaskName
        if ($fullName.Length -gt 200 -or $fullName -cnotmatch '^\\[A-Za-z0-9_][A-Za-z0-9 _.-]*(?:\\[A-Za-z0-9_][A-Za-z0-9 _.-]*)*\z' -or
            @($fullName.Substring(1).Split('\') | Where-Object { $_.Length -gt 80 -or $_.EndsWith('.') -or $_.EndsWith(' ') }).Count -ne 0) {
            throw 'BOOTSTRAP_TASK_NAME'
        }
        $separator = $fullName.LastIndexOf('\')
        $taskPath = $fullName.Substring(0, $separator + 1)
        $taskName = $fullName.Substring($separator + 1)
        try { $null = Get-Command Get-ScheduledTask, Start-ScheduledTask -ErrorAction Stop }
        catch { throw 'BOOTSTRAP_TASK_TOOLS' }
        try { $tasks = @(Get-ScheduledTask -TaskPath $taskPath -TaskName $taskName -ErrorAction Stop) }
        catch { throw 'BOOTSTRAP_TASK_UNAVAILABLE' }
        if ($tasks.Count -ne 1) { throw 'BOOTSTRAP_TASK_UNAVAILABLE' }
        $task = $tasks[0]
        if ($task.TaskPath -cne $taskPath -or $task.TaskName -cne $taskName -or
            @($task.Actions).Count -ne 1 -or $task.Actions[0].CimClass.CimClassName -cne 'MSFT_TaskExecAction') {
            throw 'BOOTSTRAP_TASK_ACTION'
        }
        $exec = $task.Actions[0]
        if ($exec.Execute -ine (Join-Path $install 'herdr-mesh.exe') -or
            $exec.Arguments -cne $nodeCommandLine -or
            ($exec.WorkingDirectory -and $exec.WorkingDirectory -ine $install)) {
            throw 'BOOTSTRAP_TASK_ACTION'
        }
        if ($task.Settings.MultipleInstances.ToString() -cne 'IgnoreNew') { throw 'BOOTSTRAP_TASK_INSTANCES' }
        if ($task.Settings.Enabled -ne $true -or $task.Settings.AllowDemandStart -ne $true -or
            $task.Settings.ExecutionTimeLimit -cne 'PT0S' -or
            $task.Principal.LogonType.ToString() -cnotin @('Password', 'S4U', 'ServiceAccount')) {
            throw 'BOOTSTRAP_TASK_SETTINGS'
        }
        if ($task.State.ToString() -cnotin @('Ready', 'Running', 'Queued')) { throw 'BOOTSTRAP_TASK_STATE' }
        Assert-HerdrExecutable
        Assert-Budget
        return $task
    }

    function New-StagedReceipt([string]$InstallOperationId) {
        return [ordered]@{
            SchemaVersion = 1
            Status = 'staged-not-started'
            OperationId = $InstallOperationId
            InstallDirectory = $install
            StateDirectory = $state
            Executable = Join-Path $install 'herdr-mesh.exe'
            NodeArguments = $nodeArguments
            ArchiveSHA256 = $request.ExpectedSHA256
            BinarySHA256 = $request.BinarySHA256
            Architecture = $request.Architecture
            Startup = 'not-attempted'
            Enrollment = 'not-checked'
            Connectivity = 'not-checked'
            RunnerRequired = $true
        }
    }

    function Get-VerifiedInstall {
        Assert-PrivateDirectory $install
        $installedArchive = Join-Path $install 'release.zip'
        $installedBinary = Join-Path $install 'herdr-mesh.exe'
        $receiptPath = Join-Path $install 'bootstrap-receipt.json'
        foreach ($path in @($installedArchive, $installedBinary, $receiptPath)) { Assert-NoReparse $path }
        $file = [IO.File]::Open($receiptPath, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
        try {
            if ($file.Length -le 0 -or $file.Length -gt 16KB) { throw 'BOOTSTRAP_INSTALLED_MISMATCH' }
            $reader = [IO.StreamReader]::new($file, [Text.Encoding]::UTF8, $true, 1024, $true)
            try { $record = ConvertFrom-Json $reader.ReadToEnd() -AsHashtable }
            finally { $reader.Dispose() }
        } finally { $file.Dispose() }
        if ($record.OperationId -cnotmatch '^[0-9a-f]{32}$') { throw 'BOOTSTRAP_INSTALLED_MISMATCH' }
        $expected = New-StagedReceipt $record.OperationId
        foreach ($key in $expected.Keys) {
            if (-not $record.ContainsKey($key) -or
                (ConvertTo-Json -InputObject $record[$key] -Compress -Depth 4) -cne
                (ConvertTo-Json -InputObject $expected[$key] -Compress -Depth 4)) {
                throw 'BOOTSTRAP_INSTALLED_MISMATCH'
            }
        }
        $info = Get-ArchiveInfo $installedArchive
        if ($info.BinarySHA256 -cne $request.BinarySHA256) { throw 'BOOTSTRAP_INSTALLED_MISMATCH' }
        $file = [IO.File]::Open($installedBinary, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
        $hash = [Security.Cryptography.IncrementalHash]::CreateHash([Security.Cryptography.HashAlgorithmName]::SHA256)
        try {
            if ($file.Length -ne $info.BinaryBytes) { throw 'BOOTSTRAP_INSTALLED_MISMATCH' }
            $buffer = [byte[]]::new(64KB)
            while (($count = $file.Read($buffer, 0, $buffer.Length)) -gt 0) {
                Assert-Budget
                $hash.AppendData($buffer, 0, $count)
            }
            if ([Convert]::ToHexString($hash.GetHashAndReset()).ToLowerInvariant() -cne $request.BinarySHA256) {
                throw 'BOOTSTRAP_INSTALLED_MISMATCH'
            }
        } finally {
            $hash.Dispose()
            $file.Dispose()
        }
        Assert-Budget
        return [pscustomobject]$expected
    }

    if (-not [OperatingSystem]::IsWindows() -or $PSVersionTable.PSVersion -lt [version]'7.4') {
        throw 'BOOTSTRAP_PLATFORM'
    }
    if ($request.ExpectedSHA256 -cnotmatch '^[0-9a-fA-F]{64}$' -or
        $request.Architecture -notin @('amd64', 'arm64') -or
        $request.RemainingMilliseconds -le 0 -or $request.RemainingMilliseconds -gt 1800000) {
        throw 'BOOTSTRAP_INPUT'
    }
    Assert-Budget
    if ($Action -eq 'validate') {
        return Get-ArchiveInfo $request.ArchivePath
    }

    $install = Get-LocalDirectoryPath $request.InstallDirectory
    $state = Get-LocalDirectoryPath $request.StateDirectory
    $herdrExecutable = ''
    if ($request.HerdrExecutable) { $herdrExecutable = Get-LocalDirectoryPath $request.HerdrExecutable }
    Assert-HerdrExecutable
    $nodeArguments = @('node', '-server', $request.Server, '-state-dir', $state)
    # Values passed the bounded path/address grammar; this is not a shell command.
    $nodeCommandLine = 'node -server "' + $request.Server + '" -state-dir "' + $state + '"'
    if ($herdrExecutable) {
        $nodeArguments += @('-herdr-executable', $herdrExecutable)
        $nodeCommandLine += ' -herdr-executable "' + $herdrExecutable + '"'
    }
    if ($install.Equals($state, [StringComparison]::OrdinalIgnoreCase) -or
        $install.StartsWith("$state\", [StringComparison]::OrdinalIgnoreCase) -or
        $state.StartsWith("$install\", [StringComparison]::OrdinalIgnoreCase)) {
        throw 'BOOTSTRAP_STATE_OVERLAP'
    }
    if ($request.OperationId -cnotmatch '^[0-9a-f]{32}$' -or
        $request.ArchiveBytes -le 0 -or $request.ArchiveBytes -gt $maxArchive -or
        $request.BinarySHA256 -cnotmatch '^[0-9a-f]{64}$') {
        throw 'BOOTSTRAP_INPUT'
    }
    foreach ($path in @($install, $state)) {
        if ([IO.DriveInfo]::new([IO.Path]::GetPathRoot($path)).DriveType -ne [IO.DriveType]::Fixed) {
            throw 'BOOTSTRAP_LOCAL_DISK'
        }
        Assert-NoReparse $path
    }
    $nativeArchitecture = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
    if (($request.Architecture -eq 'amd64' -and $nativeArchitecture -ne 'X64') -or
        ($request.Architecture -eq 'arm64' -and $nativeArchitecture -ne 'Arm64')) {
        throw 'BOOTSTRAP_ARCHITECTURE'
    }
    $parent = [IO.Path]::GetDirectoryName($install)
    Assert-PrivateDirectory $parent
    Assert-PrivateDirectory $state
    $installExists = [IO.File]::Exists($install) -or [IO.Directory]::Exists($install)
    if ($installExists -and -not ($request.Start -and $Action -in @('prepare', 'start'))) {
        throw 'BOOTSTRAP_EXISTS'
    }
    $stage = Join-Path $parent ".herdr-bootstrap-$($request.OperationId)"
    if ($state.Equals($stage, [StringComparison]::OrdinalIgnoreCase) -or
        $state.StartsWith("$stage\", [StringComparison]::OrdinalIgnoreCase)) {
        throw 'BOOTSTRAP_STATE_OVERLAP'
    }
    Assert-NoReparse $stage
    $archive = Join-Path $stage 'release.zip'
    $binary = Join-Path $stage 'herdr-mesh.exe'
    Assert-Budget

    switch ($Action) {
        'prepare' {
            if ($request.Start) { $null = Get-ExistingRunner }
            if ($installExists) {
                return [pscustomobject]@{ Status = 'installed'; OperationId = $request.OperationId; Receipt = (Get-VerifiedInstall) }
            }
            if ([IO.File]::Exists($stage) -or [IO.Directory]::Exists($stage)) { throw 'BOOTSTRAP_EXISTS' }
            New-Item -ItemType Directory -Path $stage -ErrorAction Stop | Out-Null
            Assert-PrivateDirectory $stage
            $file = [IO.File]::Open($archive, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
            $file.Dispose()
            return [pscustomobject]@{ Status = 'prepared'; OperationId = $request.OperationId }
        }
        'upload' {
            Assert-PrivateDirectory $stage
            Assert-NoReparse $archive
            if ($request.Data.Length -gt ($chunkSize / 3 * 4)) { throw 'BOOTSTRAP_CHUNK' }
            try { $bytes = [Convert]::FromBase64String($request.Data) }
            catch { throw 'BOOTSTRAP_CHUNK' }
            if ($bytes.Length -le 0 -or $bytes.Length -gt $chunkSize -or $request.Offset -lt 0 -or
                $request.Offset + $bytes.Length -gt $request.ArchiveBytes) {
                throw 'BOOTSTRAP_CHUNK'
            }
            $file = [IO.File]::Open($archive, [IO.FileMode]::Open, [IO.FileAccess]::Write, [IO.FileShare]::None)
            try {
                if ($file.Length -ne $request.Offset) { throw 'BOOTSTRAP_OFFSET' }
                $file.Position = $file.Length
                Assert-Budget
                $file.Write($bytes, 0, $bytes.Length)
                $file.Flush($true)
                return [pscustomobject]@{ Status = 'uploaded'; Bytes = $file.Length }
            } finally { $file.Dispose() }
        }
        'commit' {
            if ($request.Start) { $null = Get-ExistingRunner }
            Assert-PrivateDirectory $stage
            Assert-NoReparse $archive
            if ([IO.FileInfo]::new($archive).Length -ne $request.ArchiveBytes) { throw 'BOOTSTRAP_ARCHIVE_SIZE' }
            # Static verification never executes the payload, including "version".
            $info = Get-ArchiveInfo $archive $binary
            if ($info.BinarySHA256 -cne $request.BinarySHA256) { throw 'BOOTSTRAP_HASH' }
            $receipt = New-StagedReceipt $request.OperationId
            $receiptPath = Join-Path $stage 'bootstrap-receipt.json'
            $file = [IO.File]::Open($receiptPath, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
            try {
                $bytes = [Text.Encoding]::UTF8.GetBytes(($receipt | ConvertTo-Json -Depth 4))
                $file.Write($bytes, 0, $bytes.Length)
                $file.Flush($true)
            } finally { $file.Dispose() }
            if ($request.Start) { $null = Get-ExistingRunner }
            Assert-HerdrExecutable
            Assert-Budget
            # Same-parent rename publishes a complete directory, never replaces one.
            [IO.Directory]::Move($stage, $install)
            return [pscustomobject]$receipt
        }
        'start' {
            if (-not $request.Start) { throw 'BOOTSTRAP_RUNNER_REQUIRED' }
            $installed = Get-VerifiedInstall
            $task = Get-ExistingRunner
            $disposition = 'reused-running'
            if ($task.State.ToString() -ceq 'Ready') {
                Assert-Budget
                # Exactly one dispatch. A failure can still mean it reached the scheduler.
                try { Start-ScheduledTask -InputObject $task -ErrorAction Stop | Out-Null }
                catch { throw 'BOOTSTRAP_TASK_START' }
                $disposition = 'requested-once'
            } elseif ($task.State.ToString() -ceq 'Queued') {
                $disposition = 'observed-pending'
            }
            $observation = [Diagnostics.Stopwatch]::StartNew()
            do {
                $fresh = Get-ExistingRunner
                if ($fresh.State.ToString() -ceq 'Running') {
                    return [pscustomobject]@{
                        Status = 'runner-running-mesh-unverified'
                        OperationId = $request.OperationId
                        InstallOperationId = $installed.OperationId
                        ExistingTaskName = $request.ExistingTaskName
                        Startup = $disposition
                        RunnerState = 'Running'
                        RunnerObservedAtUtc = [DateTimeOffset]::UtcNow.ToString('O')
                        Enrollment = 'not-checked'
                        Connectivity = 'not-checked'
                    }
                }
                Assert-Budget
                [Threading.Thread]::Sleep(100)
            } while ($observation.Elapsed.TotalSeconds -lt 10)
            throw 'BOOTSTRAP_TASK_OBSERVATION'
        }
    }
}

function Assert-HerdrBootstrapBudget {
    param([Diagnostics.Stopwatch]$Clock, [int]$TimeoutSeconds, [Threading.CancellationToken]$CancellationToken)
    if ($CancellationToken.IsCancellationRequested) { throw 'BOOTSTRAP_CANCELLED' }
    if ($Clock.Elapsed.TotalSeconds -ge $TimeoutSeconds) { throw 'BOOTSTRAP_TIMEOUT' }
}

function Invoke-HerdrBootstrapRequest {
    param(
        $Session,
        [ValidateSet('prepare', 'upload', 'commit', 'start')][string]$Action,
        [hashtable]$Request,
        [Diagnostics.Stopwatch]$Clock,
        [int]$TimeoutSeconds,
        [Threading.CancellationToken]$CancellationToken
    )
    Assert-HerdrBootstrapBudget $Clock $TimeoutSeconds $CancellationToken
    $Request.RemainingMilliseconds = [Math]::Floor($TimeoutSeconds * 1000 - $Clock.Elapsed.TotalMilliseconds)
    if ($Request.RemainingMilliseconds -le 0) { throw 'BOOTSTRAP_TIMEOUT' }
    $json = ConvertTo-Json -InputObject $Request -Compress -Depth 4
    $job = $null
    try {
        # The script is fixed; operator input is serialized data, never PowerShell source.
        $job = Invoke-Command -Session $Session -ScriptBlock $script:HerdrBootstrapProgram `
            -ArgumentList @($Action, $json) -AsJob -ErrorAction Stop
        while ($job.State -notin @('Completed', 'Failed', 'Stopped')) {
            Assert-HerdrBootstrapBudget $Clock $TimeoutSeconds $CancellationToken
            $null = $job.Finished.WaitOne(100)
        }
        Assert-HerdrBootstrapBudget $Clock $TimeoutSeconds $CancellationToken
        $result = @(Receive-Job -Job $job -ErrorAction Stop)
        if ($job.State -ne 'Completed' -or $result.Count -ne 1) { throw 'BOOTSTRAP_TRANSPORT' }
        return $result[0]
    } finally {
        if ($job) {
            # Best effort only: stopping a remoting job is not transactional rollback.
            if ($job.State -notin @('Completed', 'Failed', 'Stopped')) { Stop-Job -Job $job -ErrorAction Stop }
            Remove-Job -Job $job -ErrorAction Stop
        }
    }
}

function Invoke-HerdrBootstrapCore {
    param(
        $Session,
        [string]$ArchivePath,
        [string]$ExpectedSHA256,
        [string]$Architecture,
        [string]$InstallDirectory,
        [string]$StateDirectory,
        [string]$Server,
        [string]$HerdrExecutable,
        [int]$TimeoutSeconds = 300,
        [Threading.CancellationToken]$CancellationToken = [Threading.CancellationToken]::None,
        [switch]$Start,
        [string]$ExistingTaskName
    )
    $ErrorActionPreference = 'Stop'
    Set-StrictMode -Version Latest
    $clock = [Diagnostics.Stopwatch]::StartNew()
    $phase = 'validation'
    $dispatched = $false
    $startDispatched = $false
    $operationId = [Guid]::NewGuid().ToString('N')
    $archiveStream = $null
    try {
        if ($PSBoundParameters.ContainsKey('HerdrExecutable') -and [string]::IsNullOrWhiteSpace($HerdrExecutable)) {
            throw 'BOOTSTRAP_HERDR_EXECUTABLE'
        }
        if ($Start -and -not $ExistingTaskName) { throw 'BOOTSTRAP_RUNNER_REQUIRED' }
        if (-not $Start -and $ExistingTaskName) { throw 'BOOTSTRAP_RUNNER_REQUIRED' }
        if ($Start -and ($ExistingTaskName.Length -gt 200 -or
            $ExistingTaskName -cnotmatch '^\\[A-Za-z0-9_][A-Za-z0-9 _.-]*(?:\\[A-Za-z0-9_][A-Za-z0-9 _.-]*)*\z' -or
            @($ExistingTaskName.Substring(1).Split('\') | Where-Object { $_.Length -gt 80 -or $_.EndsWith('.') -or $_.EndsWith(' ') }).Count -ne 0)) {
            throw 'BOOTSTRAP_TASK_NAME'
        }
        if ($ExpectedSHA256 -cnotmatch '^[0-9a-fA-F]{64}$' -or
            $Architecture -notin @('amd64', 'arm64') -or $TimeoutSeconds -lt 1 -or $TimeoutSeconds -gt 1800) {
            throw 'BOOTSTRAP_INPUT'
        }
        if (-not [OperatingSystem]::IsWindows()) { throw 'BOOTSTRAP_PLATFORM' }
        # This intentionally accepts only DNS/IPv4 host:port, not URLs or command fragments.
        if ($Server -cnotmatch '^([A-Za-z0-9][A-Za-z0-9.-]{0,252}):([0-9]{1,5})\z') { throw 'BOOTSTRAP_SERVER' }
        $hostName = $Matches[1]
        $port = [int]$Matches[2]
        if ($port -lt 1 -or $port -gt 65535) { throw 'BOOTSTRAP_SERVER' }
        foreach ($label in $hostName.Split('.')) {
            if ($label -cnotmatch '^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$') { throw 'BOOTSTRAP_SERVER' }
        }
        Assert-HerdrBootstrapBudget $clock $TimeoutSeconds $CancellationToken
        $archivePathFull = [IO.Path]::GetFullPath($ArchivePath)
        if ($archivePathFull -cnotmatch '^[A-Za-z]:\\' -or
            [IO.DriveInfo]::new([IO.Path]::GetPathRoot($archivePathFull)).DriveType -ne [IO.DriveType]::Fixed) {
            throw 'BOOTSTRAP_LOCAL_ARCHIVE'
        }
        # Keep a read-only, non-write-sharing handle throughout validation and upload.
        $archiveStream = [IO.File]::Open($archivePathFull,
            [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
        $validation = @{
            ArchivePath = $archivePathFull
            ExpectedSHA256 = $ExpectedSHA256
            Architecture = $Architecture
            RemainingMilliseconds = [Math]::Floor($TimeoutSeconds * 1000 - $clock.Elapsed.TotalMilliseconds)
        }
        if ($validation.RemainingMilliseconds -le 0) { throw 'BOOTSTRAP_TIMEOUT' }
        $info = & $script:HerdrBootstrapProgram 'validate' (ConvertTo-Json $validation -Compress) $CancellationToken
        $request = @{
            ExpectedSHA256 = $ExpectedSHA256.ToLowerInvariant()
            BinarySHA256 = $info.BinarySHA256
            Architecture = $Architecture
            ArchiveBytes = $info.ArchiveBytes
            InstallDirectory = $InstallDirectory
            StateDirectory = $StateDirectory
            Server = $Server
            HerdrExecutable = $HerdrExecutable
            OperationId = $operationId
            Start = $Start.IsPresent
            ExistingTaskName = $ExistingTaskName
        }
        $transport = @{
            Session = $Session
            Clock = $clock
            TimeoutSeconds = $TimeoutSeconds
            CancellationToken = $CancellationToken
        }
        $phase = 'prepare'
        Assert-HerdrBootstrapBudget $clock $TimeoutSeconds $CancellationToken
        $dispatched = $true
        $reply = Invoke-HerdrBootstrapRequest @transport -Action prepare -Request $request
        if ($reply.OperationId -cne $operationId) { throw 'BOOTSTRAP_TRANSPORT' }
        $installOperationId = $operationId
        if ($reply.Status -ceq 'installed' -and $Start) {
            $reply = $reply.Receipt
            $installOperationId = $reply.OperationId
            if ($installOperationId -cnotmatch '^[0-9a-f]{32}$') { throw 'BOOTSTRAP_TRANSPORT' }
        } elseif ($reply.Status -ceq 'prepared') {
            $phase = 'upload'
            $buffer = [byte[]]::new(192KB)
            [long]$offset = 0
            while (($count = $archiveStream.Read($buffer, 0, $buffer.Length)) -gt 0) {
                Assert-HerdrBootstrapBudget $clock $TimeoutSeconds $CancellationToken
                $chunk = $request.Clone()
                $chunk.Offset = $offset
                $chunk.Data = [Convert]::ToBase64String($buffer, 0, $count)
                $reply = Invoke-HerdrBootstrapRequest @transport -Action upload -Request $chunk
                $offset += $count
                if ($reply.Status -cne 'uploaded' -or $reply.Bytes -ne $offset) { throw 'BOOTSTRAP_TRANSPORT' }
            }
            if ($offset -ne $info.ArchiveBytes) { throw 'BOOTSTRAP_ARCHIVE_SIZE' }
            $phase = 'commit'
            Assert-HerdrBootstrapBudget $clock $TimeoutSeconds $CancellationToken
            $reply = Invoke-HerdrBootstrapRequest @transport -Action commit -Request $request
        } else {
            throw 'BOOTSTRAP_TRANSPORT'
        }
        Assert-HerdrBootstrapBudget $clock $TimeoutSeconds $CancellationToken
        $argumentCount = if ($HerdrExecutable) { 7 } else { 5 }
        if ($reply.Status -cne 'staged-not-started' -or $reply.OperationId -cne $installOperationId -or
            $reply.SchemaVersion -ne 1 -or $reply.Architecture -cne $Architecture -or
            -not $reply.InstallDirectory.Equals($InstallDirectory, [StringComparison]::OrdinalIgnoreCase) -or
            -not $reply.StateDirectory.Equals($StateDirectory, [StringComparison]::OrdinalIgnoreCase) -or
            -not $reply.Executable.Equals((Join-Path $InstallDirectory 'herdr-mesh.exe'), [StringComparison]::OrdinalIgnoreCase) -or
            @($reply.NodeArguments).Count -ne $argumentCount -or
            $reply.NodeArguments[0] -cne 'node' -or $reply.NodeArguments[1] -cne '-server' -or
            $reply.NodeArguments[2] -cne $Server -or $reply.NodeArguments[3] -cne '-state-dir' -or
            -not $reply.NodeArguments[4].Equals($StateDirectory, [StringComparison]::OrdinalIgnoreCase) -or
            ($HerdrExecutable -and ($reply.NodeArguments[5] -cne '-herdr-executable' -or
                -not $reply.NodeArguments[6].Equals($HerdrExecutable, [StringComparison]::OrdinalIgnoreCase))) -or
            $reply.ArchiveSHA256 -cne $request.ExpectedSHA256 -or $reply.BinarySHA256 -cne $info.BinarySHA256 -or
            $reply.Startup -cne 'not-attempted' -or $reply.Enrollment -cne 'not-checked' -or
            $reply.Connectivity -cne 'not-checked' -or $reply.RunnerRequired -ne $true) {
            throw 'BOOTSTRAP_TRANSPORT'
        }
        $result = $reply | Select-Object SchemaVersion, Status, OperationId, InstallDirectory, StateDirectory,
            Executable, NodeArguments, ArchiveSHA256, BinarySHA256, Architecture, Startup,
            Enrollment, Connectivity, RunnerRequired
        if ($Start) {
            $phase = 'start'
            Assert-HerdrBootstrapBudget $clock $TimeoutSeconds $CancellationToken
            $startDispatched = $true
            $started = Invoke-HerdrBootstrapRequest @transport -Action start -Request $request
            Assert-HerdrBootstrapBudget $clock $TimeoutSeconds $CancellationToken
            [DateTimeOffset]$observedAt = [DateTimeOffset]::MinValue
            if ($started.Status -cne 'runner-running-mesh-unverified' -or
                $started.OperationId -cne $operationId -or $started.InstallOperationId -cne $installOperationId -or
                $started.ExistingTaskName -cne $ExistingTaskName -or $started.RunnerState -cne 'Running' -or
                $started.Startup -cnotin @('requested-once', 'reused-running', 'observed-pending') -or
                -not [DateTimeOffset]::TryParse($started.RunnerObservedAtUtc, [ref]$observedAt) -or
                $started.Enrollment -cne 'not-checked' -or $started.Connectivity -cne 'not-checked') {
                throw 'BOOTSTRAP_TRANSPORT'
            }
            $result.Status = $started.Status
            $result.OperationId = $operationId
            $result.Startup = $started.Startup
            $result.RunnerRequired = $false
            $result | Add-Member -NotePropertyMembers @{
                InstallOperationId = $installOperationId
                ExistingTaskName = $ExistingTaskName
                RunnerState = 'Running'
                RunnerObservedAtUtc = $observedAt.ToString('O')
            }
        }
        return $result
    } catch {
        $code = 'BOOTSTRAP_IO'
        if ($_.Exception.GetBaseException() -is [IO.InvalidDataException]) { $code = 'BOOTSTRAP_ARCHIVE_FORMAT' }
        elseif ($_.Exception.Message -match '\bBOOTSTRAP_[A-Z_]+\b') { $code = $Matches[0] }
        $advice = switch ($code) {
            'BOOTSTRAP_RUNNER_REQUIRED' { 'Use -Start together with -ExistingTaskName for an operator-prepared Scheduled Task, or omit both to stage only. No runner is created or modified.' }
            'BOOTSTRAP_TASK_NAME' { 'Specify one full task name such as \HerdrMesh\Node, with ordinary ASCII components and no wildcards, expressions, trailing dots or spaces.' }
            'BOOTSTRAP_TASK_TOOLS' { 'The target must already provide the Windows ScheduledTasks Get-ScheduledTask and Start-ScheduledTask commands.' }
            'BOOTSTRAP_TASK_UNAVAILABLE' { 'The explicitly named task must already exist and be readable by the connected account. No task will be registered or repaired.' }
            'BOOTSTRAP_TASK_ACTION' { 'The existing task must have one direct Exec action for this exact installed binary and canonical node/server/state arguments, including the optional Herdr executable when supplied, with no wrapper or extra action.' }
            'BOOTSTRAP_HERDR_EXECUTABLE' { 'When supplied, -HerdrExecutable must name an existing target-side local .exe file accessible to the intended account. Prepare Herdr separately; bootstrap does not install or verify its capabilities.' }
            'BOOTSTRAP_TASK_INSTANCES' { 'The existing task must use the IgnoreNew multiple-instance policy; parallel, queued-duplicate and stop-existing policies are refused.' }
            'BOOTSTRAP_TASK_SETTINGS' { 'The existing task must be enabled, allow demand start, use PT0S (no execution time limit), and use Password, S4U or ServiceAccount logon rather than an interactive session.' }
            'BOOTSTRAP_TASK_STATE' { 'The task must report Ready, Running or Queued. Disabled or unknown runner states are not usable.' }
            'BOOTSTRAP_INSTALLED_MISMATCH' { 'An existing install may only be reused with -Start when its receipt, archive and binary match the trusted release and intended node arguments. Nothing is overwritten.' }
            'BOOTSTRAP_TASK_START' { 'The one-shot scheduler start did not acknowledge cleanly. Inspect the existing task and normal mesh inventory; do not automatically retry.' }
            'BOOTSTRAP_TASK_OBSERVATION' { 'A fresh Running task state was not observed within ten seconds. The task may still start or may already have exited; inspect before retrying.' }
            'BOOTSTRAP_INPUT' { 'Supply a mandatory trusted 64-hex SHA256, windows amd64/arm64 architecture, and timeout of 1..1800 seconds.' }
            'BOOTSTRAP_SERVER' { 'Supply a DNS name or IPv4 address with port 1..65535, not a URL or shell expression.' }
            'BOOTSTRAP_PATH' { 'Use absolute local drive paths, at most 200 ASCII characters; no root, trailing separator, dot segments, devices, or shell metacharacters.' }
            'BOOTSTRAP_STATE_OVERLAP' { 'Install, staging, and persistent state directories must be disjoint. Choose a new versioned install path.' }
            'BOOTSTRAP_DIRECTORY_REQUIRED' { 'The operator must first provision an existing install parent and separate persistent state directory.' }
            'BOOTSTRAP_PRIVATE_DIRECTORY' { 'Use directories owned by the connected account, SYSTEM, or Administrators, with access allowed only to those identities.' }
            'BOOTSTRAP_REPARSE' { 'Use real directories/files without junctions, symlinks, or other reparse points in their ancestry.' }
            'BOOTSTRAP_LOCAL_DISK' { 'Use fixed local disks, not network shares, mapped network drives, or removable storage.' }
            'BOOTSTRAP_LOCAL_ARCHIVE' { 'Copy the trusted release ZIP to a fixed local drive first; UNC/device paths and network/removable drives are not accepted.' }
            'BOOTSTRAP_EXISTS' { 'Nothing is overwritten. Choose an absent versioned install directory; preserve existing state.' }
            'BOOTSTRAP_HASH' { 'Archive or extracted binary hash mismatch. Re-obtain the local release and expected checksum through a trusted channel.' }
            'BOOTSTRAP_ARCHIVE_FORMAT' { 'Supply the Windows ZIP produced by scripts\build-release.ps1.' }
            'BOOTSTRAP_ARCHIVE_LAYOUT' { 'The ZIP must contain only the regular root entry herdr-mesh.exe, without directories, links, or extra entries.' }
            'BOOTSTRAP_ARCHIVE_SIZE' { 'The complete archive must be nonempty and at most 256 MiB.' }
            'BOOTSTRAP_BINARY_SIZE' { 'The executable must have a consistent expanded size between 256 bytes and 512 MiB.' }
            'BOOTSTRAP_BINARY_FORMAT' { 'The payload must be a Windows PE32+ executable with a standard PE header in its first 4096 bytes.' }
            'BOOTSTRAP_ARCHITECTURE' { 'Select the native Windows amd64 or arm64 release matching the remote OS.' }
            'BOOTSTRAP_PLATFORM' { 'Use Windows and PowerShell 7.4+ on both ends, with an already-open compatible PSSession.' }
            'BOOTSTRAP_CHUNK' { 'Upload data exceeded the fixed chunk/size boundary or was malformed. Do not replay the failed operation.' }
            'BOOTSTRAP_OFFSET' { 'Remote upload offset differs from the expected value. Do not append or replay automatically.' }
            'BOOTSTRAP_CANCELLED' { 'Cancellation requested. Inspect the named remote paths before any retry.' }
            'BOOTSTRAP_TIMEOUT' { 'The operation deadline expired. Inspect the named remote paths before any retry.' }
            'BOOTSTRAP_TRANSPORT' { 'The existing PSSession did not return the expected fixed-operation receipt. Inspect the remote install before retrying.' }
            default {
                $code = 'BOOTSTRAP_IO'
                'Check the existing PSSession, endpoint permissions, disk space, and local release readability. Remote diagnostics are not copied into this error.'
            }
        }
        $outcome = if ($startDispatched) { 'start-unknown' } elseif ($dispatched) { 'indeterminate' } else { 'not-installed' }
        $startup = if ($startDispatched) {
            'A task start may have been dispatched; no automatic retry is made. Mesh and provider readiness are unverified.'
        } else { 'This operation did not request a task start.' }
        $exception = [InvalidOperationException]::new("$code during $phase. $advice Outcome: $outcome. $startup Operation: $operationId.")
        $exception.Data['BootstrapOutcome'] = $outcome
        $exception.Data['BootstrapPhase'] = $phase
        $exception.Data['OperationId'] = $operationId
        if ($startDispatched) {
            $exception.Data['BootstrapResult'] = [pscustomobject]@{
                Status = 'start-unknown'
                OperationId = $operationId
                InstallOperationId = $installOperationId
                ExistingTaskName = $ExistingTaskName
                Startup = 'unknown'
                RunnerState = 'unknown'
                Enrollment = 'not-checked'
                Connectivity = 'not-checked'
            }
        }
        throw $exception
    } finally {
        if ($archiveStream) { $archiveStream.Dispose() }
    }
}

function Invoke-HerdrNodeBootstrap {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][System.Management.Automation.Runspaces.PSSession]$Session,
        [Parameter(Mandatory)][string]$ArchivePath,
        [Parameter(Mandatory)][string]$ExpectedSHA256,
        [Parameter(Mandatory)][ValidateSet('amd64', 'arm64')][string]$Architecture,
        [Parameter(Mandatory)][string]$InstallDirectory,
        [Parameter(Mandatory)][string]$StateDirectory,
        [Parameter(Mandatory)][string]$Server,
        [ValidateNotNullOrEmpty()][string]$HerdrExecutable,
        [ValidateRange(1, 1800)][int]$TimeoutSeconds = 300,
        [Threading.CancellationToken]$CancellationToken = [Threading.CancellationToken]::None,
        [switch]$Start,
        [string]$ExistingTaskName
    )
    if ($null -eq $Session -or $Session.State -ne 'Opened' -or $Session.Availability -ne 'Available') {
        throw 'An existing open, available operator-established PSSession is required. This command does not connect, authenticate, or enroll.'
    }
    Invoke-HerdrBootstrapCore @PSBoundParameters
}
