param([string]$OptionsPath)

function Get-HerdrBootstrapSSHConfig {
    Join-Path ([Environment]::GetFolderPath('UserProfile')) '.ssh\config'
}

function Invoke-HerdrBootstrapEntry {
    param([string]$OptionsPath)
    $ErrorActionPreference = 'Stop'
    Set-StrictMode -Version Latest
    $clock = [Diagnostics.Stopwatch]::StartNew()
    $phase = 'validation'
    $session = $null
    $effectsPossible = $false
    $startRequested = $false
    $envelope = $null
    try {
        if (-not [OperatingSystem]::IsWindows() -or $PSVersionTable.PSVersion -lt [version]'7.4') {
            throw 'BOOTSTRAP_PLATFORM'
        }
        $file = [IO.File]::OpenRead($OptionsPath)
        try {
            if ($file.Length -le 0 -or $file.Length -gt 32KB) { throw 'BOOTSTRAP_INPUT' }
            $reader = [IO.StreamReader]::new($file)
            try { $options = ConvertFrom-Json $reader.ReadToEnd() -AsHashtable }
            finally { $reader.Dispose() }
        } finally { $file.Dispose() }
        if ($options.SSHHost -cnotmatch '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}\z' -or
            $options.TimeoutSeconds -lt 1 -or $options.TimeoutSeconds -gt 1800) { throw 'BOOTSTRAP_INPUT' }
        $startRequested = [bool]$options.Start
        $null = & $script:HerdrBootstrapProgram 'validate' (ConvertTo-Json -Compress @{
            ArchivePath = $options.ArchivePath
            ExpectedSHA256 = $options.ExpectedSHA256
            Architecture = $options.Architecture
            RemainingMilliseconds = $options.TimeoutSeconds * 1000
        })
        $phase = 'connection'
        try { $null = Get-Command ssh -CommandType Application -ErrorAction Stop }
        catch { throw 'SSH_MISSING' }
        try {
            $config = Get-HerdrBootstrapSSHConfig
            $file = [IO.File]::Open($config, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
            try {
                if ($file.Length -le 0 -or $file.Length -gt 64KB) { throw 'SSH_CONFIG' }
                $reader = [IO.StreamReader]::new($file)
                try { $text = $reader.ReadToEnd() } finally { $reader.Dispose() }
            } finally { $file.Dispose() }
            $aliasFound = $false
            foreach ($line in $text.Split("`n")) {
                $line = ($line -split '#', 2)[0]
                if ($line -match '^\s*(Include|Match|SendEnv|SetEnv)\s+') { throw 'SSH_CONFIG' }
                if ($line -match '^\s*Host\s+(.+)$' -and
                    $options.SSHHost -iin @($Matches[1].Trim() -split '\s+')) { $aliasFound = $true }
            }
            if (-not $aliasFound) { throw 'SSH_CONFIG' }
        } catch { throw 'SSH_CONFIG' }

        $remaining = [Math]::Floor($options.TimeoutSeconds * 1000 - $clock.Elapsed.TotalMilliseconds)
        if ($remaining -le 0) { throw 'BOOTSTRAP_TIMEOUT' }
        $sshOptions = @{
            BatchMode = 'yes'
            StrictHostKeyChecking = 'yes'
            PasswordAuthentication = 'no'
            KbdInteractiveAuthentication = 'no'
            PreferredAuthentications = 'publickey'
            NumberOfPasswordPrompts = '0'
            UpdateHostKeys = 'no'
            VerifyHostKeyDNS = 'no'
            ConnectionAttempts = '1'
            ConnectTimeout = [string][Math]::Min(30, [Math]::Max(1, [Math]::Floor($remaining / 1000)))
            ServerAliveInterval = '5'
            ServerAliveCountMax = '1'
            ForwardAgent = 'no'
            ForwardX11 = 'no'
            ClearAllForwardings = 'yes'
            PermitLocalCommand = 'no'
            ProxyCommand = 'none'
            ProxyJump = 'none'
            KnownHostsCommand = 'none'
            RemoteCommand = 'none'
            RequestTTY = 'no'
            ControlMaster = 'no'
            ControlPath = 'none'
            ControlPersist = 'no'
            SendEnv = '-*'
            LogLevel = 'ERROR'
        }
        # Keep credentials out of the child SSH environment and forbid askpass.
        $allowedEnvironment = @('PATH', 'PSModulePath', 'SystemRoot', 'windir', 'SystemDrive',
            'USERPROFILE', 'HOME', 'HOMEDRIVE', 'HOMEPATH', 'APPDATA', 'LOCALAPPDATA',
            'TEMP', 'TMP', 'ProgramFiles', 'ProgramFiles(x86)', 'ProgramW6432', 'ProgramData',
            'COMSPEC', 'PATHEXT', 'OS', 'PROCESSOR_ARCHITECTURE', 'SSH_AUTH_SOCK', 'SSH_AGENT_PID')
        foreach ($name in [Environment]::GetEnvironmentVariables('Process').Keys) {
            if ($name -notin $allowedEnvironment) { [Environment]::SetEnvironmentVariable($name, $null, 'Process') }
        }
        $env:SSH_ASKPASS_REQUIRE = 'never'
        try {
            $session = New-PSSession -HostName $options.SSHHost -SSHTransport -Subsystem powershell `
                -ConnectingTimeout ([int][Math]::Min(30000, $remaining)) -Options $sshOptions -ErrorAction Stop
        } catch { throw 'SSH_CONNECTION' }
        $remainingSeconds = [Math]::Floor($options.TimeoutSeconds - $clock.Elapsed.TotalSeconds)
        if ($remainingSeconds -lt 1) { throw 'BOOTSTRAP_TIMEOUT' }
        $parameters = @{
            Session = $session
            ArchivePath = $options.ArchivePath
            ExpectedSHA256 = $options.ExpectedSHA256
            Architecture = $options.Architecture
            InstallDirectory = $options.InstallDirectory
            StateDirectory = $options.StateDirectory
            Server = $options.Server
            TimeoutSeconds = [int]$remainingSeconds
        }
        if ($options.HerdrExecutable) { $parameters.HerdrExecutable = $options.HerdrExecutable }
        if ($startRequested) {
            $parameters.Start = $true
            $parameters.ExistingTaskName = $options.ExistingTaskName
        }
        $phase = 'prepare'
        $effectsPossible = $true
        $result = Invoke-HerdrNodeBootstrap @parameters
        $projected = [ordered]@{
            schema_version = $result.SchemaVersion
            status = $result.Status
            operation_id = $result.OperationId
            install_directory = $result.InstallDirectory
            state_directory = $result.StateDirectory
            executable = $result.Executable
            node_arguments = @($result.NodeArguments)
            archive_sha256 = $result.ArchiveSHA256
            binary_sha256 = $result.BinarySHA256
            architecture = $result.Architecture
            startup = $result.Startup
            enrollment = $result.Enrollment
            connectivity = $result.Connectivity
            runner_required = $result.RunnerRequired
        }
        if ($startRequested) {
            $projected.install_operation_id = $result.InstallOperationId
            $projected.existing_task_name = $result.ExistingTaskName
            $projected.runner_state = $result.RunnerState
            $projected.runner_observed_at_utc = $result.RunnerObservedAtUtc
        }
        $envelope = @{ version = 1; ok = $true; result = $projected }
    } catch {
        $code = 'operation_failed'
        if ($_.Exception.Message -match '^(BOOTSTRAP_(INPUT|PATH|SERVER|STATE_OVERLAP|DIRECTORY_REQUIRED|PRIVATE_DIRECTORY|REPARSE|LOCAL_DISK|LOCAL_ARCHIVE|EXISTS|HASH|ARCHIVE_FORMAT|ARCHIVE_LAYOUT|ARCHIVE_SIZE|BINARY_SIZE|BINARY_FORMAT|ARCHITECTURE|PLATFORM|HERDR_EXECUTABLE|RUNNER_REQUIRED|TASK_NAME|TASK_TOOLS|TASK_UNAVAILABLE|TASK_ACTION|TASK_INSTANCES|TASK_SETTINGS|TASK_STATE|INSTALLED_MISMATCH|TASK_START|TASK_OBSERVATION|CHUNK|OFFSET|TRANSPORT|IO))\b') {
            $code = $Matches[1]
        } elseif ($_.Exception.Message -match '^BOOTSTRAP_TIMEOUT\b') { $code = 'timeout' }
        elseif ($_.Exception.Message -match '^BOOTSTRAP_CANCELLED\b') { $code = 'canceled' }
        elseif ($_.Exception.Message -eq 'SSH_MISSING') { $code = 'ssh_missing' }
        elseif ($_.Exception.Message -eq 'SSH_CONFIG') { $code = 'ssh_config' }
        elseif ($_.Exception.Message -eq 'SSH_CONNECTION') { $code = 'ssh_connection' }
        $outcome = if (-not $effectsPossible) { 'not-installed' } elseif ($startRequested) { 'start-unknown' } else { 'indeterminate' }
        if ($_.Exception.Data['BootstrapOutcome'] -in @('not-installed', 'indeterminate', 'start-unknown')) {
            $outcome = $_.Exception.Data['BootstrapOutcome']
        }
        if ($_.Exception.Data['BootstrapPhase'] -in @('validation', 'prepare', 'upload', 'commit', 'start')) {
            $phase = $_.Exception.Data['BootstrapPhase']
        }
        $id = ''
        if ($_.Exception.Data['OperationId'] -cmatch '^[0-9a-f]{32}\z') { $id = $_.Exception.Data['OperationId'] }
        $envelope = @{ version = 1; ok = $false; error = @{
            code = $code; status = $outcome; phase = $phase; operation_id = $id
        } }
    } finally {
        if ($null -ne $session) {
            try { Remove-PSSession -Session $session -ErrorAction Stop }
            catch {
                $outcome = if (-not $effectsPossible) { 'not-installed' } elseif ($startRequested) { 'start-unknown' } else { 'indeterminate' }
                $envelope = @{ version = 1; ok = $false; error = @{
                    code = 'process_failed'; status = $outcome; phase = 'cleanup'; operation_id = ''
                } }
            }
        }
    }
    return $envelope
}

if ($MyInvocation.InvocationName -ne '.') {
    $ErrorActionPreference = 'Stop'
    [Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
    $OutputEncoding = [Console]::OutputEncoding
    if ($PSVersionTable.PSVersion -lt [version]'7.4' -or -not [OperatingSystem]::IsWindows()) {
        @{ version = 1; ok = $false; error = @{
            code = 'BOOTSTRAP_PLATFORM'; status = 'not-installed'; phase = 'validation'; operation_id = ''
        } } | ConvertTo-Json -Compress
        return
    }
    . (Join-Path $PSScriptRoot 'bootstrap-node.ps1')
    Invoke-HerdrBootstrapEntry -OptionsPath $OptionsPath | ConvertTo-Json -Depth 8 -Compress
}
