[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
param(
    [Parameter(Mandatory)]
    [string]$Tailnet,

    [string]$ApiTokenEnvironmentVariable = 'TAILSCALE_API_TOKEN',

    [string]$TagOwner = 'autogroup:admin',

    [ValidateRange(3600, 7776000)]
    [int]$KeyExpirySeconds = 604800,

    [ValidateRange(1, 100)]
    [int]$KeysPerRole = 2,

    [ValidateRange(1, 65535)]
    [int]$DashboardPort,

    [Parameter(Mandatory)]
    [string]$OutputDirectory,

    [switch]$Apply,
    [switch]$Structured
)

$ErrorActionPreference = 'Stop'

function Protect-SecretFile {
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
        $mode = if ([IO.Directory]::Exists($Path)) { 448 } else { 384 }
        [IO.File]::SetUnixFileMode($Path, [IO.UnixFileMode]$mode)
        return
    }
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
    $acl = Get-Acl -LiteralPath $Path
    $acl.SetAccessRuleProtection($true, $false)
    foreach ($existingRule in @($acl.Access)) { [void]$acl.RemoveAccessRuleSpecific($existingRule) }
    $inheritance = if ([IO.Directory]::Exists($Path)) {
        [Security.AccessControl.InheritanceFlags]'ContainerInherit, ObjectInherit'
    } else { [Security.AccessControl.InheritanceFlags]::None }
    $rule = [Security.AccessControl.FileSystemAccessRule]::new(
        $identity,
        [Security.AccessControl.FileSystemRights]::FullControl,
        $inheritance,
        [Security.AccessControl.PropagationFlags]::None,
        [Security.AccessControl.AccessControlType]::Allow
    )
    $acl.SetAccessRule($rule)
    Set-Acl -LiteralPath $Path -AclObject $acl -WhatIf:$false
}

function Stop-Setup {
    param([string]$Code)
    $exception = [InvalidOperationException]::new("Tailnet setup failed: $Code.")
    $exception.Data['HerdrSetupCode'] = $Code
    throw $exception
}

function Assert-PrivateDirectory {
    param([string]$Path)
    if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
        if (([int][IO.File]::GetUnixFileMode($Path) -band 63) -ne 0) { Stop-Setup 'output_unavailable' }
        return
    }
    $acl = Get-Acl -LiteralPath $Path
    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    if (-not $acl.AreAccessRulesProtected) { Stop-Setup 'output_unavailable' }
    foreach ($rule in $acl.GetAccessRules($true, $true, [Security.Principal.SecurityIdentifier])) {
        if ($rule.AccessControlType -eq 'Allow' -and $rule.IdentityReference.Value -notin @($sid, 'S-1-5-18')) {
            Stop-Setup 'output_unavailable'
        }
    }
}

function Assert-SafeOutputPath {
    param([string]$Path)
    $current = [IO.Path]::GetFullPath($Path)
    while ($current) {
        if (Test-Path -LiteralPath $current) {
            $item = Get-Item -LiteralPath $current -Force
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                Stop-Setup 'output_unavailable'
            }
        }
        $current = [IO.Path]::GetDirectoryName($current)
    }
}

function ConvertTo-Hashtable {
    param(
        $InputObject
    )

    if ($null -eq $InputObject) {
        return $null
    }
    if ($InputObject -is [Collections.IDictionary]) {
        $result = @{}
        foreach ($key in $InputObject.Keys) {
            $result[$key] = ConvertTo-Hashtable -InputObject $InputObject[$key]
        }
        return $result
    }
    if ($InputObject -is [Management.Automation.PSCustomObject]) {
        $result = @{}
        foreach ($property in $InputObject.PSObject.Properties) {
            $result[$property.Name] = ConvertTo-Hashtable -InputObject $property.Value
        }
        return $result
    }
    if (
        $InputObject -is [Collections.IEnumerable] -and
        $InputObject -isnot [string]
    ) {
        $result = @()
        foreach ($item in $InputObject) {
            $result += ,(ConvertTo-Hashtable -InputObject $item)
        }
        return ,$result
    }
    return $InputObject
}

function Write-Utf8NoBomFile {
    param(
        [Parameter(Mandatory)]
        [string]$Path,
        [Parameter(Mandatory)]
        [AllowEmptyString()]
        [string]$Content,
        [switch]$NoClobber
    )

    $encoding = New-Object Text.UTF8Encoding($false)
    $mode = if ($NoClobber) { [IO.FileMode]::CreateNew } else { [IO.FileMode]::Create }
    $stream = [IO.File]::Open(
        $Path,
        $mode,
        [IO.FileAccess]::Write,
        [IO.FileShare]::None
    )
    try {
        $writer = New-Object IO.StreamWriter($stream, $encoding)
        try {
            $writer.Write($Content)
            $writer.Flush()
            $stream.Flush($true)
        } finally {
            $writer.Dispose()
        }
    } finally {
        $stream.Dispose()
    }
}

function Write-ProtectedSecretFile {
    param(
        [Parameter(Mandatory)]
        [string]$Path,
        [Parameter(Mandatory)]
        [string]$Content
    )

    $temporaryPath = "$Path.$([Guid]::NewGuid().ToString('N')).tmp"
    try {
        Write-Utf8NoBomFile -Path $temporaryPath -Content '' -NoClobber
        Protect-SecretFile -Path $temporaryPath
        Write-Utf8NoBomFile -Path $temporaryPath -Content $Content
        [IO.File]::Move($temporaryPath, $Path)
    } finally {
        if (Test-Path -LiteralPath $temporaryPath) {
            Remove-Item -LiteralPath $temporaryPath -Force -WhatIf:$false
        }
    }
}

function Test-HasWildcardAllow {
    param(
        [Parameter(Mandatory)]
        [hashtable]$Policy
    )

    if ($Policy.ContainsKey('acls')) {
        foreach ($acl in @($Policy.acls)) {
            if (
                $acl.action -eq 'accept' -and
                '*' -in @($acl.src) -and
                (@($acl.dst) | Where-Object { $_ -eq '*' -or $_ -eq '*:*' })
            ) {
                return $true
            }
        }
    }

    if ($Policy.ContainsKey('grants')) {
        foreach ($grant in @($Policy.grants)) {
            if (
                '*' -in @($grant.src) -and
                (@($grant.dst) | Where-Object { $_ -eq '*' -or $_ -eq '*:*' }) -and
                (
                    -not $grant.ContainsKey('ip') -or
                    '*' -in @($grant.ip) -or
                    '*:*' -in @($grant.ip)
                )
            ) {
                return $true
            }
        }
    }

    return $false
}

if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT -and
    ($PSVersionTable.PSVersion.Major -lt 7 -or
     ($PSVersionTable.PSVersion.Major -eq 7 -and $PSVersionTable.PSVersion.Minor -lt 3))) {
    Stop-Setup 'prerequisite_version'
}
$token = [Environment]::GetEnvironmentVariable($ApiTokenEnvironmentVariable)
if ([string]::IsNullOrWhiteSpace($token)) {
    throw "Environment variable $ApiTokenEnvironmentVariable is not set."
}
$token = $token.Trim()
if (-not $token.StartsWith('tskey-api-', [StringComparison]::Ordinal)) {
    throw (
        "Environment variable $ApiTokenEnvironmentVariable must contain a " +
        "Tailscale API access token beginning with 'tskey-api-'. Enrollment " +
        "auth keys beginning with 'tskey-auth-' cannot call the Tailscale API."
    )
}

$encodedTailnet = [Uri]::EscapeDataString($Tailnet)
$baseUri = "https://api.tailscale.com/api/v2/tailnet/$encodedTailnet"
$headers = @{
    Authorization = "Bearer $token"
    Accept = 'application/json'
}

# Preserve Windows short-name spelling in the structured report, matching the
# caller's absolute path rather than expanding it as .NET GetFullPath does.
$provider = $null
$drive = $null
$OutputDirectory = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath(
    $OutputDirectory, [ref]$provider, [ref]$drive)
if ($provider.Name -ne 'FileSystem') { Stop-Setup 'output_unavailable' }
Assert-SafeOutputPath $OutputDirectory
if (-not (Test-Path -LiteralPath $OutputDirectory)) {
    New-Item -ItemType Directory -Path $OutputDirectory -WhatIf:$false | Out-Null
    Protect-SecretFile $OutputDirectory
} else {
    Assert-PrivateDirectory $OutputDirectory
}
if (-not [IO.Directory]::Exists($OutputDirectory)) { Stop-Setup 'output_unavailable' }
$lockPath = Join-Path $OutputDirectory '.configure-tailnet.lock'
$lockStream = $null
try {
    $lockStream = [IO.File]::Open($lockPath, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
} catch { Stop-Setup 'output_unavailable' }
try {
if ($Apply) {
    foreach ($role in @('server', 'node', 'client')) {
        for ($number = 1; $number -le $KeysPerRole; $number++) {
            if (Test-Path -LiteralPath (Join-Path $OutputDirectory "$role-key-$number.ps1")) {
                throw 'Refusing to create auth keys because numbered key files already exist. Choose a new OutputDirectory.'
            }
        }
    }
}
$proposedPath = Join-Path $OutputDirectory 'policy-proposed.json'
if (Test-Path -LiteralPath $proposedPath) { Stop-Setup 'output_exists' }

Write-Host "Reading the current policy for tailnet $Tailnet..."
try {
    $response = Invoke-WebRequest `
        -Method Get `
        -Uri "$baseUri/acl" `
        -Headers $headers `
        -UseBasicParsing
} catch {
    $statusCode = [int]$_.Exception.Response.StatusCode
    if ($statusCode -eq 401) {
        Stop-Setup 'token_rejected'
    }
    Stop-Setup 'api_read_failed'
}
$etag = @($response.Headers['ETag'])[0]
if ([string]::IsNullOrWhiteSpace($etag)) {
    Stop-Setup 'etag_missing'
}
$etag = [string]$etag

$policyContent = $response.Content
if ($policyContent -is [byte[]]) {
    $policyContent = [Text.Encoding]::UTF8.GetString($policyContent)
}
if ($policyContent -isnot [string] -or $policyContent.Length -gt 1048576) { Stop-Setup 'policy_invalid' }

$timestamp = (Get-Date -Format 'yyyyMMdd-HHmmssfff') + '-' + [Guid]::NewGuid().ToString('N')
$backupPath = Join-Path $OutputDirectory "policy-before-$timestamp.json"
Write-ProtectedSecretFile -Path $backupPath -Content $policyContent

try { $policyObject = $policyContent | ConvertFrom-Json } catch { Stop-Setup 'policy_invalid' }
$policy = ConvertTo-Hashtable -InputObject $policyObject
$warningCodes = @('policy_round_trip')
Write-Warning (
    'POLICY ROUND-TRIP: applying the proposed policy serializes it as JSON and ' +
    'removes HuJSON comments and original key formatting. The untouched response ' +
    "is backed up at '$backupPath'. This behavior is intended only for the " +
    'scratch/hackathon tailnet workflow.'
)
if (Test-HasWildcardAllow -Policy $policy) {
    $warningCodes += 'wildcard_allow_preserved'
    Write-Warning (
        'NETWORK ISOLATION IS NOT PROVIDED: the existing policy contains a ' +
        'wildcard allow rule. The role grants added by this script do not restrict ' +
        'traffic while that rule remains. The script preserves the wildcard rule; ' +
        'review and narrow it manually only when appropriate for this tailnet.'
    )
}
if (-not $policy.ContainsKey('tagOwners')) {
    $policy.tagOwners = @{}
}

$roles = [ordered]@{
    server = 'tag:herdr-mesh-server'
    node = 'tag:herdr-mesh-node'
    client = 'tag:herdr-mesh-client'
}

foreach ($tag in $roles.Values) {
    if (-not $policy.tagOwners.ContainsKey($tag)) {
        $policy.tagOwners[$tag] = @($TagOwner)
        continue
    }
    $owners = @($policy.tagOwners[$tag])
    if ($TagOwner -notin $owners) {
        $policy.tagOwners[$tag] = @($owners + $TagOwner)
    }
}

if (-not $policy.ContainsKey('grants')) {
    $policy.grants = @()
}

function Add-GrantIfMissing {
    param(
        [Parameter(Mandatory)]
        [hashtable]$Policy,
        [Parameter(Mandatory)]
        [string]$Source,
        [Parameter(Mandatory)]
        [string]$Destination,
        [ValidateRange(1, 65535)]
        [int]$Port = 50052
    )

    foreach ($grant in @($Policy.grants)) {
        if (
            @($grant.src).Count -eq 1 -and $grant.src[0] -eq $Source -and
            @($grant.dst).Count -eq 1 -and $grant.dst[0] -eq $Destination -and
            @($grant.ip).Count -eq 1 -and $grant.ip[0] -eq "tcp:$Port"
        ) {
            return
        }
    }

    $Policy.grants = @($Policy.grants) + @{
        src = @($Source)
        dst = @($Destination)
        ip = @("tcp:$Port")
    }
}

Add-GrantIfMissing -Policy $policy -Source $roles.node -Destination $roles.server
Add-GrantIfMissing -Policy $policy -Source $roles.client -Destination $roles.server
if ($PSBoundParameters.ContainsKey('DashboardPort')) {
    Add-GrantIfMissing -Policy $policy -Source $roles.client -Destination $roles.server -Port $DashboardPort
}

$proposedPolicy = $policy | ConvertTo-Json -Depth 100
Write-ProtectedSecretFile -Path $proposedPath -Content $proposedPolicy

$changesApplied = $Apply -and $PSCmdlet.ShouldProcess(
    $Tailnet,
    "Update Tailscale policy and create $($roles.Count * $KeysPerRole) scoped auth keys"
)
if ($changesApplied) {
    $createdKeys = @()
    $createdAliases = @()
    try {
    Write-Host 'Updating tailnet policy with ETag protection...'
    $policyHeaders = @{
        Authorization = "Bearer $token"
        Accept = 'application/json'
        'If-Match' = $etag
    }
    try { Invoke-RestMethod `
        -Method Post `
        -Uri "$baseUri/acl" `
        -Headers $policyHeaders `
        -ContentType 'application/json' `
        -Body $proposedPolicy | Out-Null
    } catch { Stop-Setup 'api_update_failed' }

    foreach ($role in $roles.Keys) {
        $tag = $roles[$role]
        for ($keyNumber = 1; $keyNumber -le $KeysPerRole; $keyNumber++) {
            $body = @{
                keyType = 'auth'
                description = "herdr mesh $role hackathon $keyNumber"
                expirySeconds = $KeyExpirySeconds
                capabilities = @{
                    devices = @{
                        create = @{
                            reusable = $false
                            ephemeral = $false
                            preauthorized = $true
                            tags = @($tag)
                        }
                    }
                }
            } | ConvertTo-Json -Depth 10

            Write-Host "Creating one-off $role auth key $keyNumber of $KeysPerRole..."
            try { $keyResponse = Invoke-RestMethod `
                -Method Post `
                -Uri "$baseUri/keys" `
                -Headers $headers `
                -ContentType 'application/json' `
                -Body $body
            } catch { Stop-Setup 'key_creation_failed' }
            if ([string]$keyResponse.id -notmatch '^[A-Za-z0-9_-]{1,256}$') {
                Stop-Setup 'key_response_invalid'
            }

            $environmentVariable = "TS_AUTHKEY_$($role.ToUpperInvariant())"
            $secretPath = Join-Path $OutputDirectory "$role-key-$keyNumber.ps1"
            $createdKeys += [pscustomobject]@{
                Id = [string]$keyResponse.id
                Path = $secretPath
            }
            if ([string]$keyResponse.key -cnotmatch '^tskey-auth-[A-Za-z0-9_-]+$') {
                Stop-Setup 'key_response_invalid'
            }
            Write-ProtectedSecretFile `
                -Path $secretPath `
                -Content "`$env:$environmentVariable = '$($keyResponse.key)'"
        }

        $primaryPath = Join-Path $OutputDirectory "$role-key.ps1"
        if (Test-Path -LiteralPath $primaryPath) {
            $warningCodes += 'primary_alias_preserved'
            Write-Warning (
                "Preserving existing primary key file '$primaryPath'. Use the " +
                'new numbered files for this run.'
            )
        } else {
            Write-ProtectedSecretFile `
                -Path $primaryPath `
                -Content ". `"`$PSScriptRoot\$role-key-1.ps1`""
            $createdAliases += $primaryPath
        }
    }
    } catch {
        $originalError = $_
        $revocationFailed = $false
        $cleanupFailed = $false
        foreach ($createdKey in $createdKeys) {
            try {
                $encodedKeyId = [Uri]::EscapeDataString($createdKey.Id)
                Invoke-RestMethod `
                    -Method Delete `
                    -Uri "$baseUri/keys/$encodedKeyId" `
                    -Headers $headers | Out-Null
            } catch {
                $revocationFailed = $true
                Write-Warning 'KEY REVOCATION UNCONFIRMED: inspect newly created keys in the Tailscale admin console.'
            }
            if (Test-Path -LiteralPath $createdKey.Path) {
                try { Remove-Item -LiteralPath $createdKey.Path -Force -WhatIf:$false } catch { $cleanupFailed = $true }
            }
        }
        foreach ($alias in $createdAliases) {
            try { Remove-Item -LiteralPath $alias -Force -WhatIf:$false } catch { $cleanupFailed = $true }
        }
        if ($revocationFailed) { Stop-Setup 'key_revocation_unconfirmed' }
        if ($cleanupFailed) { Stop-Setup 'key_cleanup_failed' }
        throw $originalError
    }
}

Write-Host "Existing policy backup: $backupPath"
Write-Host "Proposed merged policy: $proposedPath"
if ($changesApplied) {
    $warningCodes += 'auth_key_secrets'
    Write-Host (
        "Numbered role key files: $OutputDirectory\server-key-1.ps1 through " +
        "server-key-$KeysPerRole.ps1, with equivalent node and client files."
    )
    Write-Host (
        'Primary server-key.ps1, node-key.ps1, and client-key.ps1 aliases were ' +
        'created only where they did not already exist.'
    )
    Write-Warning (
        'Auth key files are secrets. After enrollment, clear the TS_AUTHKEY_* ' +
        'environment variables and securely delete consumed key files. Revoke any ' +
        'unused keys before their configured expiry.'
    )
} else {
    Write-Host 'Preview complete; the tailnet was not changed and no auth keys were created.'
}
if ($Structured) {
    [pscustomobject]@{
        mode = $(if ($changesApplied) { 'applied' } else { 'preview' })
        policy_backup = $backupPath
        policy_proposal = $proposedPath
        keys_created = $(if ($changesApplied) { 3 * $KeysPerRole } else { 0 })
        warnings = @($warningCodes | Select-Object -Unique)
    }
}
} finally {
    $lockStream.Dispose()
    Remove-Item -LiteralPath $lockPath -Force -WhatIf:$false
}
