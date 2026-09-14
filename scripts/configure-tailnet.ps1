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

    [string]$OutputDirectory = (Join-Path $env:LOCALAPPDATA 'herdr-mesh\tailnet-setup')
)

$ErrorActionPreference = 'Stop'

function Protect-SecretFile {
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    $identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
    $acl = Get-Acl -LiteralPath $Path
    $acl.SetAccessRuleProtection($true, $false)
    $rule = [Security.AccessControl.FileSystemAccessRule]::new(
        $identity,
        [Security.AccessControl.FileSystemRights]::FullControl,
        [Security.AccessControl.AccessControlType]::Allow
    )
    $acl.SetAccessRule($rule)
    Set-Acl -LiteralPath $Path -AclObject $acl
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
    if (-not $NoClobber) {
        [IO.File]::WriteAllText($Path, $Content, $encoding)
        return
    }

    $stream = [IO.File]::Open(
        $Path,
        [IO.FileMode]::CreateNew,
        [IO.FileAccess]::Write,
        [IO.FileShare]::None
    )
    try {
        $writer = New-Object IO.StreamWriter($stream, $encoding)
        try {
            $writer.Write($Content)
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
            Remove-Item -LiteralPath $temporaryPath -Force
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

New-Item -ItemType Directory -Path $OutputDirectory -Force -WhatIf:$false | Out-Null

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
        throw (
            'Tailscale rejected the API access token. Generate a current API ' +
            'access token from the Tailscale admin console Keys page; do not ' +
            'use a device enrollment auth key.'
        )
    }
    throw
}
$etag = @($response.Headers['ETag'])[0]
if ([string]::IsNullOrWhiteSpace($etag)) {
    throw 'The Tailscale policy response did not include an ETag.'
}
$etag = [string]$etag

$policyContent = $response.Content
if ($policyContent -is [byte[]]) {
    $policyContent = [Text.Encoding]::UTF8.GetString($policyContent)
}

$timestamp = Get-Date -Format 'yyyyMMdd-HHmmssfff'
$backupPath = Join-Path $OutputDirectory "policy-before-$timestamp.json"
Write-Utf8NoBomFile -Path $backupPath -Content $policyContent

$policyObject = $policyContent | ConvertFrom-Json
$policy = ConvertTo-Hashtable -InputObject $policyObject
Write-Warning (
    'POLICY ROUND-TRIP: applying the proposed policy serializes it as JSON and ' +
    'removes HuJSON comments and original key formatting. The untouched response ' +
    "is backed up at '$backupPath'. This behavior is intended only for the " +
    'scratch/hackathon tailnet workflow.'
)
if (Test-HasWildcardAllow -Policy $policy) {
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
        [string]$Destination
    )

    foreach ($grant in @($Policy.grants)) {
        if (
            @($grant.src).Count -eq 1 -and $grant.src[0] -eq $Source -and
            @($grant.dst).Count -eq 1 -and $grant.dst[0] -eq $Destination -and
            @($grant.ip).Count -eq 1 -and $grant.ip[0] -eq 'tcp:50052'
        ) {
            return
        }
    }

    $Policy.grants = @($Policy.grants) + @{
        src = @($Source)
        dst = @($Destination)
        ip = @('tcp:50052')
    }
}

Add-GrantIfMissing -Policy $policy -Source $roles.node -Destination $roles.server
Add-GrantIfMissing -Policy $policy -Source $roles.client -Destination $roles.server

$proposedPolicy = $policy | ConvertTo-Json -Depth 100
$proposedPath = Join-Path $OutputDirectory 'policy-proposed.json'
Write-Utf8NoBomFile -Path $proposedPath -Content $proposedPolicy

$changesApplied = $PSCmdlet.ShouldProcess(
    $Tailnet,
    "Update Tailscale policy and create $($roles.Count * $KeysPerRole) scoped auth keys"
)
if ($changesApplied) {
    $lockPath = Join-Path $OutputDirectory '.configure-tailnet.lock'
    try {
        $lockStream = [IO.File]::Open(
            $lockPath,
            [IO.FileMode]::CreateNew,
            [IO.FileAccess]::Write,
            [IO.FileShare]::None
        )
    } catch {
        throw "Another tailnet configuration run is using '$OutputDirectory'."
    }
    try {
    $plannedSecretPaths = @()
    $createdKeys = @()
    try {
    foreach ($role in $roles.Keys) {
        for ($keyNumber = 1; $keyNumber -le $KeysPerRole; $keyNumber++) {
            $plannedSecretPaths += Join-Path $OutputDirectory "$role-key-$keyNumber.ps1"
        }
    }
    $existingSecretPaths = @($plannedSecretPaths | Where-Object {
        Test-Path -LiteralPath $_
    })
    if ($existingSecretPaths.Count -gt 0) {
        throw (
            'Refusing to create auth keys because these numbered key files already ' +
            "exist and may contain unconsumed secrets:`n" +
            ($existingSecretPaths -join "`n") +
            "`nMove or securely delete them, or choose a new OutputDirectory."
        )
    }

    Write-Host 'Updating tailnet policy with ETag protection...'
    $policyHeaders = @{
        Authorization = "Bearer $token"
        Accept = 'application/json'
        'If-Match' = $etag
    }
    Invoke-RestMethod `
        -Method Post `
        -Uri "$baseUri/acl" `
        -Headers $policyHeaders `
        -ContentType 'application/json' `
        -Body $proposedPolicy | Out-Null

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
            $keyResponse = Invoke-RestMethod `
                -Method Post `
                -Uri "$baseUri/keys" `
                -Headers $headers `
                -ContentType 'application/json' `
                -Body $body
            if ([string]::IsNullOrWhiteSpace($keyResponse.id)) {
                throw "Tailscale did not return an ID for the new $role auth key."
            }

            $environmentVariable = "TS_AUTHKEY_$($role.ToUpperInvariant())"
            $secretPath = Join-Path $OutputDirectory "$role-key-$keyNumber.ps1"
            $createdKeys += [pscustomobject]@{
                Id = [string]$keyResponse.id
                Path = $secretPath
            }
            Write-ProtectedSecretFile `
                -Path $secretPath `
                -Content "`$env:$environmentVariable = '$($keyResponse.key)'"
        }

        $primaryPath = Join-Path $OutputDirectory "$role-key.ps1"
        if (Test-Path -LiteralPath $primaryPath) {
            Write-Warning (
                "Preserving existing primary key file '$primaryPath'. Use the " +
                'new numbered files for this run.'
            )
        } else {
            Write-ProtectedSecretFile `
                -Path $primaryPath `
                -Content ". `"`$PSScriptRoot\$role-key-1.ps1`""
        }
    }
    } catch {
        $originalError = $_
        foreach ($createdKey in $createdKeys) {
            try {
                $encodedKeyId = [Uri]::EscapeDataString($createdKey.Id)
                Invoke-RestMethod `
                    -Method Delete `
                    -Uri "$baseUri/keys/$encodedKeyId" `
                    -Headers $headers | Out-Null
            } catch {
                Write-Warning "Failed to revoke auth key ID $($createdKey.Id): $_"
            }
            if (Test-Path -LiteralPath $createdKey.Path) {
                Remove-Item -LiteralPath $createdKey.Path -Force
            }
        }
        throw $originalError
    }
    } finally {
        if ($null -ne $lockStream) {
            $lockStream.Dispose()
        }
        Remove-Item -LiteralPath $lockPath -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "Existing policy backup: $backupPath"
Write-Host "Proposed merged policy: $proposedPath"
if ($changesApplied) {
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
