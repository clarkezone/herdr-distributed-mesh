[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
param(
    [Parameter(Mandatory)]
    [string]$Tailnet,

    [string]$ApiTokenEnvironmentVariable = 'TAILSCALE_API_TOKEN',

    [string]$TagOwner = 'autogroup:admin',

    [ValidateRange(3600, 7776000)]
    [int]$KeyExpirySeconds = 604800,

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
    $response = Invoke-WebRequest -Method Get -Uri "$baseUri/acl" -Headers $headers
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
$etag = $response.Headers['ETag']
if ([string]::IsNullOrWhiteSpace($etag)) {
    throw 'The Tailscale policy response did not include an ETag.'
}

$policyContent = $response.Content
if ($policyContent -is [byte[]]) {
    $policyContent = [Text.Encoding]::UTF8.GetString($policyContent)
}

$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$backupPath = Join-Path $OutputDirectory "policy-before-$timestamp.json"
$policyContent |
    Set-Content -LiteralPath $backupPath -Encoding utf8NoBOM -WhatIf:$false

$policy = $policyContent | ConvertFrom-Json -AsHashtable
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
$proposedPolicy |
    Set-Content -LiteralPath $proposedPath -Encoding utf8NoBOM -WhatIf:$false

$changesApplied = $PSCmdlet.ShouldProcess(
    $Tailnet,
    'Update Tailscale policy and create three scoped auth keys'
)
if ($changesApplied) {
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
        $body = @{
            keyType = 'auth'
            description = "herdr mesh $role hackathon"
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

        Write-Host "Creating one-off $role auth key..."
        $keyResponse = Invoke-RestMethod `
            -Method Post `
            -Uri "$baseUri/keys" `
            -Headers $headers `
            -ContentType 'application/json' `
            -Body $body

        $environmentVariable = "TS_AUTHKEY_$($role.ToUpperInvariant())"
        if ($role -eq 'client') {
            $environmentVariable = 'TS_AUTHKEY_CLIENT'
        }
        $secretPath = Join-Path $OutputDirectory "$role-key.ps1"
        "`$env:$environmentVariable = '$($keyResponse.key)'" |
            Set-Content -LiteralPath $secretPath -Encoding utf8NoBOM
        Protect-SecretFile -Path $secretPath
    }
}

Write-Host "Existing policy backup: $backupPath"
Write-Host "Proposed merged policy: $proposedPath"
if ($changesApplied) {
    Write-Host "Role key files: $OutputDirectory\server-key.ps1, node-key.ps1, client-key.ps1"
} else {
    Write-Host 'Preview complete; the tailnet was not changed and no auth keys were created.'
}
