[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

function Assert-True {
    param(
        [Parameter(Mandatory)]
        [bool]$Condition,
        [Parameter(Mandatory)]
        [string]$Message
    )

    if (-not $Condition) {
        throw $Message
    }
}

function Assert-Equal {
    param(
        $Expected,
        $Actual,
        [Parameter(Mandatory)]
        [string]$Message
    )

    if ($Expected -ne $Actual) {
        throw "$Message Expected '$Expected', got '$Actual'."
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

function ConvertFrom-TestJson {
    param(
        [Parameter(Mandatory)]
        [string]$Json
    )

    return ConvertTo-Hashtable -InputObject ($Json | ConvertFrom-Json)
}

$scriptPath = Join-Path $PSScriptRoot 'configure-tailnet.ps1'
$testRoot = Join-Path ([IO.Path]::GetTempPath()) "herdr-tailnet-test-$([Guid]::NewGuid())"
$firstOutput = Join-Path $testRoot 'first'
$secondOutput = Join-Path $testRoot 'second'
$failureOutput = Join-Path $testRoot 'failure'
$tokenVariable = 'HERDR_TAILNET_TEST_TOKEN'
$global:HerdrTailnetMockPolicy = @'
{
  "acls": [
    {
      "action": "accept",
      "src": ["*"],
      "dst": ["*:*"]
    }
  ],
  "tagOwners": {
    "tag:herdr-mesh-server": ["group:existing"]
  },
  "grants": [
    {
      "src": ["tag:herdr-mesh-node"],
      "dst": ["tag:herdr-mesh-server"],
      "ip": ["tcp:50052"]
    },
    {
      "src": ["group:existing"],
      "dst": ["tag:unrelated"],
      "ip": ["tcp:443"]
    }
  ]
}
'@
$global:HerdrTailnetMockRestCalls = [Collections.Generic.List[object]]::new()
$global:HerdrFailSecretAcl = $false
$global:HerdrFailureOutput = $failureOutput

function global:Set-Acl {
    param(
        [string]$LiteralPath,
        $AclObject
    )

    if ($global:HerdrFailSecretAcl -and $LiteralPath.StartsWith($global:HerdrFailureOutput)) {
        throw 'simulated ACL failure'
    }
    Microsoft.PowerShell.Security\Set-Acl `
        -LiteralPath $LiteralPath `
        -AclObject $AclObject
}

function global:Invoke-WebRequest {
    param(
        [string]$Method,
        [string]$Uri,
        [hashtable]$Headers,
        [switch]$UseBasicParsing
    )

    Assert-Equal -Expected 'Get' -Actual $Method -Message 'Policy request method mismatch.'
    Assert-True -Condition $Uri.EndsWith('/api/v2/tailnet/example.com/acl') `
        -Message "Unexpected policy URI: $Uri"
    Assert-True -Condition $Headers.Authorization.EndsWith('test-token') `
        -Message 'Policy request did not use the configured API token.'

    return [pscustomobject]@{
        Headers = @{ ETag = @('"test-etag"') }
        Content = $global:HerdrTailnetMockPolicy
    }
}

function global:Invoke-RestMethod {
    param(
        [string]$Method,
        [string]$Uri,
        [hashtable]$Headers,
        [string]$ContentType,
        [string]$Body
    )

    $global:HerdrTailnetMockRestCalls.Add([pscustomobject]@{
        Method = $Method
        Uri = $Uri
        Headers = $Headers
        ContentType = $ContentType
        Body = $Body
    })

    if ($Method -eq 'Delete') {
        return
    }
    if ($Uri.EndsWith('/keys')) {
        $request = ConvertFrom-TestJson -Json $Body
        $tag = $request.capabilities.devices.create.tags[0]
        $role = $tag.Replace('tag:herdr-mesh-', '')
        $keyNumber = $request.description.Split(' ')[-1]
        return @{
            id = "key-id-$role-$keyNumber"
            key = "tskey-auth-$role-$keyNumber-test"
        }
    }
}

try {
    [Environment]::SetEnvironmentVariable($tokenVariable, 'tskey-auth-wrong-kind')
    try {
        & $scriptPath `
            -Tailnet 'example.com' `
            -ApiTokenEnvironmentVariable $tokenVariable `
            -OutputDirectory $firstOutput `
            -Confirm:$false
        throw 'Expected the enrollment auth key to be rejected.'
    } catch {
        Assert-True `
            -Condition $_.Exception.Message.Contains("beginning with 'tskey-api-'") `
            -Message 'Enrollment auth key failure did not explain the required token type.'
    }

    [Environment]::SetEnvironmentVariable($tokenVariable, ' tskey-api-test-token ')
    $invalidKeysPerRoleRejected = $false
    try {
        & $scriptPath `
            -Tailnet 'example.com' `
            -ApiTokenEnvironmentVariable $tokenVariable `
            -OutputDirectory $firstOutput `
            -KeysPerRole 0 `
            -Confirm:$false
    } catch {
        $invalidKeysPerRoleRejected = $true
    }
    Assert-True -Condition $invalidKeysPerRoleRejected `
        -Message 'KeysPerRole accepted a value below its validated range.'

    $warnings = @()
    & $scriptPath `
        -Tailnet 'example.com' `
        -ApiTokenEnvironmentVariable $tokenVariable `
        -OutputDirectory $firstOutput `
        -WarningVariable warnings `
        -Confirm:$false

    Assert-Equal -Expected 7 -Actual $global:HerdrTailnetMockRestCalls.Count `
        -Message 'Unexpected number of policy/key API calls.'
    Assert-True -Condition (
        ($warnings -join "`n").Contains('NETWORK ISOLATION IS NOT PROVIDED')
    ) -Message 'Wildcard allow policy did not produce the isolation warning.'
    Assert-True -Condition (
        ($warnings -join "`n").Contains('POLICY ROUND-TRIP')
    ) -Message 'Policy serialization did not produce the HuJSON warning.'

    $policyCall = $global:HerdrTailnetMockRestCalls[0]
    Assert-True -Condition $policyCall.Uri.EndsWith('/acl') `
        -Message 'The first REST call was not the policy update.'
    Assert-Equal -Expected '"test-etag"' -Actual $policyCall.Headers['If-Match'] `
        -Message 'Policy update did not preserve the fetched ETag.'

    $policy = ConvertFrom-TestJson -Json $policyCall.Body
    foreach ($tag in @(
        'tag:herdr-mesh-server',
        'tag:herdr-mesh-node',
        'tag:herdr-mesh-client'
    )) {
        Assert-True -Condition $policy.tagOwners.ContainsKey($tag) `
            -Message "Missing tag owner entry for $tag."
        Assert-True -Condition ('autogroup:admin' -in @($policy.tagOwners[$tag])) `
            -Message "Missing configured owner for $tag."
    }
    Assert-True -Condition ('group:existing' -in @($policy.tagOwners['tag:herdr-mesh-server'])) `
        -Message 'Existing tag owners were not preserved.'
    $wildcardAcls = @($policy.acls | Where-Object {
        $_.action -eq 'accept' -and
        '*' -in @($_.src) -and
        '*:*' -in @($_.dst)
    })
    Assert-Equal -Expected 1 -Actual $wildcardAcls.Count `
        -Message 'The existing wildcard allow ACL was changed or removed.'

    $nodeGrants = @($policy.grants | Where-Object {
        @($_.src).Count -eq 1 -and $_.src[0] -eq 'tag:herdr-mesh-node' -and
        @($_.dst).Count -eq 1 -and $_.dst[0] -eq 'tag:herdr-mesh-server' -and
        @($_.ip).Count -eq 1 -and $_.ip[0] -eq 'tcp:50052'
    })
    $clientGrants = @($policy.grants | Where-Object {
        @($_.src).Count -eq 1 -and $_.src[0] -eq 'tag:herdr-mesh-client' -and
        @($_.dst).Count -eq 1 -and $_.dst[0] -eq 'tag:herdr-mesh-server' -and
        @($_.ip).Count -eq 1 -and $_.ip[0] -eq 'tcp:50052'
    })
    Assert-Equal -Expected 1 -Actual $nodeGrants.Count `
        -Message 'Node grant was duplicated or omitted.'
    Assert-Equal -Expected 1 -Actual $clientGrants.Count `
        -Message 'Client grant was duplicated or omitted.'

    foreach ($role in @('server', 'node', 'client')) {
        $keyCalls = @($global:HerdrTailnetMockRestCalls |
            Where-Object { $_.Uri.EndsWith('/keys') } |
            Where-Object {
                (ConvertFrom-TestJson -Json $_.Body).description.StartsWith(
                    "herdr mesh $role hackathon "
                )
            })
        Assert-Equal -Expected 2 -Actual $keyCalls.Count `
            -Message "Expected two key requests for $role."

        for ($keyNumber = 1; $keyNumber -le 2; $keyNumber++) {
            $keyCall = @($keyCalls | Where-Object {
                (ConvertFrom-TestJson -Json $_.Body).description -eq
                    "herdr mesh $role hackathon $keyNumber"
            })
            Assert-Equal -Expected 1 -Actual $keyCall.Count `
                -Message "Expected key request $keyNumber for $role."

            $keyRequest = ConvertFrom-TestJson -Json $keyCall[0].Body
            $create = $keyRequest.capabilities.devices.create
            Assert-Equal -Expected $false -Actual $create.reusable `
                -Message "$role key must not be reusable."
            Assert-Equal -Expected $false -Actual $create.ephemeral `
                -Message "$role key must preserve its tsnet identity."
            Assert-Equal -Expected $true -Actual $create.preauthorized `
                -Message "$role key must be preauthorized."
            Assert-Equal -Expected "tag:herdr-mesh-$role" -Actual $create.tags[0] `
                -Message "$role key requested the wrong tag."
            Assert-Equal -Expected 604800 -Actual $keyRequest.expirySeconds `
                -Message "$role key requested the wrong expiry."

            $secretPath = Join-Path $firstOutput "$role-key-$keyNumber.ps1"
            Assert-True -Condition (Test-Path -LiteralPath $secretPath) `
                -Message "Missing numbered secret file $keyNumber for $role."
            $secret = Get-Content -LiteralPath $secretPath -Raw
            Assert-True -Condition $secret.Contains(
                "tskey-auth-$role-$keyNumber-test"
            ) -Message "$role secret file $keyNumber contains the wrong key."
            $acl = Get-Acl -LiteralPath $secretPath
            Assert-True -Condition $acl.AreAccessRulesProtected `
                -Message "$role secret file $keyNumber still inherits access rules."
        }

        $primaryPath = Join-Path $firstOutput "$role-key.ps1"
        Assert-True -Condition (Test-Path -LiteralPath $primaryPath) `
            -Message "Missing primary key alias for $role."
        $primary = Get-Content -LiteralPath $primaryPath -Raw
        Assert-True -Condition $primary.Contains("$role-key-1.ps1") `
            -Message "$role primary key alias does not load key 1."
    }

    $existingPrimary = Join-Path $firstOutput 'server-key.ps1'
    $existingPrimaryContent = Get-Content -LiteralPath $existingPrimary -Raw
    try {
        & $scriptPath `
            -Tailnet 'example.com' `
            -ApiTokenEnvironmentVariable $tokenVariable `
            -OutputDirectory $firstOutput `
            -Confirm:$false
        throw 'Expected existing numbered key files to block another run.'
    } catch {
        Assert-True -Condition $_.Exception.Message.Contains(
            'Refusing to create auth keys'
        ) -Message 'Existing numbered key files did not produce a safe failure.'
    }
    Assert-Equal -Expected $existingPrimaryContent -Actual (
        Get-Content -LiteralPath $existingPrimary -Raw
    ) -Message 'A blocked rerun overwrote the existing primary key file.'

    $global:HerdrTailnetMockRestCalls.Clear()
    $global:HerdrFailSecretAcl = $true
    try {
        & $scriptPath `
            -Tailnet 'example.com' `
            -ApiTokenEnvironmentVariable $tokenVariable `
            -OutputDirectory $failureOutput `
            -KeysPerRole 1 `
            -Confirm:$false
        throw 'Expected secure secret persistence to fail.'
    } catch {
        Assert-True -Condition $_.Exception.Message.Contains('simulated ACL failure') `
            -Message 'Secret persistence failure did not propagate.'
    } finally {
        $global:HerdrFailSecretAcl = $false
    }
    $deleteCalls = @($global:HerdrTailnetMockRestCalls | Where-Object {
        $_.Method -eq 'Delete'
    })
    Assert-Equal -Expected 1 -Actual $deleteCalls.Count `
        -Message 'Failed secret persistence did not revoke the created key.'
    Assert-True -Condition $deleteCalls[0].Uri.EndsWith('/keys/key-id-server-1') `
        -Message 'Compensating key revocation used the wrong key ID.'
    Assert-True -Condition (-not (Test-Path -LiteralPath (
        Join-Path $failureOutput 'server-key-1.ps1'
    ))) -Message 'Failed secret persistence left a key file behind.'
    Assert-True -Condition (-not (Test-Path -LiteralPath (
        Join-Path $failureOutput '.configure-tailnet.lock'
    ))) -Message 'Failed secret persistence left the run lock behind.'

    $global:HerdrTailnetMockPolicy = Get-Content `
        -LiteralPath (Join-Path $firstOutput 'policy-proposed.json') `
        -Raw
    $global:HerdrTailnetMockRestCalls.Clear()

    & $scriptPath `
        -Tailnet 'example.com' `
        -ApiTokenEnvironmentVariable $tokenVariable `
        -OutputDirectory $secondOutput `
        -WhatIf

    Assert-Equal -Expected 0 -Actual $global:HerdrTailnetMockRestCalls.Count `
        -Message 'WhatIf unexpectedly changed policy or created keys.'
    foreach ($role in @('server', 'node', 'client')) {
        foreach ($name in @("$role-key.ps1", "$role-key-1.ps1", "$role-key-2.ps1")) {
            Assert-True -Condition (-not (Test-Path -LiteralPath (
                Join-Path $secondOutput $name
            ))) -Message "WhatIf unexpectedly wrote $name."
        }
    }

    $secondPolicyJson = Get-Content `
        -LiteralPath (Join-Path $secondOutput 'policy-proposed.json') `
        -Raw
    $secondPolicy = ConvertFrom-TestJson -Json $secondPolicyJson
    foreach ($source in @('tag:herdr-mesh-node', 'tag:herdr-mesh-client')) {
        $matchingGrants = @($secondPolicy.grants | Where-Object {
            @($_.src).Count -eq 1 -and $_.src[0] -eq $source -and
            @($_.dst).Count -eq 1 -and $_.dst[0] -eq 'tag:herdr-mesh-server' -and
            @($_.ip).Count -eq 1 -and $_.ip[0] -eq 'tcp:50052'
        })
        Assert-Equal -Expected 1 -Actual $matchingGrants.Count `
            -Message "Repeated merge duplicated the grant for $source."
    }

    Write-Host 'configure-tailnet.ps1 tests passed.'
} finally {
    [Environment]::SetEnvironmentVariable($tokenVariable, $null)
    Remove-Item Function:\global:Invoke-WebRequest -ErrorAction SilentlyContinue
    Remove-Item Function:\global:Invoke-RestMethod -ErrorAction SilentlyContinue
    Remove-Item Function:\global:Set-Acl -ErrorAction SilentlyContinue
    Remove-Item Variable:\global:HerdrTailnetMockPolicy -ErrorAction SilentlyContinue
    Remove-Item Variable:\global:HerdrTailnetMockRestCalls -ErrorAction SilentlyContinue
    Remove-Item Variable:\global:HerdrFailSecretAcl -ErrorAction SilentlyContinue
    Remove-Item Variable:\global:HerdrFailureOutput -ErrorAction SilentlyContinue
    if (Test-Path -LiteralPath $testRoot) {
        Remove-Item -LiteralPath $testRoot -Recurse -Force
    }
}
