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

$scriptPath = Join-Path $PSScriptRoot 'configure-tailnet.ps1'
$testRoot = Join-Path ([IO.Path]::GetTempPath()) "herdr-tailnet-test-$([Guid]::NewGuid())"
$firstOutput = Join-Path $testRoot 'first'
$secondOutput = Join-Path $testRoot 'second'
$tokenVariable = 'HERDR_TAILNET_TEST_TOKEN'
$global:HerdrTailnetMockPolicy = @'
{
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

function global:Invoke-WebRequest {
    param(
        [string]$Method,
        [string]$Uri,
        [hashtable]$Headers
    )

    Assert-Equal -Expected 'Get' -Actual $Method -Message 'Policy request method mismatch.'
    Assert-True -Condition $Uri.EndsWith('/api/v2/tailnet/example.com/acl') `
        -Message "Unexpected policy URI: $Uri"
    Assert-True -Condition $Headers.Authorization.EndsWith('test-token') `
        -Message 'Policy request did not use the configured API token.'

    return [pscustomobject]@{
        Headers = @{ ETag = '"test-etag"' }
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

    if ($Uri.EndsWith('/keys')) {
        $request = $Body | ConvertFrom-Json -AsHashtable
        $tag = $request.capabilities.devices.create.tags[0]
        $role = $tag.Replace('tag:herdr-mesh-', '')
        return @{ key = "tskey-auth-$role-test" }
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

    & $scriptPath `
        -Tailnet 'example.com' `
        -ApiTokenEnvironmentVariable $tokenVariable `
        -OutputDirectory $firstOutput `
        -Confirm:$false

    Assert-Equal -Expected 4 -Actual $global:HerdrTailnetMockRestCalls.Count `
        -Message 'Unexpected number of policy/key API calls.'

    $policyCall = $global:HerdrTailnetMockRestCalls[0]
    Assert-True -Condition $policyCall.Uri.EndsWith('/acl') `
        -Message 'The first REST call was not the policy update.'
    Assert-Equal -Expected '"test-etag"' -Actual $policyCall.Headers['If-Match'] `
        -Message 'Policy update did not preserve the fetched ETag.'

    $policy = $policyCall.Body | ConvertFrom-Json -AsHashtable
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
        $keyCall = $global:HerdrTailnetMockRestCalls |
            Where-Object { $_.Uri.EndsWith('/keys') } |
            Where-Object {
                ($_.Body | ConvertFrom-Json -AsHashtable).description -eq
                    "herdr mesh $role hackathon"
            }
        Assert-Equal -Expected 1 -Actual @($keyCall).Count `
            -Message "Expected one key request for $role."

        $keyRequest = $keyCall.Body | ConvertFrom-Json -AsHashtable
        $create = $keyRequest.capabilities.devices.create
        Assert-Equal -Expected $false -Actual $create.reusable `
            -Message "$role key must not be reusable."
        Assert-Equal -Expected $false -Actual $create.ephemeral `
            -Message "$role key must preserve its tsnet identity."
        Assert-Equal -Expected $true -Actual $create.preauthorized `
            -Message "$role key must be preauthorized."
        Assert-Equal -Expected "tag:herdr-mesh-$role" -Actual $create.tags[0] `
            -Message "$role key requested the wrong tag."

        $secretPath = Join-Path $firstOutput "$role-key.ps1"
        Assert-True -Condition (Test-Path -LiteralPath $secretPath) `
            -Message "Missing secret file for $role."
        $secret = Get-Content -LiteralPath $secretPath -Raw
        Assert-True -Condition $secret.Contains("tskey-auth-$role-test") `
            -Message "$role secret file contains the wrong key."
        $acl = Get-Acl -LiteralPath $secretPath
        Assert-True -Condition $acl.AreAccessRulesProtected `
            -Message "$role secret file still inherits access rules."
    }

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
        Assert-True -Condition (-not (Test-Path -LiteralPath (
            Join-Path $secondOutput "$role-key.ps1"
        ))) -Message "WhatIf unexpectedly wrote the $role key."
    }

    $secondPolicy = Get-Content `
        -LiteralPath (Join-Path $secondOutput 'policy-proposed.json') `
        -Raw |
        ConvertFrom-Json -AsHashtable
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
    Remove-Item Variable:\global:HerdrTailnetMockPolicy -ErrorAction SilentlyContinue
    Remove-Item Variable:\global:HerdrTailnetMockRestCalls -ErrorAction SilentlyContinue
    if (Test-Path -LiteralPath $testRoot) {
        Remove-Item -LiteralPath $testRoot -Recurse -Force
    }
}
