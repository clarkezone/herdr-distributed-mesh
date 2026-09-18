param([Parameter(Mandatory)][string]$OptionsPath)
$ErrorActionPreference = 'Stop'
try {
    $options = [IO.File]::ReadAllText($OptionsPath) | ConvertFrom-Json
    $parameters = @{}
    foreach ($property in $options.PSObject.Properties) { $parameters[$property.Name] = $property.Value }
    $parameters.Structured = $true
    $parameters.Confirm = $false
    $report = & (Join-Path $PSScriptRoot 'configure-tailnet.ps1') @parameters 3>$null 6>$null
    @{ ok = $true; report = $report } | ConvertTo-Json -Depth 8 -Compress
} catch {
    $code = $_.Exception.Data['HerdrSetupCode']
    $allowed = @('api_read_failed', 'api_update_failed', 'key_creation_failed',
        'key_response_invalid', 'key_revocation_unconfirmed', 'policy_invalid',
        'output_exists', 'output_unavailable', 'token_rejected', 'etag_missing', 'key_cleanup_failed', 'prerequisite_version', 'policy_changed')
    if ($code -notin $allowed) { $code = 'local_failure' }
    @{ ok = $false; error_code = $code } | ConvertTo-Json -Compress
}
