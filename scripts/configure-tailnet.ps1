# Developer wrapper. Product users run herdr-mesh setup tailnet.
[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
param(
    [Parameter(Mandatory)][string]$Tailnet,
    [string]$ApiTokenEnvironmentVariable = 'TAILSCALE_API_TOKEN',
    [string]$TagOwner = 'autogroup:admin',
    [ValidateRange(3600, 7776000)][int]$KeyExpirySeconds = 604800,
    [ValidateRange(1, 100)][int]$KeysPerRole = 2,
    [ValidateRange(1, 65535)][int]$DashboardPort,
    [string]$OutputDirectory = (Join-Path $env:LOCALAPPDATA 'herdr-mesh\tailnet-setup'),
    [switch]$Apply,
    [switch]$PolicyOnly,
    [string]$ExpectedPolicySHA256
)
$ErrorActionPreference = 'Stop'
$arguments = @{}
foreach ($key in $PSBoundParameters.Keys) { $arguments[$key] = $PSBoundParameters[$key] }
$arguments.OutputDirectory = $OutputDirectory
& (Join-Path $PSScriptRoot '..\src\internal\setup\assets\configure-tailnet.ps1') @arguments
