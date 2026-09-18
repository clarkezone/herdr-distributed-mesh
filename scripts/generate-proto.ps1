[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$protoc = Get-Command protoc -ErrorAction SilentlyContinue
if (-not $protoc) {
    $wingetProtoc = Get-ChildItem "$env:LOCALAPPDATA\Microsoft\WinGet\Packages" `
        -Filter protoc.exe -Recurse -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if (-not $wingetProtoc) {
        throw 'protoc was not found. Install it with: winget install --id Google.Protobuf --exact'
    }
    $protocPath = $wingetProtoc.FullName
} else {
    $protocPath = $protoc.Source
}

$goBin = & go env GOBIN
if (-not $goBin) {
    $goBin = Join-Path (& go env GOPATH) 'bin'
}
$env:PATH = "$goBin;$env:PATH"

foreach ($plugin in @('protoc-gen-go.exe', 'protoc-gen-go-grpc.exe')) {
    if (-not (Test-Path (Join-Path $goBin $plugin))) {
        throw "$plugin was not found in $goBin. Install the protobuf Go plugins documented in README.md."
    }
}

Push-Location $repositoryRoot
try {
    & $protocPath `
        --proto_path=api\proto `
        --go_out=. `
        --go_opt=module=github.com/clarkezone/herdr-distributed-mesh `
        --go-grpc_out=. `
        --go-grpc_opt=module=github.com/clarkezone/herdr-distributed-mesh `
        api\proto\agentflow\v1\control.proto
    if ($LASTEXITCODE -ne 0) {
        throw "protoc failed with exit code $LASTEXITCODE"
    }
} finally {
    Pop-Location
}
