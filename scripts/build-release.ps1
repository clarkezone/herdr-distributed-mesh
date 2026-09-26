[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidatePattern('^[0-9A-Za-z][0-9A-Za-z.+_-]{0,63}$')]
    [string]$Version,
    [string]$OutputDirectory,
    [ValidateNotNullOrEmpty()]
    [ValidateSet('windows-amd64', 'windows-arm64', 'linux-amd64', 'linux-arm64', 'darwin-amd64', 'darwin-arm64')]
    [string[]]$Targets = @('windows-amd64', 'windows-arm64', 'linux-amd64', 'linux-arm64', 'darwin-amd64', 'darwin-arm64')
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path $repositoryRoot "dist\$Version"
}
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
$originalGOOS = $env:GOOS
$originalGOARCH = $env:GOARCH
$originalCGO = $env:CGO_ENABLED
$originalGOFIPS140 = $env:GOFIPS140
$releaseGOFIPS140 = 'v1.0.0'
$targetsUnique = @($Targets | Select-Object -Unique)
if ($targetsUnique.Count -ne $Targets.Count) {
    throw 'Release targets must be unique.'
}
foreach ($target in $Targets) {
    if (Test-Path -LiteralPath (Join-Path $OutputDirectory $target)) {
        throw "Release target directory already exists: $target. Choose a new output directory."
    }
    if (Test-Path -LiteralPath (Join-Path $OutputDirectory "herdr-mesh-$Version-$target.zip")) {
        throw "Release archive already exists: $target. Choose a new output directory."
    }
}
$checksumPath = Join-Path $OutputDirectory 'SHA256SUMS'
if (Test-Path -LiteralPath $checksumPath) {
    throw 'SHA256SUMS already exists. Choose a new output directory rather than overwriting a release.'
}

Push-Location $repositoryRoot
try {
    New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
    $env:CGO_ENABLED = '0'
    $env:GOFIPS140 = $releaseGOFIPS140
    $checksums = [Collections.Generic.List[string]]::new()
    foreach ($target in $Targets) {
        $osName, $architecture = $target.Split('-')
        $env:GOOS = $osName
        $env:GOARCH = $architecture
        $directory = Join-Path $OutputDirectory $target
        New-Item -ItemType Directory -Path $directory | Out-Null
        $binaryName = if ($osName -eq 'windows') { 'herdr-mesh.exe' } else { 'herdr-mesh' }
        $binary = Join-Path $directory $binaryName
        & go build -trimpath -ldflags "-X github.com/clarkezone/herdr-distributed-mesh/src/internal/buildinfo.Version=$Version" `
            -o $binary .\src\cmd\herdr-mesh
        if ($LASTEXITCODE -ne 0) {
            throw "Build failed for $target. Partial artifacts remain for inspection; no checksum manifest was published."
        }
        $metadata = (& go version -m $binary 2>&1) -join "`n"
        if ($LASTEXITCODE -ne 0 -or $metadata -notmatch "(?m)^\s*build\s+GOFIPS140=$([regex]::Escape($releaseGOFIPS140))(?:-|$)") {
            throw "Release contract violated: $target was not built with GOFIPS140=$releaseGOFIPS140."
        }
        $archiveName = "herdr-mesh-$Version-$target.zip"
        $archive = Join-Path $OutputDirectory $archiveName
        Compress-Archive -LiteralPath $binary -DestinationPath $archive
        $package = [IO.Compression.ZipFile]::OpenRead($archive)
        try {
            if ($package.Entries.Count -ne 1 -or $package.Entries[0].FullName -cne $binaryName) {
                throw 'Release contract violated: each archive must contain exactly the single public mesh executable.'
            }
        } finally {
            $package.Dispose()
        }
        $hash = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant()
        $checksums.Add("$hash  $archiveName")
    }
    [IO.File]::WriteAllLines($checksumPath, $checksums, [Text.Encoding]::ASCII)
    Write-Output "Built $($Targets.Count) archives with SHA256SUMS in $OutputDirectory"
} finally {
    $env:GOOS = $originalGOOS
    $env:GOARCH = $originalGOARCH
    $env:CGO_ENABLED = $originalCGO
    $env:GOFIPS140 = $originalGOFIPS140
    Pop-Location
}
