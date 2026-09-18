#requires -Version 7.4
# Developer fixture wrapper; no shipped script dependency.
if ($MyInvocation.InvocationName -ne '.') {
    throw 'Dot-source this developer fixture wrapper only for tests; the operator entrypoint is herdr-mesh bootstrap.'
}
. (Join-Path $PSScriptRoot '..\src\internal\bootstrap\assets\bootstrap-node.ps1')
