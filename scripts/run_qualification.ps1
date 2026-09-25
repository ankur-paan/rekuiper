# run_qualification.ps1 - Rekuiper Reliability Qualification & Soak Runner (PowerShell)
param (
    [string]$Mode = "short",
    [int]$Duration = 10,
    [int]$Rate = 100,
    [string]$Output = "target/qualification_results.json",
    [string]$Broker = ""
)

$ErrorActionPreference = "Stop"

if (-not $Broker -and $env:REKUIPER_TEST_MQTT) {
    $Broker = $env:REKUIPER_TEST_MQTT
}

Write-Host "============================================================"
Write-Host " rekuiper Reliability Qualification Runner (Windows / PowerShell)"
Write-Host " Mode:     $Mode"
Write-Host " Duration: ${Duration}s"
Write-Host " Rate:     $Rate msg/s"
Write-Host " Output:   $Output"
if ($Broker) {
    Write-Host " Broker:   $Broker"
    $env:REKUIPER_TEST_MQTT = $Broker
}
Write-Host "============================================================"

if ($Mode -eq "soak") {
    Write-Host "[NOTICE] Long-running soak qualification selected (${Duration}s)."
    Write-Host "[NOTICE] For 24h-72h production soak gate, ensure continuous disk monitoring."
}

$env:REKUIPER_QUAL_MODE = $Mode
$env:REKUIPER_QUAL_DURATION_SECS = "$Duration"
$env:REKUIPER_QUAL_RATE = "$Rate"
$env:REKUIPER_QUAL_OUTPUT = $Output

cargo build -p kuiperd
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to build kuiperd binary" -ForegroundColor Red
    exit $LASTEXITCODE
}

cargo test -p rekuiper-server --test qualification_harness -- --nocapture
$testExit = $LASTEXITCODE

if ($testExit -ne 0) {
    Write-Host ""
    Write-Host "[FAILED] Qualification harness failed with exit code $testExit" -ForegroundColor Red
    if (Test-Path "target/qualification_logs/kuiperd_child.log") {
        Write-Host "Preserved child logs: target/qualification_logs/kuiperd_child.log"
    }
    exit $testExit
}

Write-Host ""
Write-Host "[SUCCESS] Qualification harness run finished."
if (Test-Path "target/qualification_logs/kuiperd_child.log") {
    Write-Host "Preserved child logs: target/qualification_logs/kuiperd_child.log"
}
if (Test-Path $Output) {
    Write-Host "Results written to: $Output"
    Get-Content $Output
}
