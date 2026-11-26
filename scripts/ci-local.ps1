# SPDX-License-Identifier: Apache-2.0
<#
.SYNOPSIS
    Run pg_bolt CI locally inside a Linux Docker container.
.PARAMETER PgVersions
    PostgreSQL versions for pgrx checks (e.g. "pg17" or "pg15,pg16,pg17,pg18"). Default: "pg17".
.PARAMETER SkipPgrx
    Skip cargo-pgrx clippy/test matrix.
.PARAMETER SkipDeny
    Skip cargo-deny check.
.PARAMETER RebuildImage
    Force rebuild of the pg_bolt-ci Docker image.
#>
[CmdletBinding()]
param(
    [string]$PgVersions = "pg17",
    [switch]$SkipPgrx,
    [switch]$SkipDeny,
    [switch]$RebuildImage
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$ParentRoot = (Resolve-Path (Join-Path $RepoRoot "..")).Path
$BoltRoot = Join-Path $ParentRoot "bolt"

if (-not (Test-Path $BoltRoot)) {
    throw "Sibling craton-bolt repository not found at '$BoltRoot'."
}

$ImageName = "pg_bolt-ci:latest"
$DockerFile = Join-Path $RepoRoot "docker\Dockerfile.ci"

$ImageExists = $false
if (-not $RebuildImage) {
    $prevEAP = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    docker image inspect $ImageName *> $null
    $ImageExists = ($LASTEXITCODE -eq 0)
    $ErrorActionPreference = $prevEAP
}

if (-not $ImageExists) {
    Write-Host "==> Building Docker image $ImageName..." -ForegroundColor Cyan
    docker build -f $DockerFile -t $ImageName $RepoRoot
    if ($LASTEXITCODE -ne 0) {
        throw "Docker build failed with exit code $LASTEXITCODE"
    }
}

# Ensure cache volumes exist and are writable by the container user (uid 1000)
$Volumes = @("pg_bolt_ci_cargo", "pg_bolt_ci_pgrx", "pg_bolt_ci_target")
foreach ($vol in $Volumes) {
    $prevEAP = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    docker volume inspect $vol *> $null
    $volExists = ($LASTEXITCODE -eq 0)
    $ErrorActionPreference = $prevEAP
    if (-not $volExists) {
        docker volume create $vol | Out-Null
    }
}

# Initialize volume permissions as root so the non-root `ubuntu` user can write to them
docker run --rm --user root --entrypoint /bin/bash `
    -v pg_bolt_ci_cargo:/cache/cargo `
    -v pg_bolt_ci_pgrx:/cache/pgrx `
    -v pg_bolt_ci_target:/cache/target `
    $ImageName -c "chown -R ubuntu:ubuntu /cache"
if ($LASTEXITCODE -ne 0) {
    throw "Failed to prepare cache volumes"
}

$EnvArgs = @(
    "-e", "PG_VERSIONS=$PgVersions",
    "-e", "SKIP_PGRX=$(if ($SkipPgrx) { '1' } else { '0' })",
    "-e", "SKIP_DENY=$(if ($SkipDeny) { '1' } else { '0' })"
)

Write-Host "==> Running local CI in Docker ($ImageName)..." -ForegroundColor Cyan
docker run --rm `
    --shm-size=1g `
    @EnvArgs `
    -v "${RepoRoot}:/workspace/pg_bolt" `
    -v "${BoltRoot}:/workspace/bolt" `
    -v pg_bolt_ci_cargo:/cache/cargo `
    -v pg_bolt_ci_pgrx:/cache/pgrx `
    -v pg_bolt_ci_target:/cache/target `
    $ImageName

if ($LASTEXITCODE -ne 0) {
    throw "Local CI failed with exit code $LASTEXITCODE"
}
