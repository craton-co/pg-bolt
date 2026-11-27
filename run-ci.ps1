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

& (Join-Path $PSScriptRoot "scripts\ci-local.ps1") @PSBoundParameters
