# run-backend.ps1 — start the Pipeline Studio backend on http://127.0.0.1:8000
# Uses the compact-dagster venv as the execution engine; installs the few extra
# runtime deps (fastapi/openpyxl/rapidfuzz/...) if they're missing.
$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$backend = Join-Path $here "backend"

# Locate a Python with compact-dagster importable.
$candidates = @(
  (Join-Path $here "..\..\compact-dagster\.venv\Scripts\python.exe"),   # sibling layout
  (Join-Path $backend ".venv\Scripts\python.exe")                        # dedicated backend venv
)
$py = $null
foreach ($c in $candidates) { if (Test-Path $c) { $py = (Resolve-Path $c).Path; break } }
if (-not $py) {
  Write-Error "No suitable Python venv found. See backend/README.md to set one up (needs compact-dagster's dagster importable)."
  exit 1
}
Write-Host "Using Python: $py"

# Ensure runtime deps are present.
& $py -c "import fastapi, uvicorn, openpyxl, rapidfuzz, multipart, dagster" 2>$null
if ($LASTEXITCODE -ne 0) {
  Write-Host "Installing backend dependencies into the venv..."
  uv pip install --python $py -r (Join-Path $backend "requirements.txt")
}

Write-Host "Starting backend on http://127.0.0.1:8000  (Ctrl+C to stop)"
Push-Location $backend
try { & $py "app.py" } finally { Pop-Location }
