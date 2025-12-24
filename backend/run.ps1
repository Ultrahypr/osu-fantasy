# PowerShell helper to set up and run the backend (Windows)
# Run: .\run.ps1

Set-Location -Path $PSScriptRoot

$venvPath = Join-Path $PSScriptRoot ".venv"
$python = Join-Path $venvPath "Scripts\\python.exe"

# 1) create venv if needed
if (-not (Test-Path $python)) {
  python -m venv $venvPath
}

# 2) install deps
& $python -m pip install -r requirements.txt

# 3) run server
& $python -m uvicorn app.main:app --reload --port 8000
