# PowerShell helper to set up and run the backend (Windows)
# Run: .\run.ps1

# 1) create venv if needed
python -m venv .venv

# 2) activate (uncomment in interactive use)
.\.venv\Scripts\Activate.ps1

# 3) install deps
pip install -r requirements.txt

# 4) run server
uvicorn app.main:app --reload --port 8000
