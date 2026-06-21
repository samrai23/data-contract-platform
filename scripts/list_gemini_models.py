"""
scripts/list_gemini_models.py
==============================
Lists all Gemini models that are actually available for generateContent
with your current API key. Run this to find the right model name.

Usage:
    python scripts/list_gemini_models.py
"""
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent
try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env", override=False)
except ImportError:
    pass

api_key = os.getenv("GEMINI_API_KEY", "")
if not api_key:
    print("ERROR: GEMINI_API_KEY not set in .env")
    sys.exit(1)

import urllib.request
import json

url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"

print(f"Fetching models from v1beta API...")
print(f"Key prefix: {api_key[:8]}...")
print()

try:
    with urllib.request.urlopen(url) as resp:
        data = json.loads(resp.read())
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)

models = data.get("models", [])
print(f"Found {len(models)} models. Filtering for generateContent support:\n")

for m in models:
    name = m.get("name", "")
    supported = m.get("supportedGenerationMethods", [])
    if "generateContent" in supported:
        display = name.replace("models/", "")
        print(f"  {display}")
