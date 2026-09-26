"""
Local conftest for Groq smoke tests.

This file intentionally does NOT import or extend the root conftest.py
so that the DB session fixture (setup_db) is never triggered for tests
that have no database dependency. All groq smoke tests are self-contained
live API calls that only require GROQ_API_KEY in .env.
"""
import sys
from pathlib import Path

# Make sure the intel-system package root is importable
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
