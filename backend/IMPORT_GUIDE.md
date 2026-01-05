# Import Guide: When to Add Path Setup

## Quick Answer

**Only add path setup to standalone scripts** (files in `scripts/` and `ops/` that are run directly with `python script.py`).

**Do NOT add it to:**
- Modules imported by FastAPI (in `api/`, `aggregation/`, etc.)
- Files in `common/` (they should use relative imports)

---

## Detailed Rules

### ✅ YES - Add Path Setup

**Standalone Scripts** (run directly with `python`):

1. **Files in `scripts/`** - Test scripts, utilities
   ```python
   from common.path_setup import setup_backend_path
   setup_backend_path()
   from common.db import get_cursor
   ```

2. **Files in `ops/`** - Operational scripts
   ```python
   from common.path_setup import setup_backend_path
   setup_backend_path()
   from common.db import get_cursor
   ```

**Example:**
- `scripts/test_db_connect.py` ✅
- `scripts/test_auth_jwt.py` ✅
- `ops/device_provisioning.py` ✅

---

### ❌ NO - Don't Add Path Setup

**1. FastAPI Modules** (imported by `main.py`):
- Files in `api/`, `aggregation/`, `alerts/`, `dashboard/`
- These use relative imports: `from ..common.db import get_cursor`
- Or absolute imports work because `main.py` is in `backend/`

**Example:**
```python
# api/event_ingest_api.py
from ..common.db import get_db  # Relative import ✅
# OR
from common.db import get_db  # Works because main.py sets context
```

**2. Common Module Files** (files in `common/`):
- Should use **relative imports** since they're in the same package
- `from .db import get_cursor` ✅
- `from common.db import get_cursor` ❌ (circular/wrong)

**Example:**
```python
# common/audit_logger.py
from .db import get_cursor  # Relative import ✅
from .time_utils import utc_now  # Relative import ✅
```

**3. Main Application** (`main.py`):
- Already in `backend/` root, so imports work directly
- Uses relative imports for submodules: `from .api import ...`

---

## How to Use Path Setup

**Copy this pattern into your standalone scripts:**

```python
import sys
from pathlib import Path

# Setup path for imports (allows script to run from any directory)
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

# Now you can import from common
from common.db import get_cursor
from common.auth import parse_auth_header
```

**What this does:**
1. Gets the script's directory: `Path(__file__).resolve()`
2. Goes up to backend root: `.parent.parent`
3. Adds backend/ to Python's search path
4. Now `from common.db` works!

---

## File Type Summary

| File Location | Type | Path Setup Needed? | Import Style |
|--------------|------|-------------------|--------------|
| `scripts/*.py` | Standalone script | ✅ YES | Copy path setup pattern (see below) |
| `ops/*.py` | Standalone script | ✅ YES | Copy path setup pattern (see below) |
| `api/*.py` | FastAPI module | ❌ NO | `from ..common.db import get_db` |
| `aggregation/*.py` | FastAPI module | ❌ NO | `from ..common.db import get_cursor` |
| `common/*.py` | Common module | ❌ NO | `from .db import get_cursor` (relative) |
| `main.py` | FastAPI app | ❌ NO | `from .api import ...` (relative) |

---

## Examples

### ✅ Correct: Standalone Script
```python
# scripts/my_script.py
import sys
from pathlib import Path

# Setup path for imports
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.auth import parse_auth_header

def main():
    # Your code here
    pass
```

### ✅ Correct: FastAPI Module
```python
# api/event_ingest_api.py
from fastapi import APIRouter
from ..common.db import get_db  # Relative import

router = APIRouter()
```

### ✅ Correct: Common Module
```python
# common/audit_logger.py
from .db import get_cursor  # Relative import
from .time_utils import utc_now  # Relative import
```

---

## Why This Works

1. **Standalone Scripts**: Run with `python script.py` from any directory, so they need path setup
2. **FastAPI Modules**: Imported by `main.py` which is in `backend/`, so context is already set
3. **Common Modules**: In the same package, so relative imports work best

---

## Summary

**Only add `setup_backend_path()` to files you run directly:**
- `scripts/*.py` ✅
- `ops/*.py` ✅

**Don't add it to files that are imported:**
- `api/*.py` ❌
- `aggregation/*.py` ❌
- `common/*.py` ❌ (use relative imports instead)
- `main.py` ❌

