"""
Path Setup Utility
Adds backend root to Python path for imports.
Use this in standalone scripts that need to import from common.

IMPORTANT: This file must be imported using a workaround since it's in common/.
See the inline code pattern below instead.
"""

import sys
from pathlib import Path


def _setup_backend_path_internal():
    """
    Internal function - don't call directly.
    Use the inline pattern shown in IMPORT_GUIDE.md instead.
    """
    # Get backend root (parent of common/)
    backend_root = Path(__file__).resolve().parent.parent
    
    # Add to path if not already there
    backend_root_str = str(backend_root)
    if backend_root_str not in sys.path:
        sys.path.insert(0, backend_root_str)
    
    return backend_root


# Inline pattern to use in scripts (copy this):
"""
import sys
from pathlib import Path

# Add backend root to Python path
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

# Now you can import from common
from common.db import get_cursor
"""

