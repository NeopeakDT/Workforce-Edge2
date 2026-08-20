# scripts/test_auth_jwt.py
print("=== test_auth_jwt.py LOADED ===")
import sys
from pathlib import Path

# Ensure backend root is importable
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from dotenv import load_dotenv
load_dotenv()

from common.auth import parse_auth_header

# PASTE A REAL SUPABASE ACCESS TOKEN HERE
# Get it via:
#   Supabase SQL Editor →  select auth.jwt();
ACCESS_TOKEN = "PASTE_REAL_ACCESS_TOKEN_HERE"
# This access token is valid for 1 hour every time run the commands from requirements.txt and generate new one. 
# Add the access tocken in the above varibale and rerun this file to test the authentication.
def main():
    print("🔐 Validating Supabase JWT...\n")

    if ACCESS_TOKEN == "PASTE_REAL_ACCESS_TOKEN_HERE":
        print("❌ ERROR: Please paste a REAL Supabase access token.\n")
        print("💡 How to get a VALID token:")
        print("   Option 1 (recommended):")
        print("     Supabase SQL Editor → run:")
        print("       select auth.jwt();\n")
        print("   Option 2 (frontend login):")
        print("     Login via Supabase Auth and copy access_token\n")
        print("⚠️ Do NOT use:")
        print("   - Legacy JWT Secret")
        print("   - service_role key")
        print("   - project URL\n")
        sys.exit(1)

    try:
        auth_header = f"Bearer {ACCESS_TOKEN}"
        ctx = parse_auth_header(auth_header)

        print("✅ JWT validation OK\n")
        print("User ID:", ctx.user_id)
        print("Role:", ctx.role)
        print()

    except PermissionError as e:
        print(f"❌ Authentication Error: {str(e)}\n")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected Error: {str(e)}\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
