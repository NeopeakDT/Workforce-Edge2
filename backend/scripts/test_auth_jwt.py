# scripts/test_auth_jwt.py

import sys
from pathlib import Path

# Setup path for imports (allows script to run from any directory)
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from dotenv import load_dotenv
load_dotenv()

from common.auth import parse_auth_header

# PASTE A REAL SUPABASE ACCESS TOKEN HERE
ACCESS_TOKEN = "PASTE_REAL_ACCESS_TOKEN_HERE"

def main():
    print("🔐 Validating Supabase JWT...")
    print()
    
    if ACCESS_TOKEN == "PASTE_REAL_ACCESS_TOKEN_HERE":
        print("❌ ERROR: Please paste a real Supabase access token in ACCESS_TOKEN variable")
        print()
        print("💡 How to get a Supabase access token:")
        print("   1. Go to Supabase Dashboard")
        print("   2. Settings → API")
        print("   3. Copy the 'anon' or 'service_role' key")
        print("   4. Or use a JWT token from a logged-in user")
        print()
        sys.exit(1)
    
    try:
        auth_header = f"Bearer {ACCESS_TOKEN}"
        ctx = parse_auth_header(auth_header)
        
        print("✅ JWT validation OK")
        print()
        print("User ID:", ctx.user_id)
        print("Role:", ctx.role)
        print()
        
    except PermissionError as e:
        print(f"❌ Authentication Error: {str(e)}")
        print()
        print("💡 Make sure the token is in 'Bearer <token>' format")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print()
        print("💡 Common issues:")
        print("   - Invalid or expired token")
        print("   - SUPABASE_JWT_SECRET not set in .env")
        print("   - Token format is incorrect")
        sys.exit(1)

if __name__ == "__main__":
    main()
