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
ACCESS_TOKEN = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjVlYzU5OWI5LTkyMTEtNDRlNC1iNmI0LTU3NjY3ZTJkM2Y3NSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL2N4dm9pZGpoa2Jyc2p4cGlwYmRnLnN1cGFiYXNlLmNvL2F1dGgvdjEiLCJzdWIiOiJkNDY0NmQwYi05ODg3LTQ3ZWYtOGE0Yi0zYzVkN2ZjNGYxZTUiLCJhdWQiOiJhdXRoZW50aWNhdGVkIiwiZXhwIjoxNzY3NjgzODk3LCJpYXQiOjE3Njc2ODAyOTcsImVtYWlsIjoibmVvcGVha29mZmljZUBnbWFpbC5jb20iLCJwaG9uZSI6IiIsImFwcF9tZXRhZGF0YSI6eyJwcm92aWRlciI6ImVtYWlsIiwicHJvdmlkZXJzIjpbImVtYWlsIl19LCJ1c2VyX21ldGFkYXRhIjp7ImVtYWlsX3ZlcmlmaWVkIjp0cnVlfSwicm9sZSI6ImF1dGhlbnRpY2F0ZWQiLCJhYWwiOiJhYWwxIiwiYW1yIjpbeyJtZXRob2QiOiJwYXNzd29yZCIsInRpbWVzdGFtcCI6MTc2NzY4MDI5N31dLCJzZXNzaW9uX2lkIjoiNzI5MzgzMTUtOTIwOS00M2JhLWFlMmUtZWVmNmYyMWU1NGY4IiwiaXNfYW5vbnltb3VzIjpmYWxzZX0.pB0IjPkfZ7CnJIgCfyzAafXtVHsNjYuKtsCH5EXjfRMP8CleIBemqLKj0SplAnR1VFm2x5FvIHsuXjWjb6jXfA"
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
