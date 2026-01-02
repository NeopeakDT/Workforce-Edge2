"""
Authentication and Authorization
Handles API authentication and role-based access control.

- Decodes Supabase JWT
- Normalizes user_id + role
- Backend and RLS stay consisten
"""
# common/auth.py

import os
import jwt

SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")

if not SUPABASE_JWT_SECRET:
    raise RuntimeError("SUPABASE_JWT_SECRET not set")

class AuthContext:
    def __init__(self, user_id: str, role: str):
        self.user_id = user_id
        self.role = role

def parse_auth_header(auth_header: str) -> AuthContext:
    if not auth_header or not auth_header.startswith("Bearer "):
        raise PermissionError("Missing or invalid Authorization header")

    token = auth_header.split(" ")[1]

    payload = jwt.decode(
        token,
        SUPABASE_JWT_SECRET,
        algorithms=["HS256"],
        options={
            "verify_aud": False,
            "verify_iss": False,
        },
    )

    return AuthContext(
        user_id=payload["sub"],
        role=payload.get("role", "authenticated"),
    )
