"""
Authentication and Authorization
Supabase ES256 JWT verification using JWKS.
"""

import json
import jwt
import requests
from functools import lru_cache
from jwt import ExpiredSignatureError, InvalidTokenError
from jwt.algorithms import ECAlgorithm

SUPABASE_PROJECT_URL = "https://cxvoidjhkbrsjxpipbdg.supabase.co"
JWKS_URL = f"{SUPABASE_PROJECT_URL}/auth/v1/.well-known/jwks.json"

class AuthContext:
    def __init__(self, user_id: str, role: str):
        self.user_id = user_id
        self.role = role

@lru_cache(maxsize=1)
def get_jwks():
    resp = requests.get(JWKS_URL, timeout=5)
    resp.raise_for_status()
    return resp.json()

def parse_auth_header(auth_header: str) -> AuthContext:
    if not auth_header or not auth_header.startswith("Bearer "):
        raise PermissionError("Missing or invalid Authorization header")

    token = auth_header.split(" ", 1)[1]

    try:
        # Read JWT header (no verification yet)
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")
        if not kid:
            raise PermissionError("JWT missing kid")

        # Fetch JWKS and select matching key
        jwks = get_jwks()
        jwk = next(k for k in jwks["keys"] if k["kid"] == kid)

        # Convert JWK → EC public key
        public_key = ECAlgorithm.from_jwk(json.dumps(jwk))

        # Verify JWT
        payload = jwt.decode(
            token,
            key=public_key,
            algorithms=["ES256"],
            audience="authenticated",
            issuer=f"{SUPABASE_PROJECT_URL}/auth/v1",
        )

    except ExpiredSignatureError:
        raise PermissionError("JWT expired")
    except (InvalidTokenError, StopIteration, ValueError):
        raise PermissionError("Invalid JWT")

    user_id = payload.get("sub")
    if not user_id:
        raise PermissionError("Invalid JWT: missing sub")

    return AuthContext(
        user_id=user_id,
        role=payload.get("role", "authenticated"),
    )
