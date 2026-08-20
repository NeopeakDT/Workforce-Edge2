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

from common.db import get_cursor

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


def user_can_access_farm(user_id: str, farm_id: str) -> bool:
    """
    Farm-level authorization check for FastAPI routes.

    Mirrors the authorization semantics already used by the
    get_posture_trend() Postgres RPC (STEP1_DATABASE_BASELINE.sql):
    a global ADMIN, or the OWNER of the farm, or any user with a
    user_farm_access row for that farm, is allowed.

    The RPC itself relies on auth.uid() inside Postgres RLS, which is
    only populated when Supabase/PostgREST runs the query as the caller.
    Our backend connects via a service-role pooled connection with no
    auth.uid() context, so this is a Python-side re-check against the
    same tables (user_profile, user_farm_access) instead of calling the
    RLS-bound RPC.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM public.user_profile
            WHERE id = %s AND role = 'ADMIN'
            UNION ALL
            SELECT 1
            FROM public.user_farm_access
            WHERE user_id = %s AND farm_id = %s
            LIMIT 1
            """,
            (user_id, user_id, farm_id),
        )
        return cur.fetchone() is not None
