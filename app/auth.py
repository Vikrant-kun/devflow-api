from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
import httpx

from app.config import settings

# ────────────────────────────────────────────────────────────────
# JWKS caching (global, simple, fetched only once)
# ────────────────────────────────────────────────────────────────

_jwks_cache = None


def get_jwks() -> dict:
    """
    Fetch Supabase JWKS once and cache it globally.
    Subsequent calls return the cached value.
    """
    global _jwks_cache
    if _jwks_cache is not None:
        return _jwks_cache

    url = f"{settings.SUPABASE_URL.rstrip('/')}/auth/v1/.well-known/jwks.json"

    try:
        resp = httpx.get(url, timeout=10.0)
        resp.raise_for_status()
        _jwks_cache = resp.json()
        return _jwks_cache
    except Exception as exc:
        raise RuntimeError(f"Failed to fetch Supabase JWKS from {url}: {exc}") from exc


# ────────────────────────────────────────────────────────────────
# FastAPI dependency
# ────────────────────────────────────────────────────────────────

bearer_scheme = HTTPBearer(
    scheme_name="Bearer",
    auto_error=True
)


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)
) -> dict:
    """
    Validate Supabase JWT and return basic user info.
    Uses cached JWKS.
    """
    token = credentials.credentials

    try:
        # Get JWKS (cached after first call)
        jwks = get_jwks()

        # Get the key ID from the token header
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")

        # Find matching key
        key = next((k for k in jwks.get("keys", []) if k.get("kid") == kid), None)

        # Fallback: use first key if no match (common in some Supabase setups)
        if key is None and jwks.get("keys"):
            key = jwks["keys"][0]

        if key is None:
            raise JWTError("No matching JWKS key found")

        # Decode and verify
        payload = jwt.decode(
            token,
            key,
            algorithms=["RS256"],          # Supabase usually uses RS256 (not HS256/ES256)
            options={
                "verify_aud": False,       # Supabase tokens often don't include audience
                "verify_iss": True,
                "verify_exp": True,
                "verify_signature": True,
            }
        )

        user_id: str | None = payload.get("sub")
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token missing subject (sub) claim"
            )

        return {
            "user_id": user_id,
            "email": payload.get("email", ""),
            # You can add more fields if needed: phone, role, last_sign_in, etc.
        }

    except JWTError as jwt_err:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Could not validate credentials: {str(jwt_err)}"
        ) from jwt_err

    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication service error"
        ) from exc