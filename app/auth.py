from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import httpx
import jwt

from app.config import settings

bearer_scheme = HTTPBearer(scheme_name="Bearer", auto_error=True)

_jwks_cache = None

def get_jwks() -> dict:
    global _jwks_cache
    if _jwks_cache is not None:
        return _jwks_cache
    resp = httpx.get("https://api.clerk.com/v1/jwks", headers={
        "Authorization": f"Bearer {settings.CLERK_SECRET_KEY}"
    }, timeout=10.0)
    resp.raise_for_status()
    _jwks_cache = resp.json()
    return _jwks_cache

def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)
) -> dict:
    token = credentials.credentials
    try:
        jwks = get_jwks()
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")
        key = next((k for k in jwks.get("keys", []) if k.get("kid") == kid), None)
        if key is None and jwks.get("keys"):
            key = jwks["keys"][0]
        if key is None:
            raise Exception("No matching key found")

        public_key = jwt.algorithms.RSAAlgorithm.from_jwk(key)
        payload = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            options={"verify_aud": False}
        )
        user_id = payload.get("sub")
        if not user_id:
            raise Exception("No sub claim")
        return {
            "user_id": user_id,
            "email": payload.get("email", "")
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Could not validate credentials: {str(exc)}"
        )