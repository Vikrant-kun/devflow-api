from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
import base64
from app.config import settings

bearer_scheme = HTTPBearer()

def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)
) -> dict:
    token = credentials.credentials
    try:
        secret = base64.b64decode(settings.SUPABASE_JWT_SECRET)
        payload = jwt.decode(
            token,
            secret,
            algorithms=["HS256"],
            options={"verify_aud": False}
        )
        user_id: str = payload.get("sub")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
        return {"user_id": user_id, "email": payload.get("email", "")}
    except JWTError as e:
        raise HTTPException(status_code=401, detail=f"Could not validate token: {str(e)}")
