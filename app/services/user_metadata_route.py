# Add this to your FastAPI router (e.g. routes/user.py)
# Requires: pip install clerk-backend-api  OR  use httpx directly

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
import httpx
import os

router = APIRouter(prefix="/api/user", tags=["user"])

CLERK_SECRET_KEY = os.getenv("CLERK_SECRET_KEY")  # sk_live_... or sk_test_...


class MetadataUpdate(BaseModel):
    bio: str = ""
    location: str = ""
    website: str = ""


# Reuse however you currently extract the Clerk user_id from the JWT
# This assumes you have a get_current_user_id dependency already
@router.patch("/metadata")
async def update_user_metadata(
    payload: MetadataUpdate,
    user_id: str = Depends(get_current_user_id),   # your existing auth dep
):
    """
    Merge bio/location/website into the user's publicMetadata via Clerk Backend API.
    Clerk only allows publicMetadata writes from the backend, never from the frontend.
    """
    if not CLERK_SECRET_KEY:
        raise HTTPException(status_code=500, detail="CLERK_SECRET_KEY not configured")

    async with httpx.AsyncClient() as client:
        # First fetch current metadata so we don't overwrite other keys
        get_res = await client.get(
            f"https://api.clerk.com/v1/users/{user_id}",
            headers={"Authorization": f"Bearer {CLERK_SECRET_KEY}"},
        )
        if get_res.status_code != 200:
            raise HTTPException(status_code=502, detail="Failed to fetch user from Clerk")

        current_meta = get_res.json().get("public_metadata", {})

        # Merge new values in
        updated_meta = {
            **current_meta,
            "bio":      payload.bio,
            "location": payload.location,
            "website":  payload.website,
        }

        patch_res = await client.patch(
            f"https://api.clerk.com/v1/users/{user_id}",
            headers={
                "Authorization": f"Bearer {CLERK_SECRET_KEY}",
                "Content-Type":  "application/json",
            },
            json={"public_metadata": updated_meta},
        )

        if patch_res.status_code != 200:
            raise HTTPException(status_code=502, detail="Failed to update Clerk metadata")

    return {"ok": True}