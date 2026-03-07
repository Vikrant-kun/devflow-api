from fastapi import APIRouter
router = APIRouter(tags=["health"])

@router.get("/")
async def root():
    return {"status": "ok", "service": "DevFlow API", "version": "1.0.0"}

@router.get("/health")
async def health():
    return {"status": "healthy"}
