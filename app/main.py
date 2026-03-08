from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings
from app.routes import workflows, runs, health, github

app = FastAPI(
    title="DevFlow API",
    description="Backend for DevFlow AI — workflow automation platform",
    version="1.0.0"
)

# ── CORS ─────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
 allow_origins=[
    settings.FRONTEND_URL,
    "http://localhost:5173",
    "http://localhost:5174",
    "http://localhost:3000",
    "http://localhost:4173",
    "https://dev-flow-ai-wheat.vercel.app",  # add this
],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routes ───────────────────────────────────────────────────────
app.include_router(health.router)
app.include_router(workflows.router)
app.include_router(runs.router)
app.include_router(github.router)
