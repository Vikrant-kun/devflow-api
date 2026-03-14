from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings
from app.routes import workflows, runs, health, github, webhooks, ws

app = FastAPI(
    title="DevFlow API",
    description="Backend for DevFlow AI — workflow automation platform",
    version="1.0.0",
    redirect_slashes=False
)

origins = [
    "http://localhost:5173",
    "http://localhost:5174",
    "http://localhost:3000",
    "http://localhost:4173",
    "https://dev-flow-ai-wheat.vercel.app",
]

if settings.FRONTEND_URL and settings.FRONTEND_URL not in origins:
    origins.append(settings.FRONTEND_URL)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(workflows.router)
app.include_router(runs.router)
app.include_router(github.router)
app.include_router(webhooks.router)
app.include_router(ws.router)