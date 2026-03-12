from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str
    CLERK_SECRET_KEY: str
    GROQ_API_KEY: str
    BREVO_API_KEY: str 
    FRONTEND_URL: str = "http://localhost:5173"
    OPENAI_API_KEY: str = ""
    GEMINI_API_KEY: str = ""
    GMAIL_USER: str = ""
    GMAIL_APP_PASSWORD: str = ""
    OPENROUTER_API_KEY: str = ""
    POSTHOG_API_KEY: str = ""

    class Config:
        env_file = ".env"

settings = Settings()