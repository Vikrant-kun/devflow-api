from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    DATABASE_URL: str
    CLERK_SECRET_KEY: str
    GROQ_API_KEY: str
    BREVO_API_KEY: str
    CLERK_ISSUER: str
    FRONTEND_URL: str = ""
    OPENAI_API_KEY: str = ""
    GEMINI_API_KEY: str = ""
    GMAIL_USER: str = ""
    GMAIL_APP_PASSWORD: str = ""
    OPENROUTER_API_KEY: str = ""
    POSTHOG_API_KEY: str = ""

    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()