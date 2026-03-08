from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    SUPABASE_URL: str
    SUPABASE_SERVICE_KEY: str
    SUPABASE_JWT_SECRET: str
    GROQ_API_KEY: str
    RESEND_API_KEY: str = "re_SjMdHedz_GewyfrX6iqVCNbE9YMqcAKP6"
    FRONTEND_URL: str = "http://localhost:5173"
    OPENAI_API_KEY: str = ""
    GEMINI_API_KEY: str = ""

    class Config:
        env_file = ".env"

settings = Settings()
