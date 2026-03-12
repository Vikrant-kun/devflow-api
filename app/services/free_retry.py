import re
from app.config import settings

async def execute_free_retry(target_file: str, broken_code: str, error_log: str, http_client) -> str:
    """
    Step 14: The Free Retry Loop.
    Uses OpenRouter's free tier (e.g., DeepSeek) to fix the code based on the Sandbox error log.
    Cost: $0.00
    """
    
    system_prompt = """You are an expert debugging assistant.
Your code failed the test suite. Review the console error log and the broken code.
Return ONLY the fully corrected raw code. 
DO NOT use markdown formatting (e.g., ```python).
DO NOT explain what went wrong."""

    user_prompt = f"""Target File: {target_file}

Console Error Log:
{error_log}

Broken Code:
{broken_code}"""

    # Hit OpenRouter's free DeepSeek endpoint
    # You will need an OpenRouter API key in your settings
    res = await http_client.post(
        "[https://openrouter.ai/api/v1/chat/completions](https://openrouter.ai/api/v1/chat/completions)",
        headers={
            "Authorization": f"Bearer {settings.OPENROUTER_API_KEY}",
            "HTTP-Referer": "[https://your-devflow-domain.com](https://your-devflow-domain.com)", # Required by OpenRouter
            "X-Title": "DevFlow AI",
            "Content-Type": "application/json"
        },
        json={
            "model": "deepseek/deepseek-r1:free", 
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0.1
        },
        timeout=60.0
    )

    if res.status_code != 200:
        # If the free API is down/rate-limited, return an empty string to break the loop
        return ""

    # Extract and clean the fixed code
    fixed_code = res.json()["choices"][0]["message"]["content"].strip()
    fixed_code = re.sub(r'^```[\w]*\n?', '', fixed_code)
    fixed_code = re.sub(r'\n?```$', '', fixed_code)

    return fixed_code