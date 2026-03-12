import json
import re

async def execute_ai_planner(clean_prompt: str, trimmed_context: dict, _groq_request_func) -> dict:
    """
    Step 9: The Planner (AI Call 1).
    Uses a fast/cheap model to output a strict JSON execution plan.
    Does NOT write the code yet.
    """
    
    # Format the trimmed context for the AI prompt
    context_string = ""
    for filepath, code in trimmed_context.items():
        context_string += f"\n--- FILE: {filepath} ---\n{code}\n"

    system_prompt = """You are an elite, surgical software architect. 
Your job is to read the user's request and the provided code context, then output a strict JSON plan.
DO NOT write the actual code implementation.
DO NOT output markdown, explanations, or any text outside the JSON block.

You must return exactly this JSON structure:
{
    "target_file": "exact/path/to/file.ext",
    "action_type": "modify|create|delete",
    "focus_area": "name of function or class to target",
    "instructions": "1-2 sentences of exact technical instructions for the execution AI"
}"""

    user_prompt = f"""User Request: {clean_prompt}

Code Context:
{context_string}"""

    # Call your existing Groq/LLM wrapper
    res = await _groq_request_func({
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0, # Extremely low temperature for deterministic JSON output
        "response_format": {"type": "json_object"} # Force JSON mode if supported
    }, timeout=15.0)

    if res.status_code != 200:
        return {"error": "Planner AI failed to respond."}

    try:
        content = res.json()["choices"][0]["message"]["content"].strip()
        # Clean up in case the model ignored instructions and wrapped in markdown
        content = content.replace("```json", "").replace("```", "").strip()
        plan = json.loads(content)
        
        # Verify the AI didn't hallucinate a file outside our context
        if plan.get("target_file") not in trimmed_context and plan.get("action_type") != "create":
            plan["target_file"] = list(trimmed_context.keys())[0] # Fallback to the top BM25 ranked file
            
        return plan
        
    except Exception as e:
        return {"error": f"Failed to parse Planner JSON: {str(e)}"}

async def execute_ai_coder(execution_plan: dict, original_file_content: str, _groq_request_func) -> str:
    """
    Step 10: The Executor (AI Call 2).
    Takes the strict JSON plan and writes the actual code fix.
    """
    target_file = execution_plan.get("target_file")
    instructions = execution_plan.get("instructions")
    focus_area = execution_plan.get("focus_area", "entire file")
    action_type = execution_plan.get("action_type")

    # If the AI decided to delete the file, handle it algorithmically (Zero API Cost)
    if action_type == "delete":
        return ""

    system_prompt = """You are an elite, surgical code executor.
Your task is to implement the exact technical instructions provided.
Return ONLY the raw, modified code for the target file.
DO NOT wrap the code in markdown formatting (e.g., ```python).
DO NOT output conversational text, explanations, or comments about the changes."""

    user_prompt = f"""Target File: {target_file}
Focus Area: {focus_area}
Instructions: {instructions}

Original Code:
{original_file_content}"""

    # Call the Heavy Model (e.g., Llama 3 70B, GPT-4o, or Claude 3.5 Sonnet)
    res = await _groq_request_func({
        "model": "llama-3.3-70b-versatile", 
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.2, # Keep it low to prevent creative hallucinations
    }, timeout=60.0)

    if res.status_code != 200:
        raise Exception(f"Executor AI failed: {res.status_code}")

    # Extract the code and strip any rogue markdown the LLM might have hallucinated
    fixed_code = res.json()["choices"][0]["message"]["content"].strip()
    fixed_code = re.sub(r'^```[\w]*\n?', '', fixed_code)
    fixed_code = re.sub(r'\n?```$', '', fixed_code)

    return fixed_code