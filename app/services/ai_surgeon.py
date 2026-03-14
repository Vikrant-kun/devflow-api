import json
import re

async def execute_ai_planner(clean_prompt: str, trimmed_context: dict, _groq_request_func) -> dict:
    context_string = ""
    for filepath, code in trimmed_context.items():
        context_string += f"\n--- FILE: {filepath} ---\n{code}\n"

    # Detect scan intent from prompt so planner knows not to modify
    prompt_lower = clean_prompt.lower()
    is_scan = any(w in prompt_lower for w in ["scan", "check", "inspect", "analyze", "analyse", "review", "audit", "find error", "find bug"])

    action_instruction = (
        'action_type MUST be "scan" — do NOT modify or fix anything, only report issues'
        if is_scan else
        'action_type must be "modify", "create", or "delete"'
    )

    system_prompt = f"""You are an elite, surgical software architect.
Your job is to read the user's request and the provided code context, then output a strict JSON plan.
DO NOT write the actual code implementation.
DO NOT output markdown, explanations, or any text outside the JSON block.

{action_instruction}

You must return exactly this JSON structure:
{{
    "target_file": "exact/path/to/file.ext",
    "action_type": "scan|modify|create|delete",
    "focus_area": "name of function or class to target",
    "instructions": "1-2 sentences of exact technical instructions for the execution AI"
}}"""

    user_prompt = f"""User Request: {clean_prompt}

Code Context:
{context_string}"""

    res = await _groq_request_func({
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"}
    }, timeout=15.0)

    if res.status_code != 200:
        return {"error": "Planner AI failed to respond."}

    try:
        content = res.json()["choices"][0]["message"]["content"].strip()
        content = content.replace("```json", "").replace("```", "").strip()
        plan = json.loads(content)

        # Force scan if intent was scan — never let AI override this
        if is_scan:
            plan["action_type"] = "scan"

        if plan.get("target_file") not in trimmed_context and plan.get("action_type") != "create":
            plan["target_file"] = list(trimmed_context.keys())[0]

        return plan

    except Exception as e:
        return {"error": f"Failed to parse Planner JSON: {str(e)}"}


async def execute_ai_scanner(execution_plan: dict, original_file_content: str, _groq_request_func) -> str:
    """
    Scan-only mode: reads the file, reports bugs/errors, does NOT modify anything.
    Returns a plain-English report of issues found.
    """
    target_file = execution_plan.get("target_file")
    instructions = execution_plan.get("instructions", "Find all bugs, errors, and issues.")

    scanner_prompt = f"""You are a senior code reviewer. Your ONLY job is to READ and REPORT.
DO NOT rewrite the code. DO NOT fix anything. DO NOT output any code blocks.

File: {target_file}
Task: {instructions}

Analyze the code below and respond with:
1. A one-line summary: either "ERRORS_FOUND: <short description>" or "NO_ERRORS: Code looks clean."
2. If errors found, list each issue as a bullet point with: the line/function, what the problem is, and severity (low/medium/high).

Code:
{original_file_content}
"""

    res = await _groq_request_func({
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": scanner_prompt}],
        "temperature": 0,
    }, timeout=30.0)

    if res.status_code != 200:
        raise Exception(f"Scanner AI failed: {res.status_code}")

    return res.json()["choices"][0]["message"]["content"].strip()


async def execute_ai_coder(execution_plan: dict, original_file_content: str, _groq_request_func) -> str:
    """
    Modify/create mode: writes the actual code fix.
    Only called when action_type is modify or create — never for scan.
    """
    target_file = execution_plan.get("target_file")
    instructions = execution_plan.get("instructions")
    focus_area = execution_plan.get("focus_area", "entire file")
    action_type = execution_plan.get("action_type")

    if action_type == "delete":
        return ""

    surgeon_prompt = f"""You are a senior software engineer.

Task: {action_type} in {target_file}

Instruction: {instructions or focus_area or 'Standard cleanup'}

CRITICAL RULES:
1. Output ONLY the code, but include a single-line comment at the top explaining any safety corrections made.
2. If the user asks for broken syntax, you MUST correct it and explain why in the header comment.
3. Return valid, production-ready code.

Original Code:
{original_file_content}
"""

    res = await _groq_request_func({
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": surgeon_prompt}],
        "temperature": 0.2,
    }, timeout=60.0)

    if res.status_code != 200:
        raise Exception(f"Executor AI failed: {res.status_code}")

    fixed_code = res.json()["choices"][0]["message"]["content"].strip()
    fixed_code = re.sub(r'^```[\w]*\n?', '', fixed_code)
    fixed_code = re.sub(r'\n?```$', '', fixed_code)

    return fixed_code