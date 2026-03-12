import difflib
import re

# The DevFlow Dictionary: Add any tech-specific words you want the system to recognize
DEVFLOW_DICT = [
    "scan", "folder", "files", "fix", "bug", "error", "refactor",
    "create", "build", "repository", "security", "login", "auth",
    "pull request", "merge", "deploy", "test", "database", "api"
]

def sanitize_prompt(prompt: str) -> str:
    """
    Step 2: Typo Sanitizer.
    Intercepts the prompt and silently fixes spelling mistakes without calling an AI.
    """
    words = prompt.split()
    sanitized_words = []
    
    for word in words:
        # Strip punctuation to check the raw word
        clean_word = re.sub(r'[^\w\s]', '', word.lower())
        
        if not clean_word:
            sanitized_words.append(word)
            continue
            
        # Find the closest match in our dictionary (requires 75% similarity)
        matches = difflib.get_close_matches(clean_word, DEVFLOW_DICT, n=1, cutoff=0.75)
        
        if matches:
            # Replace the misspelled word but keep the original casing/punctuation
            corrected = word.lower().replace(clean_word, matches[0])
            sanitized_words.append(corrected)
        else:
            sanitized_words.append(word)
            
    return " ".join(sanitized_words)

def parse_intent(prompt: str) -> dict:
    """
    Step 3: FSM Intent Parser.
    Deterministically categorizes the prompt into actionable states.
    """
    prompt_lower = prompt.lower()
    intent = {
        "action": "unknown",   # fix, scan, create, refactor
        "target": "unknown",   # repository, files, folder
        "category": "general"  # security, performance, auth, logic
    }

    # -- State 1: Determine Action --
    if any(word in prompt_lower for word in ["create", "build", "make", "add", "generate"]):
        intent["action"] = "create"
    elif any(word in prompt_lower for word in ["fix", "resolve", "patch", "repair", "debug"]):
        intent["action"] = "fix"
    elif any(word in prompt_lower for word in ["scan", "audit", "check", "review", "find"]):
        intent["action"] = "scan"
    elif any(word in prompt_lower for word in ["refactor", "optimize", "improve", "clean"]):
        intent["action"] = "refactor"
    if any(word in prompt_lower for word in ["delete", "remove", "drop", "clear"]):
        action = "delete"
    elif any(word in prompt_lower for word in ["create", "add", "new", "generate"]):
        action = "create"
    elif any(word in prompt_lower for word in ["update", "modify", "rewrite", "refactor", "change", "fix"]):
        action = "modify"
    else:
        return {"action": "error", "message": "I couldn't figure out if you want to create, modify, or delete a file. Try using words like 'update' or 'create'."}

    if action == "modify" and repo_files:
        # Simple heuristic to see if any filename from the repo is in the prompt
        found_file = any(f.lower() in prompt_lower for f in repo_files)
        if not found_file:
            return {
                "action": "error", 
                "message": f"I couldn't find the file you mentioned in your repository. Please check the spelling or make sure the file exists!"
            }
        
    return {"action": action, "message": "Intent verified."}
    
    # -- State 2: Determine Target --
    if any(word in prompt_lower for word in ["repo", "repository", "all", "project"]):
        intent["target"] = "repository"
    elif any(word in prompt_lower for word in ["file", "script", "code", "function"]):
        intent["target"] = "files"
    elif any(word in prompt_lower for word in ["folder", "directory", "dir"]):
        intent["target"] = "folder"

    # -- State 3: Determine Category/Context --
    if any(word in prompt_lower for word in ["security", "vulnerability", "secret", "leak"]):
        intent["category"] = "security"
    elif any(word in prompt_lower for word in ["performance", "speed", "slow", "lag"]):
        intent["category"] = "performance"
    elif any(word in prompt_lower for word in ["auth", "login", "password", "token"]):
        intent["category"] = "authentication"
    elif any(w in prompt_lower for w in ["ui", "frontend", "css", "design"]):
        intent["category"] = "frontend"

    return intent