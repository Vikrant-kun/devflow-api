import re
import asyncio

# --- Algorithmic Security Gate: High-Confidence Secret Patterns ---
# This costs $0 and runs in milliseconds.
SECRET_PATTERNS = {
    "AWS Access Key": r"AKIA[0-9A-Z]{16}",
    "GitHub Token": r"gh[p|o|u|s|r]_[A-Za-z0-9_]{36}",
    "Stripe Secret Key": r"sk_live_[0-9a-zA-Z]{24}",
    "RSA Private Key": r"-----BEGIN PRIVATE KEY-----"
}

def scan_code_for_secrets(filepath: str, content: str) -> list:
    """
    Scans raw file content for hardcoded secrets using deterministic regex.
    Returns a list of warnings if secrets are found.
    """
    warnings = []
    for secret_name, pattern in SECRET_PATTERNS.items():
        if re.search(pattern, content):
            warnings.append(f"Blocked: Detected potential {secret_name} in {filepath}")
    return warnings

async def build_repo_snapshot(repo: str, token: str, http_client) -> dict:
    """
    Step 4: Repo Snapshot Engine.
    Fetches the GitHub tree exactly ONCE and maps the architecture mathematically.
    """
    headers = {
        "Authorization": f"Bearer {token}", 
        "Accept": "application/vnd.github+json"
    }
    
    # Fetch the entire repository tree recursively
    tree_url = f"https://api.github.com/repos/{repo}/git/trees/recursive=1"
    res = await http_client.get(tree_url, headers=headers, timeout=15.0)
    
    if res.status_code != 200:
        raise Exception(f"Snapshot Failed: Cannot access repo {repo}. Status: {res.status_code}")

    tree = res.json().get("tree", [])
    
    snapshot = {
        "repo_name": repo,
        "files": [],           # Flat list of all relevant files
        "extensions": {},      # Mapped by extension (e.g., {".js": ["auth.js"]})
        "folders": {}          # Mapped by directory (e.g., {"src/components": ["button.tsx"]})
    }

    # Allowed extensions to prevent scanning images, videos, or lockfiles
    valid_exts = (".py", ".js", ".jsx", ".ts", ".tsx", ".css", ".html", ".json", ".md", ".c", ".cpp", ".h", ".go", ".rs")
    ignore_dirs = ("node_modules/", "dist/", "build/", "venv/", ".git/")

    for item in tree:
        path = item["path"]
        
        # Filter out blobs (files) and ignore compiled/vendor directories
        if item["type"] == "blob" and path.endswith(valid_exts) and not any(ign in path for ign in ignore_dirs):
            snapshot["files"].append(path)
            
            # Map by Extension
            ext = "." + path.split(".")[-1]
            snapshot["extensions"].setdefault(ext, []).append(path)
            
            # Map by Folder
            parts = path.split("/")
            folder = "/".join(parts[:-1]) if len(parts) > 1 else "root"
            snapshot["folders"].setdefault(folder, []).append(path)

    return snapshot