import os
import shutil
import subprocess

async def setup_workspace(repo: str, token: str, branch: str = "main") -> str:
    """
    Physically clones the repo into a temporary folder on the server.
    Returns the absolute path to the directory.
    """
    # Create a unique folder name for this specific run
    workspace_id = repo.replace("/", "_")
    base_path = f"/tmp/devflow_{workspace_id}"
    
    # If a previous run left junk behind, clean it up
    if os.path.exists(base_path):
        shutil.rmtree(base_path)
    
    os.makedirs(base_path, exist_ok=True)
    
    # Construct the authenticated clone URL
    clone_url = f"https://x-access-token:{token}@github.com/{repo}.git"
    
    # Shallow clone to save time and disk space (depth=1)
    try:
        subprocess.run(
            ["git", "clone", "--depth", "1", "--branch", branch, clone_url, base_path],
            check=True,
            capture_output=True,
            text=True
        )
        return base_path
    except subprocess.CalledProcessError as e:
        raise Exception(f"Failed to clone repository: {e.stderr}")

def cleanup_workspace(path: str):
    """Removes the workspace from the server disk."""
    if os.path.exists(path):
        shutil.rmtree(path)