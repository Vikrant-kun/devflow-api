import ast
from app.services.ast_engine import get_parser, LANGUAGE_MAP

def local_syntax_check(file_path: str, fixed_code: str) -> tuple[bool, str]:
    """
    Step 11: The Algorithmic Syntax Check.
    Instantly fails broken code before it ever touches a test suite or an AI Critic.
    """
    ext = "." + file_path.split(".")[-1]
    
    # Fast-path for Python using built-in AST
    if ext == ".py":
        try:
            ast.parse(fixed_code)
            return True, "Syntax valid"
        except SyntaxError as e:
            return False, f"Python SyntaxError at line {e.lineno}: {e.msg}"

    # Universal path for everything else using Tree-sitter
    lang_str = LANGUAGE_MAP.get(ext)
    if not lang_str:
        return True, "No parser available, skipping syntax check"
        
    parser = get_parser(lang_str)
    tree = parser.parse(bytes(fixed_code, "utf8"))
    
    # Tree-sitter flags syntax errors with an 'ERROR' node type
    has_error = False
    def check_errors(node):
        nonlocal has_error
        if node.type == 'ERROR' or node.is_missing:
            has_error = True
        for child in node.children:
            check_errors(child)
            
    check_errors(tree.root_node)
    
    if has_error:
        return False, f"Syntax error detected in {lang_str} file structure."
        
    return True, "Syntax valid"


def detect_manifest(repo_files: list) -> dict:
    """
    Step 12: Manifest Detector.
    Scans the repository root to figure out what Docker container and test command to use.
    """
    # Look at files in the root directory (no slashes)
    root_files = [f for f in repo_files if "/" not in f]
    
    # The deterministic mapping dictionary
    MANIFEST_MAP = {
        "package.json": {
            "image": "node:18-alpine",
            "setup": "npm install",
            "test": "npm test",
            "fallback": "node --check" # If no tests exist
        },
        "requirements.txt": {
            "image": "python:3.11-slim",
            "setup": "pip install -r requirements.txt",
            "test": "pytest",
            "fallback": "python -m py_compile"
        },
        "Cargo.toml": {
            "image": "rust:latest",
            "setup": "",
            "test": "cargo test",
            "fallback": "cargo check"
        },
        "go.mod": {
            "image": "golang:1.21",
            "setup": "go mod download",
            "test": "go test ./...",
            "fallback": "go build"
        }
    }
    
    for manifest_file, config in MANIFEST_MAP.items():
        if manifest_file in root_files:
            return config
            
    # Default fallback if no known manifest is found
    return {
        "image": "ubuntu:latest",
        "setup": "echo 'No manifest found'",
        "test": "echo 'No tests defined'",
        "fallback": "echo 'Skipped'"
    }