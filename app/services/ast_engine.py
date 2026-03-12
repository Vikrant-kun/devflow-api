from tree_sitter_languages import get_parser

# Map file extensions to Tree-sitter parsers
LANGUAGE_MAP = {
    ".py": "python",
    ".js": "javascript",
    ".jsx": "javascript",
    ".ts": "typescript",
    ".tsx": "tsx",
    ".go": "go",
    ".rs": "rust",
    ".c": "c",
    ".cpp": "cpp"
}

def extract_ast_data(file_path: str, content: str) -> dict:
    """
    Step 5: Tree-sitter AST Engine.
    Reads a file and extracts the exact lines of functions and its import statements.
    """
    ext = "." + file_path.split(".")[-1]
    lang_str = LANGUAGE_MAP.get(ext)
    
    # Fallback if we don't have a parser for this exact extension
    if not lang_str:
        return {"functions": [], "imports": []}
        
    parser = get_parser(lang_str)
    tree = parser.parse(bytes(content, "utf8"))
    
    functions = []
    imports = []
    
    def traverse(node):
        # Universal catch for functions, methods, and declarations
        if any(keyword in node.type for keyword in ["function", "method", "class_definition"]):
            functions.append({
                "type": node.type,
                "start_line": node.start_point[0],
                "end_line": node.end_point[0]
            })
            
        # Universal catch for imports and includes
        elif any(keyword in node.type for keyword in ["import", "include", "require"]):
            # Extract the raw text of the import line
            import_text = content[node.start_byte:node.end_byte].strip()
            imports.append(import_text)
            return # Skip children of imports to save traversal time
            
        for child in node.children:
            traverse(child)
            
    traverse(tree.root_node)
    
    return {
        "file": file_path,
        "functions": functions,
        "imports": imports
    }

def build_dependency_graph(ast_index: list, snapshot_files: list) -> dict:
    """
    Step 6: Dependency Graph Builder.
    Maps which files are structurally linked based on AST import data.
    """
    graph = {}
    
    for entry in ast_index:
        file_path = entry["file"]
        imports = entry["imports"]
        graph[file_path] = []
        
        for imp in imports:
            # Check if any file in our repo snapshot is mentioned in this import statement
            for target_file in snapshot_files:
                # Extract the base name (e.g., 'auth' from 'src/auth.js')
                target_name = target_file.split("/")[-1].split(".")[0]
                
                # If the target file name is inside the import string, it's a dependency
                if target_name in imp and target_file != file_path:
                    if target_file not in graph[file_path]:
                        graph[file_path].append(target_file)
                        
    return graph

def trim_code_context(target_files: list, file_contents_map: dict, ast_index: list, clean_prompt: str, max_functions: int = 2) -> dict:
    """
    Step 8: AST Token Trimmer.
    Reduces large files to just the relevant function blocks using AST line numbers.
    """
    query_words = set(clean_prompt.lower().split())
    trimmed_context = {}

    for filepath in target_files:
        raw_code = file_contents_map.get(filepath, "")
        if not raw_code:
            continue

        lines = raw_code.split("\n")
        
        # Find the AST entry for this specific file
        file_ast = next((entry for entry in ast_index if entry["file"] == filepath), None)
        
        # Fallback: if no AST functions exist (e.g., config file), send a small snippet
        if not file_ast or not file_ast.get("functions"):
            trimmed_context[filepath] = "\n".join(lines[:100])
            continue
            
        selected_chunks = []
        
        # Always inject the imports at the top so the AI knows the dependencies
        imports = file_ast.get("imports", [])
        if imports:
            selected_chunks.append("\n".join(imports))

        # Score each function body against the user's intent
        scored_functions = []
        for func in file_ast["functions"]:
            start = func["start_line"]
            end = func["end_line"]
            
            # Extract the precise function block (Tree-sitter uses 0-indexed lines)
            # We add 1 to the end to ensure we capture the closing bracket/brace
            func_text = "\n".join(lines[start:end+1])
            
            # Score how relevant this specific function is to the prompt
            func_words = set(func_text.lower().split())
            score = len(query_words & func_words)
            
            scored_functions.append((score, func_text))
            
        # Sort functions by relevance and take the top N (default: 2)
        scored_functions.sort(key=lambda x: x[0], reverse=True)
        
        for score, chunk in scored_functions[:max_functions]:
            selected_chunks.append(chunk)
            
        # Stitch it perfectly together with clear separators
        trimmed_context[filepath] = "\n\n/* ... hidden code ... */\n\n".join(selected_chunks)
        
    return trimmed_context