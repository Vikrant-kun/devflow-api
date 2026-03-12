import math
from collections import Counter
import re

class BM25:
    """Lightweight, pure Python implementation of the BM25 ranking algorithm."""
    def __init__(self, corpus: list, k1: float = 1.5, b: float = 0.75):
        self.k1 = k1
        self.b = b
        self.corpus = corpus
        self.doc_lengths = [len(doc) for doc in corpus]
        self.avgdl = sum(self.doc_lengths) / max(len(corpus), 1)
        self.doc_freqs = []
        self.idf = {}
        self._initialize()

    def _initialize(self):
        df = Counter()
        for doc in self.corpus:
            self.doc_freqs.append(Counter(doc))
            for word in set(doc):
                df[word] += 1
        
        num_docs = len(self.corpus)
        for word, freq in df.items():
            # Standard BM25 IDF calculation
            self.idf[word] = math.log(1 + (num_docs - freq + 0.5) / (freq + 0.5))

    def get_scores(self, query: list) -> list:
        scores = []
        for idx in range(len(self.corpus)):
            score = 0
            doc_len = self.doc_lengths[idx]
            frequencies = self.doc_freqs[idx]
            for word in query:
                if word not in frequencies:
                    continue
                freq = frequencies[word]
                numerator = freq * (self.k1 + 1)
                denominator = freq + self.k1 * (1 - self.b + self.b * doc_len / self.avgdl)
                score += self.idf.get(word, 0) * (numerator / denominator)
            scores.append(score)
        return scores

def rank_and_retrieve_files(clean_prompt: str, ast_index: list, dependency_graph: dict, broken_files: list = None, top_n: int = 3) -> list:
    """
    Step 7: The Retrieval Engine.
    Scores files using BM25, overrides with broken files, and expands via the Dependency Graph.
    """
    if broken_files is None:
        broken_files = []

    # 1. Build the searchable corpus from the AST Index
    corpus = []
    file_paths = []
    
    for entry in ast_index:
        file_paths.append(entry["file"])
        
        # Tokenize file path, functions, and imports into a flat list of searchable words
        searchable_text = entry["file"].replace("/", " ").replace(".", " ")
        for func in entry.get("functions", []):
            searchable_text += f" {func.get('type', '')}"
        for imp in entry.get("imports", []):
            searchable_text += f" {imp}"
            
        # Clean and split into words
        tokens = re.sub(r'[^\w\s]', ' ', searchable_text.lower()).split()
        corpus.append(tokens)

    # 2. Score files using BM25
    query_tokens = re.sub(r'[^\w\s]', ' ', clean_prompt.lower()).split()
    bm25 = BM25(corpus)
    scores = bm25.get_scores(query_tokens)

    # Combine scores with file paths and sort descending
    ranked_files = sorted(zip(scores, file_paths), key=lambda x: x[0], reverse=True)
    
    # 3. Apply Filters and VIP Overrides
    selected_files = set(broken_files) # Broken files (from linter/sandbox) ALWAYS get in
    
    for score, filepath in ranked_files:
        if len(selected_files) >= top_n:
            break
        # Only select files that actually scored higher than 0
        if score > 0:
            selected_files.add(filepath)
            
    # 4. Graph Expansion: Pull in direct dependencies for the selected files
    final_context_files = set(selected_files)
    for filepath in selected_files:
        dependencies = dependency_graph.get(filepath, [])
        for dep in dependencies:
            if len(final_context_files) < (top_n + 2): # Allow slight expansion for context
                final_context_files.add(dep)

    return list(final_context_files)