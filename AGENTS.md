# Project Instructions

## Library Selection Policy
- Do not implement common infrastructure manually if an equivalent standard or already-installed library exists.
- Prefer libraries already present in `deps.edn`.
- When using a library, use the canonical solution pattern provided by that library, not a simplified handwritten substitute around it.
- For command-line parsing, use `clojure.tools.cli`.
- If choosing custom code over an existing library, explain why before editing.

## Selection Order
1. Standard library or official library.
2. Existing dependency already present in `deps.edn`.
3. Small local helper.
4. New dependency only with explicit user approval.
