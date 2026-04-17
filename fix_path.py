import re

file_paths = [
    "backend/routers/cnpj.py",
    "backend/routers/parquet.py"
]

for fp in file_paths:
    with open(fp, "r") as f:
        content = f.read()

    # Pattern for path traversal check
    pattern1 = r"(p = Path\(([^)]+)\)\.resolve\(\)\n\s*if not p\.is_relative_to\(([^)]+)\):)"

    def repl1(m):
        var_name = m.group(2)
        root_name = m.group(3)
        return f"if \"..\" in str({var_name}):\n            raise ValueError()\n        req_p = Path({var_name})\n        p = req_p.resolve() if req_p.is_absolute() else ({root_name} / req_p).resolve()\n        if not p.is_relative_to({root_name}):"

    content = re.sub(pattern1, repl1, content)

    with open(fp, "w") as f:
        f.write(content)
