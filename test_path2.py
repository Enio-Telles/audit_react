from pathlib import Path

# Simulate CNPJ_ROOT resolution
cnpj_root = Path('/var/www/cnpj').resolve()

# Test cases that might be dangerous
cases = [
    "../../etc/passwd",
    "../",
    "/etc/passwd",
    "C:\\Windows\\System32",
    "test.parquet"
]

for req_path in cases:
    print(f"--- Testing {req_path} ---")

    # 1. Existing buggy implementation
    try:
        p = Path(req_path).resolve()
        is_rel = p.is_relative_to(cnpj_root)
        print(f"[Old] p={p}, is_relative={is_rel}")
    except Exception as e:
        print(f"[Old] Error: {e}")

    # 2. Suggested fix from memory
    try:
        if ".." in req_path:
            raise ValueError("Path traversal detected")

        req_p = Path(req_path)
        if req_p.is_absolute():
            # If the UI sends an absolute path, we must be careful.
            # But the memory says "safely construct the full path (e.g., base / requested if relative)"
            # Let's strip the absolute part or reject it if it's not relative to base
            full = req_p.resolve()
        else:
            full = (cnpj_root / req_p).resolve()

        is_rel = full.is_relative_to(cnpj_root)
        print(f"[New] full={full}, is_relative={is_rel}")
    except Exception as e:
        print(f"[New] Error: {e}")
