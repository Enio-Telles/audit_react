## 2024-05-24 - Overly Permissive CORS Configuration
**Vulnerability:** The CORS configuration in `backend/main.py` used insecure wildcards (`*`) for allowed methods and headers, and hardcoded `localhost` origins, increasing the risk of CSRF and unauthorized cross-origin requests.
**Learning:** Default FastAPI examples often use `allow_methods=["*"]` and `allow_headers=["*"]` for convenience during development, which can accidentally leak into production.
**Prevention:** Always explicitly list allowed HTTP methods (e.g., `['GET', 'POST']`) and allowed headers (e.g., `['Content-Type', 'Authorization']`). Fetch allowed origins dynamically from environment variables rather than hardcoding them.

## 2024-05-25 - Arbitrary File Write via Unvalidated Output Directory
**Vulnerability:** The `output_dir` provided by the user in `fisconforme.py` was used directly with `Path(output_dir).expanduser()` to save files, allowing arbitrary file writes anywhere on the system.
**Learning:** Internal tool features like "save to custom directory" must be confined to a safe base directory when implemented in a web backend to prevent path traversal and arbitrary writes.
**Prevention:** Always validate user-provided paths by checking for `..`, resolving them safely against a predefined base directory, and enforcing `.is_relative_to(safe_base)`.

## 2024-05-26 - Path Traversal bypass via startswith
**Vulnerability:** Path validation checking if a directory path string starts with another directory path string using `startswith` is insecure. An attacker could bypass the check if the target directory name is a prefix of the actual directory. For instance, if `SQL_ROOT` is `/app/sql`, `/app/sql_fake` would bypass the `startswith('/app/sql')` check.
**Learning:** Path strings are not just characters. Using string operations for path containment checks is dangerous.
**Prevention:** Always use `path.is_relative_to(base_path.resolve())` to safely ensure a path is confined within a directory tree.
