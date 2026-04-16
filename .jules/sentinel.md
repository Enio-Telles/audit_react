## 2024-05-24 - Overly Permissive CORS Configuration
**Vulnerability:** The CORS configuration in `backend/main.py` used insecure wildcards (`*`) for allowed methods and headers, and hardcoded `localhost` origins, increasing the risk of CSRF and unauthorized cross-origin requests.
**Learning:** Default FastAPI examples often use `allow_methods=["*"]` and `allow_headers=["*"]` for convenience during development, which can accidentally leak into production.
**Prevention:** Always explicitly list allowed HTTP methods (e.g., `['GET', 'POST']`) and allowed headers (e.g., `['Content-Type', 'Authorization']`). Fetch allowed origins dynamically from environment variables rather than hardcoding them.

## 2024-05-25 - Arbitrary File Write via Unvalidated Output Directory
**Vulnerability:** The `output_dir` provided by the user in `fisconforme.py` was used directly with `Path(output_dir).expanduser()` to save files, allowing arbitrary file writes anywhere on the system.
**Learning:** Internal tool features like "save to custom directory" must be confined to a safe base directory when implemented in a web backend to prevent path traversal and arbitrary writes.
**Prevention:** Always validate user-provided paths by checking for `..`, resolving them safely against a predefined base directory, and enforcing `.is_relative_to(safe_base)`.
## 2024-05-26 - Arbitrary File Write via Path Traversal Bypass
**Vulnerability:** The path validation logic in `backend/routers/sql_query.py` used `str.startswith()` to verify if a resolved destination directory was inside the allowed root directory (`SQL_ROOT`). This is insecure because a directory named `/app/sql_fake` technically "starts with" the string `/app/sql`, allowing path traversal bypasses.
**Learning:** String matching on path strings is insufficient for path containment checks and leaves endpoints vulnerable to directory traversal attacks.
**Prevention:** Always use object-based validation methods like `pathlib.Path.is_relative_to()` to perform robust path containment checks.
