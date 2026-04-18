## 2024-05-24 - Overly Permissive CORS Configuration
**Vulnerability:** The CORS configuration in `backend/main.py` used insecure wildcards (`*`) for allowed methods and headers, and hardcoded `localhost` origins, increasing the risk of CSRF and unauthorized cross-origin requests.
**Learning:** Default FastAPI examples often use `allow_methods=["*"]` and `allow_headers=["*"]` for convenience during development, which can accidentally leak into production.
**Prevention:** Always explicitly list allowed HTTP methods (e.g., `['GET', 'POST']`) and allowed headers (e.g., `['Content-Type', 'Authorization']`). Fetch allowed origins dynamically from environment variables rather than hardcoding them.

## 2024-05-25 - Arbitrary File Write via Unvalidated Output Directory
**Vulnerability:** The `output_dir` provided by the user in `fisconforme.py` was used directly with `Path(output_dir).expanduser()` to save files, allowing arbitrary file writes anywhere on the system.
**Learning:** Internal tool features like "save to custom directory" must be confined to a safe base directory when implemented in a web backend to prevent path traversal and arbitrary writes.
**Prevention:** Always validate user-provided paths by checking for `..`, resolving them safely against a predefined base directory, and enforcing `.is_relative_to(safe_base)`.
## 2024-05-25 - Path traversal via unsafe Path resolution and prefix matching
**Vulnerability:** Path traversal vulnerabilities identified in API endpoints using `Path(user_input).resolve()` directly and `str(dest_dir).startswith()` for validation.
**Learning:** `Path(user_input).resolve()` resolves relative paths against the CWD instead of the intended base directory, bypassing intended security boundaries. String prefix matching for paths can be bypassed by spoofed directories (e.g., `/base_dir_fake`).
**Prevention:** Explicitly reject `..` traversal sequences, safely join relative paths to their intended base directory before resolution, and strictly use `.is_relative_to()` for path containment checks.
