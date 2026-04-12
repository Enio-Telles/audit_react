## 2024-05-24 - Overly Permissive CORS Configuration
**Vulnerability:** The CORS configuration in `backend/main.py` used insecure wildcards (`*`) for allowed methods and headers, and hardcoded `localhost` origins, increasing the risk of CSRF and unauthorized cross-origin requests.
**Learning:** Default FastAPI examples often use `allow_methods=["*"]` and `allow_headers=["*"]` for convenience during development, which can accidentally leak into production.
**Prevention:** Always explicitly list allowed HTTP methods (e.g., `['GET', 'POST']`) and allowed headers (e.g., `['Content-Type', 'Authorization']`). Fetch allowed origins dynamically from environment variables rather than hardcoding them.

## 2025-02-28 - Path Traversal in File Export
**Vulnerability:** The API accepted unvalidated absolute paths and `..` sequences for the `output_dir` parameter, allowing arbitrary file writes to the server's filesystem when generating notifications.
**Learning:** Using `Path(user_input).expanduser()` does not safely confine paths. Even in desktop-like API applications where absolute paths might be expected from the frontend, they must be strictly validated against a known safe base directory.
**Prevention:** Always validate user-supplied directory paths by checking for `..`, safely resolving them (e.g., against `WORKSPACE_ROOT`), and enforcing `.is_relative_to(base.resolve())` to prevent path traversal and arbitrary writes.
