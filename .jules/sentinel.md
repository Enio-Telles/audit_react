## 2024-05-24 - Overly Permissive CORS Configuration
**Vulnerability:** The CORS configuration in `backend/main.py` used insecure wildcards (`*`) for allowed methods and headers, and hardcoded `localhost` origins, increasing the risk of CSRF and unauthorized cross-origin requests.
**Learning:** Default FastAPI examples often use `allow_methods=["*"]` and `allow_headers=["*"]` for convenience during development, which can accidentally leak into production.
**Prevention:** Always explicitly list allowed HTTP methods (e.g., `['GET', 'POST']`) and allowed headers (e.g., `['Content-Type', 'Authorization']`). Fetch allowed origins dynamically from environment variables rather than hardcoding them.
## 2024-06-03 - Path Traversal in File Saving
**Vulnerability:** The API endpoints allowed writing output notifications and ZIP files to arbitrary server directories via an unsanitized `output_dir` parameter, relying on `Path.expanduser()` and `Path.mkdir()`.
**Learning:** Using `Path(user_input).expanduser()` to determine file save paths opens the server to arbitrary file writes. Never trust user input to dictate server-side absolute paths or relative path traversals (`..`).
**Prevention:** Always restrict dynamically generated file paths to a predefined secure directory base using strict verification, rejecting absolute paths, and verifying `str(target_path).startswith(str(safe_base))`.
## 2024-06-05 - Path Traversal in File API endpoints
**Vulnerability:** The API endpoints used `Path(user_input).resolve()` combined with `is_relative_to()` for validation, which fails if an absolute path is provided by a user, or resolves against the current working directory, causing false positives and allowing bypasses depending on the environment.
**Learning:** Using `Path(user_input).resolve()` on user-supplied paths to check if they are within a directory is unreliable, as it resolves relative to the CWD and handles absolute paths unsafely.
**Prevention:** Always use strict string validation to reject path traversal characters (`..`, `/`, `\`) before creating a Path object.
