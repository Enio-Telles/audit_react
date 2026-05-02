## 2024-05-24 - Overly Permissive CORS Configuration
**Vulnerability:** The CORS configuration in `backend/main.py` used insecure wildcards (`*`) for allowed methods and headers, and hardcoded `localhost` origins, increasing the risk of CSRF and unauthorized cross-origin requests.
**Learning:** Default FastAPI examples often use `allow_methods=["*"]` and `allow_headers=["*"]` for convenience during development, which can accidentally leak into production.
**Prevention:** Always explicitly list allowed HTTP methods (e.g., `['GET', 'POST']`) and allowed headers (e.g., `['Content-Type', 'Authorization']`). Fetch allowed origins dynamically from environment variables rather than hardcoding them.

## 2024-05-25 - Arbitrary File Write via Unvalidated Output Directory
**Vulnerability:** The `output_dir` provided by the user in `fisconforme.py` was used directly with `Path(output_dir).expanduser()` to save files, allowing arbitrary file writes anywhere on the system.
**Learning:** Internal tool features like "save to custom directory" must be confined to a safe base directory when implemented in a web backend to prevent path traversal and arbitrary writes.
**Prevention:** Always validate user-provided paths by checking for `..`, resolving them safely against a predefined base directory, and enforcing `.is_relative_to(safe_base)`.
## 2025-05-02 - CRLF Environment Variable Injection

**Vulnerability:** The application allowed user-controlled configuration values to be written directly into the `.env` file without sanitization, leading to CRLF/environment variable injection if users included newline characters. Furthermore, it used direct string replacement in `re.sub`, which crashed on backslashes in user input (like Windows file paths).
**Learning:** Writing configuration directly to a `.env` file via user inputs is dangerous. A user could append malicious parameters by injecting `\nMALICIOUS_PARAM=value`. Additionally, `re.sub` parses backslashes in replacement strings as escape sequences.
**Prevention:** Sanitize user input by stripping `\r` and `\n` characters before saving to `.env`. Always use a lambda replacement in `re.sub` (e.g. `lambda m: ...`) to bypass regex engine backslash evaluation on user input.
