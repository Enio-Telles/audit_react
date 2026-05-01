## 2024-05-24 - Overly Permissive CORS Configuration
**Vulnerability:** The CORS configuration in `backend/main.py` used insecure wildcards (`*`) for allowed methods and headers, and hardcoded `localhost` origins, increasing the risk of CSRF and unauthorized cross-origin requests.
**Learning:** Default FastAPI examples often use `allow_methods=["*"]` and `allow_headers=["*"]` for convenience during development, which can accidentally leak into production.
**Prevention:** Always explicitly list allowed HTTP methods (e.g., `['GET', 'POST']`) and allowed headers (e.g., `['Content-Type', 'Authorization']`). Fetch allowed origins dynamically from environment variables rather than hardcoding them.

## 2024-05-25 - Arbitrary File Write via Unvalidated Output Directory
**Vulnerability:** The `output_dir` provided by the user in `fisconforme.py` was used directly with `Path(output_dir).expanduser()` to save files, allowing arbitrary file writes anywhere on the system.
**Learning:** Internal tool features like "save to custom directory" must be confined to a safe base directory when implemented in a web backend to prevent path traversal and arbitrary writes.
**Prevention:** Always validate user-provided paths by checking for `..`, resolving them safely against a predefined base directory, and enforcing `.is_relative_to(safe_base)`.

## 2024-05-01 - Prevent CRLF/Environment Variable Injection in .env files
**Vulnerability:** User-supplied strings were directly written to a .env file and interpolated into `re.sub` replacement strings. This allowed CRLF injection to set arbitrary environment variables and caused `re.sub` crashes when backslashes were present in the input.
**Learning:** Writing unsanitized data directly to sensitive configuration files (.env) creates significant risk of environment variable injection. `re.sub` interprets backslashes in replacement strings as escape sequences, breaking on user inputs like domain accounts.
**Prevention:** Always sanitize user inputs destined for configuration files by stripping newline characters. When using `re.sub`, pass a lambda function (`lambda m: ...`) as the replacement to prevent Python from interpreting user input as regex escape sequences.
