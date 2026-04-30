## 2024-05-24 - Overly Permissive CORS Configuration
**Vulnerability:** The CORS configuration in `backend/main.py` used insecure wildcards (`*`) for allowed methods and headers, and hardcoded `localhost` origins, increasing the risk of CSRF and unauthorized cross-origin requests.
**Learning:** Default FastAPI examples often use `allow_methods=["*"]` and `allow_headers=["*"]` for convenience during development, which can accidentally leak into production.
**Prevention:** Always explicitly list allowed HTTP methods (e.g., `['GET', 'POST']`) and allowed headers (e.g., `['Content-Type', 'Authorization']`). Fetch allowed origins dynamically from environment variables rather than hardcoding them.

## 2024-05-25 - Arbitrary File Write via Unvalidated Output Directory
**Vulnerability:** The `output_dir` provided by the user in `fisconforme.py` was used directly with `Path(output_dir).expanduser()` to save files, allowing arbitrary file writes anywhere on the system.
**Learning:** Internal tool features like "save to custom directory" must be confined to a safe base directory when implemented in a web backend to prevent path traversal and arbitrary writes.
**Prevention:** Always validate user-provided paths by checking for `..`, resolving them safely against a predefined base directory, and enforcing `.is_relative_to(safe_base)`.

## 2024-05-01 - CRLF Environment Variable Injection in .env Updates
**Vulnerability:** The application dynamically updates its root `.env` configuration file at runtime with user-supplied database credentials without sanitizing newline characters (`\n`, `\r`). This allowed CRLF injection to inject arbitrary environment variables. Also, `re.sub` interpreted backslashes in user input as escape sequences.
**Learning:** Runtime updates to configuration files like `.env` must strictly sanitize inputs, as newline characters allow appending arbitrary key-value pairs that are evaluated by the application environment.
**Prevention:** Always sanitize inputs meant for `.env` files by explicitly removing `\n` and `\r`. When using `re.sub` for string replacement, always use a `lambda` function (e.g., `lambda m: f"{key}={val}"`) instead of a raw replacement string to safely handle backslashes in user input.
