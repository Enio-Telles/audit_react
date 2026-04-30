## 2024-05-24 - Overly Permissive CORS Configuration
**Vulnerability:** The CORS configuration in `backend/main.py` used insecure wildcards (`*`) for allowed methods and headers, and hardcoded `localhost` origins, increasing the risk of CSRF and unauthorized cross-origin requests.
**Learning:** Default FastAPI examples often use `allow_methods=["*"]` and `allow_headers=["*"]` for convenience during development, which can accidentally leak into production.
**Prevention:** Always explicitly list allowed HTTP methods (e.g., `['GET', 'POST']`) and allowed headers (e.g., `['Content-Type', 'Authorization']`). Fetch allowed origins dynamically from environment variables rather than hardcoding them.

## 2024-05-25 - Arbitrary File Write via Unvalidated Output Directory
**Vulnerability:** The `output_dir` provided by the user in `fisconforme.py` was used directly with `Path(output_dir).expanduser()` to save files, allowing arbitrary file writes anywhere on the system.
**Learning:** Internal tool features like "save to custom directory" must be confined to a safe base directory when implemented in a web backend to prevent path traversal and arbitrary writes.
**Prevention:** Always validate user-provided paths by checking for `..`, resolving them safely against a predefined base directory, and enforcing `.is_relative_to(safe_base)`.

## 2024-05-20 - Fix CRLF Injection in .env Update
**Vulnerability:** User-supplied inputs written to the `.env` file were not sanitized for newline characters (`\n`, `\r`), allowing potential CRLF/environment variable injection. Additionally, backslashes were erroneously interpreted as escape sequences by `re.sub`.
**Learning:** Configuration updates using `re.sub` directly on file content strings are vulnerable to injection and broken substitution.
**Prevention:** Always sanitize user input intended for `.env` files by removing newlines. Use a lambda function for the replacement argument in `re.sub` to treat the input as a raw literal string.
