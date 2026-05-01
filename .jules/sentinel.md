## 2024-05-24 - Overly Permissive CORS Configuration
**Vulnerability:** The CORS configuration in `backend/main.py` used insecure wildcards (`*`) for allowed methods and headers, and hardcoded `localhost` origins, increasing the risk of CSRF and unauthorized cross-origin requests.
**Learning:** Default FastAPI examples often use `allow_methods=["*"]` and `allow_headers=["*"]` for convenience during development, which can accidentally leak into production.
**Prevention:** Always explicitly list allowed HTTP methods (e.g., `['GET', 'POST']`) and allowed headers (e.g., `['Content-Type', 'Authorization']`). Fetch allowed origins dynamically from environment variables rather than hardcoding them.

## 2024-05-25 - Arbitrary File Write via Unvalidated Output Directory
**Vulnerability:** The `output_dir` provided by the user in `fisconforme.py` was used directly with `Path(output_dir).expanduser()` to save files, allowing arbitrary file writes anywhere on the system.
**Learning:** Internal tool features like "save to custom directory" must be confined to a safe base directory when implemented in a web backend to prevent path traversal and arbitrary writes.
**Prevention:** Always validate user-provided paths by checking for `..`, resolving them safely against a predefined base directory, and enforcing `.is_relative_to(safe_base)`.
## 2024-05-01 - Prevent CRLF and Regex Injection in Env Config
**Vulnerability:** The `_write_key` function in `backend/routers/oracle.py` wrote user-supplied database passwords directly to the `.env` file without sanitizing newlines, leading to CRLF injection (allowing injection of arbitrary environment variables). Furthermore, it passed the password directly as the replacement string in `re.sub`, which caused it to fail with invalid regex escapes if the password contained backslashes.
**Learning:** `re.sub` parses replacement strings for backreferences (like `\1`) unless a function is provided. Additionally, any function writing user input to `.env` files must explicitly strip newline characters to prevent environment variable injection.
**Prevention:** Always sanitize input by removing `\r` and `\n` before writing to configuration files. Use a lambda function (e.g., `lambda m: replacement`) as the replacement argument in `re.sub` to treat the replacement string as a literal.
