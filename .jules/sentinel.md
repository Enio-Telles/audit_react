## 2024-05-18 - [Path Traversal in Parquet File Reading]
**Vulnerability:** A path traversal vulnerability existed in the `/api/tabelas/{cnpj}/{nome_tabela}` endpoint where `nome_tabela` was used to directly construct a file path for `.parquet` files using `pathlib.Path`. While `pathlib` concatenates strings, calling `.resolve()` would allow resolving out to external files (e.g., `../../../etc/passwd.parquet` if that file existed).
**Learning:** Even though `nome_tabela` had an implicit format assumption, FastApi path parameters allow url encoded slash traversal strings if not properly checked. In `pathlib`, simply joining `BASE_DIR / ... / f"{user_input}.parquet"` does not prevent path traversal if the user input contains `../`.
**Prevention:** Always validate that the dynamically generated file path, after resolution, remains within the expected base directory. You can enforce this in python using `try: resolved_path.relative_to(base_dir.resolve()) except ValueError: raise InvalidPath()`.

## 2025-02-27 - Insecure CORS Configuration
**Vulnerability:** Overly permissive CORS configuration (`allow_origins=["*"]`) combined with `allow_credentials=True`.
**Learning:** This combination allows any domain to make cross-origin requests with credentials, which is insecure and invalid under strict browser specifications. It was likely left open during early development.
**Prevention:** Always restrict CORS origins to trusted domains, ideally by reading them from an environment variable (e.g. `CORS_ORIGINS`) with safe localhost defaults for development. Ensure `allow_origins` does not use the wildcard `*` when `allow_credentials` is set to `True`.

## 2024-05-19 - Overly Permissive CORS Headers and Methods
**Vulnerability:** The CORS configuration in `server/python/api.py` was using the wildcard `["*"]` for both `allow_methods` and `allow_headers` and was not fully sanitizing `allow_origins` to prevent the accidental inclusion of a wildcard when using `allow_credentials=True`.
**Learning:** Using wildcards for methods and headers violates the Principle of Least Privilege and can unnecessarily expose the API to risky operations from untrusted contexts. If an environment variable is configured with a trailing space or contains `*`, it could still break the server startup if `allow_credentials=True` or expose it unintentionally.
**Prevention:** Explicitly define the required HTTP methods (e.g., `GET`, `POST`, `PUT`, `DELETE`, `OPTIONS`) and headers (e.g., `Content-Type`, `Authorization`, `Accept`). Additionally, sanitize origin lists dynamically by stripping whitespace and rejecting any origin that equals `*`.
