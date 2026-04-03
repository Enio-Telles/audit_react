## 2024-05-18 - [Path Traversal in Parquet File Reading]
**Vulnerability:** A path traversal vulnerability existed in the `/api/tabelas/{cnpj}/{nome_tabela}` endpoint where `nome_tabela` was used to directly construct a file path for `.parquet` files using `pathlib.Path`. While `pathlib` concatenates strings, calling `.resolve()` would allow resolving out to external files (e.g., `../../../etc/passwd.parquet` if that file existed).
**Learning:** Even though `nome_tabela` had an implicit format assumption, FastApi path parameters allow url encoded slash traversal strings if not properly checked. In `pathlib`, simply joining `BASE_DIR / ... / f"{user_input}.parquet"` does not prevent path traversal if the user input contains `../`.
**Prevention:** Always validate that the dynamically generated file path, after resolution, remains within the expected base directory. You can enforce this in python using `try: resolved_path.relative_to(base_dir.resolve()) except ValueError: raise InvalidPath()`.

## 2025-02-27 - Insecure CORS Configuration
**Vulnerability:** Overly permissive CORS configuration (`allow_origins=["*"]`) combined with `allow_credentials=True`.
**Learning:** This combination allows any domain to make cross-origin requests with credentials, which is insecure and invalid under strict browser specifications. It was likely left open during early development.
**Prevention:** Always restrict CORS origins to trusted domains, ideally by reading them from an environment variable (e.g. `CORS_ORIGINS`) with safe localhost defaults for development. Ensure `allow_origins` does not use the wildcard `*` when `allow_credentials` is set to `True`.

## $(date +%Y-%m-%d) - [Fix Path Traversal in File Upload]
**Vulnerability:** Path traversal via unsanitized `UploadFile.filename` in FastAPI file upload (`upload_det_cnpj` endpoint). User input was concatenated directly to the base path (`diretorio / arquivo.filename`).
**Learning:** Even internal or admin-focused applications can suffer from critical remote code execution / arbitrary file write vulnerabilities if the standard web framework objects (like FastAPI's `UploadFile`) have their properties passed blindly to filesystem operations.
**Prevention:** Always use `Path(user_filename).name` to extract just the base filename, stripping any directory traversal elements (`../`). As a defense in depth measure, verify that `resolved_destination_path.is_relative_to(resolved_base_dir)` before performing the I/O.

## $(date +%Y-%m-%d) - [Fix Path Traversal in SPA Fallback Handler]
**Vulnerability:** A path traversal vulnerability existed in the `/api.py` `spa_fallback` handler which serves static assets. By using a path starting with `assets/` followed by `../`, an attacker could escape the `BUILD_DIR` and read arbitrary files on the filesystem because `pathlib.Path` concatenates the path without resolving it by default.
**Learning:** FastApi path parameters (`/{path:path}`) capture the entire URL path exactly as is, allowing path traversal (`../`) strings. Simply joining paths using `BUILD_DIR / user_input` does not prevent path traversal if the user input contains `../`.
**Prevention:** Always validate that the dynamically generated file path, after resolution, remains within the expected base directory. Use `try: if not resolved_path.is_relative_to(base_dir.resolve()): raise Exception() except ValueError: raise InvalidPath()` or similar logic.
