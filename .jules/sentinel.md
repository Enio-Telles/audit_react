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

## 2025-02-27 - [Fix Path Traversal in Static Asset Handler]
**Vulnerability:** A path traversal vulnerability existed in the `spa_fallback` handler where the static asset path was checked using `startswith('assets/')` but not validated securely after joining with `BUILD_DIR`, allowing access to files outside the intended directory.
**Learning:** Checking a path prefix using string operations is insufficient because `../` traversal elements can bypass these checks when passed to file system operations. FastApi/Starlette's string path parameters allow traversal.
**Prevention:** Always sanitize dynamically constructed paths. Wrap path operations in a `try...except` block, resolve the constructed path using `.resolve()`, and explicitly enforce bounds using `.is_relative_to(base_dir.resolve())` before file access.
