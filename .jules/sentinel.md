## 2024-05-24 - Masking Passwords in Config APIs
**Vulnerability:** Cleartext passwords from .env file were exposed through /api/oracle/config GET endpoint to pre-fill frontend UI.
**Learning:** Sending cleartext passwords to frontend configuration UIs creates a major credential exposure risk, even for local tools.
**Prevention:** Mask passwords with a dummy string (e.g. "********") when serving them to the client, and explicitly ignore this mask string when the frontend submits updates to save, preserving the original password securely on the server.
## 2026-04-08 - [SQL Injection Prevention in Dynamic Identifiers]
**Vulnerability:** SQL Injection via string concatenation of table/schema/column names in dynamic queries.
**Learning:** Even when using bind variables for values, concatenating unvalidated identifiers (like table names) creates a SQL injection vector if those identifiers come from user input.
**Prevention:** Use strict regex validation (e.g. ^[A-Za-z0-9_]+$) on all database identifiers before concatenating them into SQL queries.
