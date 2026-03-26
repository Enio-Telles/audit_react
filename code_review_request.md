Migrated the table structure in the Python backend from `modulos/` to `tabelas/` as instructed in `AGENTS.md`.

- Created the folder structure for the 11 tables (`tabelas/{nome}`).
- Split `contratos/tabelas.py` into separate `contrato.py` files for each table.
- Renamed the remaining classes in `contratos/tabelas.py` to `contratos/base.py`.
- Migrated generator functions from `modulos/` into `tabelas/{nome}/gerador.py`.
- Added missing stubs with Portuguese function names and comments in `produtos_unidades`, `nfe_entrada`, and `mov_estoque`.
- Added `AGENTS.md` context files into each `tabelas/{nome}/` folder.
- Added `__init__.py` file to `tabelas` and imported all tables.
- Updated imports in `api.py`, `orquestrador.py`, and `__init__.py` to use `contratos.base`.
- Deleted the `modulos/` folder.
