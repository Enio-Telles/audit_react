# PLANO DE LIMPEZA DO CÓDIGO — Fiscal Parquet Analyzer

> **Data:** 8 de abril de 2026  
> **Objetivo:** Eliminar código redundante, obsoleto e não utilizado, mantendo a integridade do projeto.  
> **Princípio:** Medir duas vezes, cortar uma vez. Cada remoção deve ser verificável e reversível via git.

---

## Resumo Executivo

| Categoria | Qtd. Arquivos | Risco | Ação |
|---|---|---|---|
| Artefatos temporários/debug | ~16 | Zero | Apagar imediatamente |
| Diretórios temporários de teste | ~6 | Zero | Apagar conteúdo |
| Módulos ETL órfãos | ~5 | Zero | Apagar |
| Utilitários não utilizados | ~4 | Zero | Apagar |
| Duplicatas numeradas (01_, 02_, etc.) | ~8 | Baixo | Apagar versões antigas |
| Testes quebrados | ~8 | Baixo | Corrigir imports ou apagar |
| Módulos proxy de compatibilidade | ~17 | Médio | Marcar deprecated, migrar depois |
| Módulos pendentes de decisão | ~4 | Médio | Decidir manter ou remover |

**Total potencial:** ~68 arquivos tocados, ~50 removíveis.

---

## FASE 1 — Artefatos Temporários / Debug / Log

**Risco:** Zero — nenhum arquivo é importado ou referenciado por código ativo.

| Arquivo | Justificativa |
|---|---|
| `_tmp_34556_1051e7ae26725f46a652b2ddfca2ccb3` | Arquivo temporário vazio (0 bytes) |
| `clean_log.txt` | Log de extração com encoding corrompido |
| `extract_log.txt` | Log de extração Oracle duplicado |
| `extract_log2.txt` | Cópia idêntica de extract_log.txt |
| `cols_mov.txt` | Array JSON de colunas — debug |
| `schema_out.txt` | Schema de saída de teste antigo |
| `sitafe_cols.txt` | Lista de colunas Sitafe — debug |
| `sitafe_cols.json` | Mesma lista em JSON — redundante |
| `traceback.txt` | Traceback de erro antigo (caminho de outra máquina) |
| `map_estoque.json` | Gerado por convert_map.py — sem uso |
| `analise_estoque_2021.py` | Script one-off de análise — não importado |
| `convert_map.py` | Gera map_estoque.json — usa Pandas, one-off |
| `read_excel.py` | Debug: imprime 20 linhas de Excel |
| `update_excel.py` | Script one-off de manutenção de Excel |
| `test_c170.py` | Teste manual ad-hoc de conexão Oracle |

**Comando:**
```bash
git rm _tmp_34556_1051e7ae26725f46a652b2ddfca2ccb3 \
       clean_log.txt extract_log.txt extract_log2.txt \
       cols_mov.txt schema_out.txt sitafe_cols.txt sitafe_cols.json \
       traceback.txt map_estoque.json \
       analise_estoque_2021.py convert_map.py read_excel.py update_excel.py test_c170.py
```

---

## FASE 2 — Módulos ETL Órfãos

**Risco:** Zero — não registrados no orquestrador, não importados por módulos ativos.

| Arquivo | Justificativa |
|---|---|
| `src/transformacao/produtos.py` | Substituído por `produtos_final`. Não está no registry. |
| `src/transformacao/produtos_itens.py` | Não está no registry. Referenciado apenas como string em lista de limpeza. |
| `src/transformacao/fix_fontes.py` | Zero imports apontando para ele. Script de correção one-off. |
| `src/interface_grafica/ui/fix_menus.py` | Script one-off de modificação de main_window.py. Não importado. |

**Comando:**
```bash
git rm src/transformacao/produtos.py \
       src/transformacao/produtos_itens.py \
       src/transformacao/fix_fontes.py \
       src/interface_grafica/ui/fix_menus.py
```

---

## FASE 3 — Utilitários Não Utilizados

**Risco:** Zero — nenhum import ativo referenciando estes módulos.

| Arquivo | Justificativa |
|---|---|
| `src/utilitarios/aux_calc_mva_ajustado.py` | Zero imports |
| `src/utilitarios/aux_st.py` | Zero imports |
| `src/utilitarios/normalizar_parquet.py` | Zero imports |
| `src/utilitarios/exportar_excel.py` | Zero imports (substituído por `exportar_excel_adaptado.py`) |

**Comando:**
```bash
git rm src/utilitarios/aux_calc_mva_ajustado.py \
       src/utilitarios/aux_st.py \
       src/utilitarios/normalizar_parquet.py \
       src/utilitarios/exportar_excel.py
```

---

## FASE 4 — Consolidar Duplicatas Numeradas

**Risco:** Baixo — existem versões antigas na raiz de `transformacao/` e versões canônicas nos subdiretórios `_pkg/`.

### Duplicatas identificadas

| Versão antiga (apagar) | Versão canônica (manter) |
|---|---|
| `src/transformacao/01_item_unidades.py` | `src/transformacao/tabelas_base/01_item_unidades.py` |
| `src/transformacao/02_itens.py` | `src/transformacao/tabelas_base/02_itens.py` |
| `src/transformacao/03_descricao_produtos.py` | `src/transformacao/rastreabilidade_produtos/03_descricao_produtos.py` |
| `src/transformacao/04_produtos_final.py` | `src/transformacao/rastreabilidade_produtos/04_produtos_final.py` |

**Proxies de compatibilidade (manter por enquanto)** — Os arquivos a seguir na raiz de `transformacao/` são apenas re-exports dos módulos reais. Devem ser mantidos temporariamente para retrocompatibilidade, mas marcados como `@deprecated`:

| Proxy | Módulo real |
|---|---|
| `src/transformacao/item_unidades.py` | `src/transformacao/tabelas_base/item_unidades.py` |
| `src/transformacao/itens.py` | `src/transformacao/tabelas_base/itens.py` |
| `src/transformacao/descricao_produtos.py` | `src/transformacao/rastreabilidade_produtos/descricao_produtos.py` |
| `src/transformacao/produtos_final_v2.py` | `src/transformacao/rastreabilidade_produtos/produtos_final_v2.py` |
| `src/transformacao/produtos_agrupados.py` | `src/transformacao/rastreabilidade_produtos/produtos_agrupados.py` |
| `src/transformacao/id_agrupados.py` | `src/transformacao/rastreabilidade_produtos/id_agrupados.py` |
| `src/transformacao/fontes_produtos.py` | `src/transformacao/rastreabilidade_produtos/fontes_produtos.py` |
| `src/transformacao/fatores_conversao.py` | `src/transformacao/rastreabilidade_produtos/fatores_conversao.py` |
| `src/transformacao/precos_medios_produtos_final.py` | `src/transformacao/rastreabilidade_produtos/precos_medios_produtos_final.py` |
| `src/transformacao/c170_xml.py` | `src/transformacao/movimentacao_estoque_pkg/c170_xml.py` |
| `src/transformacao/c176_xml.py` | `src/transformacao/movimentacao_estoque_pkg/c176_xml.py` |
| `src/transformacao/co_sefin.py` | `src/transformacao/movimentacao_estoque_pkg/co_sefin.py` |
| `src/transformacao/co_sefin_class.py` | `src/transformacao/movimentacao_estoque_pkg/co_sefin_class.py` |
| `src/transformacao/movimentacao_estoque.py` | `src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py` |
| `src/transformacao/calculos_mensais.py` | `src/transformacao/calculos_mensais_pkg/calculos_mensais.py` |
| `src/transformacao/calculos_anuais.py` | `src/transformacao/calculos_anuais_pkg/calculos_anuais.py` |
| `src/transformacao/enriquecimento_fontes.py` | `src/transformacao/tabelas_base/enriquecimento_fontes.py` |

**Comando (apagar duplicatas numeradas):**
```bash
git rm src/transformacao/01_item_unidades.py \
       src/transformacao/02_itens.py \
       src/transformacao/03_descricao_produtos.py \
       src/transformacao/04_produtos_final.py
```

**Pós-limpeza (futuro):** Atualizar imports em `orquestrador_pipeline.py` e `__init__.py` para apontar diretamente para os módulos `_pkg/`, depois remover os 17 proxies.

---

## FASE 5 — Testes Quebrados

**Risco:** Baixo — testes já estão quebrados (não passam).

### 5.1 Testes com imports de `funcoes_auxiliares` (pacote inexistente)

| Teste | Problema | Ação recomendada |
|---|---|---|
| `tests/test_encontrar_arquivo_cnpj.py` | `from funcoes_auxiliares.encontrar_arquivo_cnpj` | Reescrever import para `src.utilitarios.encontrar_arquivo_cnpj` |
| `tests/test_extrair_parametros.py` | `from funcoes_auxiliares.extrair_parametros` | Reescrever ou apagar |
| `tests/test_exportar_excel.py` | `from funcoes_auxiliares.exportar_excel` | Apagar (módulo removido na Fase 3) |
| `tests/test_aux_classif_merc.py` | `from funcoes_auxiliares.aux_classif_merc` | Reescrever ou apagar |
| `tests/test_ler_sql.py` | `from funcoes_auxiliares.ler_sql` | Reescrever import para `src.utilitarios.ler_sql` |

### 5.2 Testes com imports de estrutura antiga

| Teste | Problema | Ação recomendada |
|---|---|---|
| `tests/funcoes_tabelas/test_fator_conversao.py` | `sys.path.insert(0, "funcoes_auxiliares")` | Reescrever ou apagar diretório inteiro |
| `tests/funcoes_tabelas/tabela_produtos/test_co_sefin.py` | `from funcoes_tabelas.tabela_produtos.co_sefin` | Reescrever ou apagar diretório inteiro |
| `tests/test_pipeline_efd_atomizacao_registro.py` | Espera `efd_atomizacao` no orquestrador (não está) | Corrigir ou apagar |

**Ação recomendada:**
- Apagar `tests/funcoes_tabelas/` inteiro (estrutura antiga)
- Corrigir imports dos testes avulsos que tiverem módulos correspondentes
- Apagar testes cujos módulos foram removidos

---

## FASE 6 — Limpar Diretórios Temporários de Teste

**Risco:** Zero — são caches e diretórios efêmeros.

| Diretório | Conteúdo |
|---|---|
| `_tmp_testes/` | Subdiretórios com hashes UUID (caches pytest) |
| `tmp7kiiu53u/` | Diretório temporário pytest |
| `tmpzrbx3w_v/` | Diretório temporário pytest |
| `validacao_tmp/` | Validação temporária |
| `.pytest_tmp_local/` | Cache local pytest |
| `tmp_pytest_run/` | Diretório temporário pytest |
| `.validacao_atomizacao/` | Cache de validação |
| `.validacao_extracao/` | Cache de validação |
| `.validacao_extracao_dedup/` | Cache de validação |

**Comando:**
```bash
git rm -r _tmp_testes/ tmp7kiiu53u/ tmpzrbx3w_v/ validacao_tmp/ .pytest_tmp_local/ tmp_pytest_run/ .validacao_atomizacao/ .validacao_extracao/ .validacao_extracao_dedup/
```

**Recomendação:** Adicionar ao `.gitignore`:
```
_tmp_*/
tmp*/
_tmp_testes/
.pytest_tmp_*/
validacao_tmp/
.validacao_*/
```

---

## FASE 7 — Decisões Pendentes

### 7.1 `src/transformacao/produtos_unidades.py`

- **Status:** Não está no registry do orquestrador.
- **Dependência:** `produtos_agrupados.py` lê o arquivo `produtos_unidades_{cnpj}.parquet` como base.
- **Análise:** A tabela base agora é `item_unidades`. Verificar se o parquet de saída ainda é gerado por outro módulo.
- **Ação:** Se `item_unidades` cobre o mesmo schema, remover a dependência em `produtos_agrupados.py` e apagar este módulo.

### 7.2 `src/transformacao/efd_atomizacao.py` + `atomizacao_pkg/`

- **Status:** Não está no registry do orquestrador.
- **UI:** Registrado no catálogo da PySide6 (`pipeline_funcoes_service.py`).
- **Teste:** `test_pipeline_efd_atomizacao_registro.py` espera que esteja no orquestrador.
- **Decisão:**
  - **Opção A:** Registrar no orquestrador e manter a feature.
  - **Opção B:** Remover do catálogo da UI, apagar módulos + testes associados.

### 7.3 `frontend/src/components/tabs/RessarcimentoTab.tsx`

- **Status:** Não importado em `App.tsx`.
- **Backend:** Router `ressarcimento.py` ainda ativo.
- **Decisão:**
  - **Opção A:** Re-adicionar a aba ao App.tsx.
  - **Opção B:** Apagar o componente.

### 7.4 `src/transformacao/__init__.py` — Re-exports de compatibilidade

- Contém re-exports para retrocompatibilidade com os proxies.
- **Ação futura:** Simplificar quando os 17 proxies forem removidos.

---

## Cronograma Sugerido

| Fase | Arquivos | Verificação Pós-Limpeza |
|---|---|---|
| **Fase 1** | ~15 | `git status` — nenhum módulo afetado |
| **Fase 2** | ~4 | `PYTHONPATH=src python -c "import transformacao"` — sem erros |
| **Fase 3** | ~4 | `grep -r "aux_calc\|aux_st\|normalizar_parquet\|exportar_excel[^_]" src/` — zero resultados |
| **Fase 4** | ~4 | `PYTHONPATH=src python -m pytest tests/` — testes existentes passam |
| **Fase 5** | ~8 | `PYTHONPATH=src python -m pytest tests/` — testes corrigidos passam |
| **Fase 6** | ~9 diretórios | `git status` — nenhum arquivo de código afetado |
| **Fase 7** | TBD | Decisão manual |

---

## Regras de Segurança

1. **Cada fase é um commit separado.** Nunca misturar fases.
2. **Rodar testes após cada fase.** Se algo quebrar, reverter o commit.
3. **Nunca apagar sem verificar imports.** Usar `grep -r "from modulo" src/` antes.
4. **Backup via git.** Tudo é recuperável via `git revert`.

---

## Checklist de Verificação Final

Após todas as fases:

```bash
# 1. Testes pytest
PYTHONPATH=src python -m pytest tests/ -v

# 2. Import do pacote principal
PYTHONPATH=src python -c "from transformacao import *"

# 3. Verificar orquestrador
PYTHONPATH=src python -c "from orquestrador_pipeline import REGISTO_TABELAS; print(list(REGISTO_TABELAS.keys()))"

# 4. TypeScript check
cd frontend && pnpm exec tsc --noEmit && pnpm lint

# 5. Backend check
cd backend && python -c "from main import app; print(app.title)"
```
