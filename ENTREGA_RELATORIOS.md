# Relatório de Análise de Fisconforme Não Cumprido

## Entrega

Foram gerados **6 Relatórios Fiscais Conclusivos** em formato PDF profissional, cada um unido à respectiva Notificação DET original (preservada intacta).

## Estrutura de Armazenamento

### Dados do Auditor (centralizado)

Os dados do auditor ficam armazenados em local único e reutilizável para todas as futuras gerações de relatório.

| Campo | Valor |
|-------|-------|
| Arquivo | `storage/_config/auditor.json` |
| Nome | ENIO CARSTENS TELLES |
| Cargo | Auditor Fiscal de Tributos Estaduais |
| Matrícula | 300201625 |
| Órgão | SEFIN/CRE/GEFIS - Gerência de Fiscalização |

### Dados por CNPJ (em pastas individuais)

Cada CNPJ possui sua própria pasta com os dados e o relatório final gerado.

| CNPJ | Contribuinte | Pasta |
|------|-------------|-------|
| 19288989000290 | M C Industria e Comercio de Papeis LTDA | `storage/CNPJ/19288989000290/relatorio/` |
| 21418376000190 | PRIM Industria e Comercio de Artefatos de Cimento LTDA | `storage/CNPJ/21418376000190/relatorio/` |
| 22397093000172 | Portogases Comercio e Distribuicao de Gases LTDA | `storage/CNPJ/22397093000172/relatorio/` |
| 23722199000167 | Carlos Gomes Consultoria e Servicos Geologicos e Ambientais LTDA | `storage/CNPJ/23722199000167/relatorio/` |
| 32846441000103 | Megafortt Comercio de Alimentos LTDA | `storage/CNPJ/32846441000103/relatorio/` |
| 46675389000176 | Cooffer Comercio de Alimentos LTDA | `storage/CNPJ/46675389000176/relatorio/` |

Cada pasta `relatorio/` contém:

| Arquivo | Descrição |
|---------|-----------|
| `dados.json` | Dados estruturados do contribuinte (CNPJ, IE, DSF, notificação, manifestação, contatos, decisão, desfecho) |
| `Relatorio_Final_*.pdf` | PDF final com relatório fiscal conclusivo + notificação DET anexada |

## Modelo do Relatório PDF

O relatório segue um layout profissional com as seguintes seções:

| Seção | Conteúdo |
|-------|----------|
| Cabeçalho | Governo do Estado de Rondônia / Secretaria de Finanças / CRE |
| Dados do Contribuinte | Nome, CNPJ, IE, DSF, Notificação DET |
| 1. Identificação da Ação Fiscal | Contexto da DSF e Fisconforme Não Atendido |
| 2. Procedimentos Realizados | Lista de providências adotadas |
| 3. Manifestação do Contribuinte | Checklist + detalhamento dos contatos |
| 4. Análise Fiscal | Princípios e decisão (destaque em amarelo) |
| 5. Situação Final das Pendências | Desfecho (destaque em vermelho) |
| 6. Conclusão | Encerramento formal |
| Assinatura | Nome, cargo, matrícula do auditor |
| Anexo | Notificação DET original (PDF intacto) |

## Scripts

Os scripts estão em `server/python/`:

| Script | Função |
|--------|--------|
| `setup_data.py` | Cria a estrutura de pastas e popula os dados do auditor e dos CNPJs |
| `gerar_relatorios.py` | Gera os PDFs por CNPJ e une com as notificações DET |

### Dependências

```
pip install weasyprint pypdf
```

### Execução

```bash
cd server/python
python gerar_relatorios.py
```

## Integração com audit_react

Os caminhos seguem a mesma convenção do projeto (`storage/CNPJ/{cnpj}/`), permitindo futura integração com a API FastAPI para geração de relatórios diretamente pela interface web. Os dados do auditor em `storage/_config/auditor.json` podem ser editados pela interface de configurações do sistema.
