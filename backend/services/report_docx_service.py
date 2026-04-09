from pathlib import Path

try:
    from docxtpl import DocxTemplate
except ImportError:  # pragma: no cover - depende do ambiente de execucao
    DocxTemplate = None


BASE_DIR = Path(__file__).resolve().parents[2]
TEMPLATE_PATH = BASE_DIR / "modelo" / "Template_Relatorio_fisconforme.docx"
OUTPUT_DIR = BASE_DIR / "dados" / "notificacoes"


class ReportDocxService:
    """
    Servico especializado na geracao de relatorios Microsoft Word (.docx).

    A dependencia `docxtpl` e validada apenas quando o endpoint DOCX e usado,
    evitando que a API inteira deixe de subir em ambientes sem esse pacote.
    """

    def __init__(self, template_path: str = str(TEMPLATE_PATH)):
        self.template_path = template_path

        if DocxTemplate is None:
            raise RuntimeError(
                "Dependencia ausente para gerar DOCX: instale `docxtpl` no ambiente atual."
            )

        caminho_template = Path(self.template_path)
        if not caminho_template.exists():
            raise FileNotFoundError(
                f"Erro: O modelo DOCX nao foi localizado em: {self.template_path}"
            )

    def gerar_relatorio(self, dados: dict, output_filename: str) -> str:
        """
        Gera um arquivo .docx preenchido com base no modelo.

        Args:
            dados: Dicionario com os placeholders usados pelo template.
            output_filename: Nome do arquivo de saida.

        Returns:
            Caminho absoluto do arquivo gerado.
        """

        doc = DocxTemplate(self.template_path)
        doc.render(dados)

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_DIR / output_filename
        doc.save(str(output_path))

        return str(output_path)
