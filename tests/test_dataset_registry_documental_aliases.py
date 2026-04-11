from utilitarios import dataset_registry as registry


def test_aliases_documentais_normalizam_para_canonicos():
    assert registry.normalizar_dataset_id("cte") == "cte_base"
    assert registry.normalizar_dataset_id("info_complementar") == "docs_info_complementar"
    assert registry.normalizar_dataset_id("email_nfe") == "docs_contatos"


def test_definicoes_documentais_existem_no_catalogo():
    assert registry.obter_definicao("cte_base") is not None
    assert registry.obter_definicao("docs_info_complementar") is not None
    assert registry.obter_definicao("docs_contatos") is not None
