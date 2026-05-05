from xiaoyan.lark.base_sync import LarkBaseManager


def test_paper_to_fields_uses_exact_base_select_labels():
    manager = LarkBaseManager(base_token="base", table_id="table")

    wos_fields = manager._paper_to_fields({
        "platform": "wos",
        "title": "A paper",
        "journal": "Journal",
        "abstract": "Abstract",
        "data_hash": "hash-wos",
    })
    cnki_fields = manager._paper_to_fields({
        "platform": "cnki",
        "title": "一篇论文",
        "journal": "期刊",
        "abstract": "摘要",
        "data_hash": "hash-cnki",
    })

    assert wos_fields["来源平台"] == "WoS"
    assert cnki_fields["来源平台"] == "CNKI"
