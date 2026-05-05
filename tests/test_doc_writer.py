from xiaoyan.lark.doc_writer import _normalize_create_doc_response


def test_normalize_create_doc_response_reads_nested_doc_url():
    payload = {
        "ok": True,
        "data": {
            "doc_id": "doc-token",
            "doc_url": "https://www.feishu.cn/wiki/test",
        },
    }

    normalized = _normalize_create_doc_response(payload)

    assert normalized["url"] == "https://www.feishu.cn/wiki/test"
    assert normalized["doc_token"] == "doc-token"
