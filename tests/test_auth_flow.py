import json

from xiaoyan.scraper import auth_flow


def test_auth_start_url_respects_platform_and_mode():
    assert auth_flow.auth_start_url("cnki", "carsi") == "https://fsso.cnki.net/"
    assert auth_flow.auth_start_url("wos", "carsi") == "https://webofscience.com/UA"
    assert "AdvSearch" in auth_flow.auth_start_url("cnki", "ip")
    assert auth_flow.auth_start_url("wos", "ip").startswith("https://www.webofscience.com/")
    assert "advanced-search" in auth_flow.auth_start_url("wos", "ip")


def test_write_auth_status_uses_configured_data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(auth_flow, "DATA_DIR", tmp_path)

    path = auth_flow.write_auth_status(
        "wos",
        "carsi",
        institution="测试大学",
        verified=True,
        note="ok",
    )

    assert path == tmp_path / "auth_status.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["wos"]["mode"] == "carsi"
    assert data["wos"]["institution"] == "测试大学"
    assert data["wos"]["verified"] is True
