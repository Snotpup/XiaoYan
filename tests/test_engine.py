from xiaoyan.core.engine import XiaoYanEngine


def test_duplicate_message_detector_remembers_recent_ids():
    engine = XiaoYanEngine()

    assert engine._is_duplicate_message("om_test") is False
    assert engine._is_duplicate_message("om_test") is True
    assert engine._is_duplicate_message("om_other") is False
