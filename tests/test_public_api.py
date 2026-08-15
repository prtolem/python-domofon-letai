import domofon_letai


def test_version_and_public_exports() -> None:
    assert domofon_letai.__version__ == "0.1.0"
    assert domofon_letai.DomofonLetaiClient
    assert domofon_letai.StreamFormat.MPEG_TS.value == "mpeg_ts"
