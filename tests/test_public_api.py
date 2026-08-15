import domofon_letai


def test_version_and_public_exports() -> None:
    assert domofon_letai.__version__ == "0.2.0"
    assert domofon_letai.DomofonLetaiClient
    assert domofon_letai.IncomingCallListener
    assert domofon_letai.FileFcmCredentialStore
    assert domofon_letai.StreamFormat.MPEG_TS.value == "mpeg_ts"
