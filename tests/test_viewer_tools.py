from siamquantum_atlas.utils.viewer_tools import viewer_url


def test_viewer_url_shape() -> None:
    assert viewer_url(8765).endswith("/viewer/index.html")
