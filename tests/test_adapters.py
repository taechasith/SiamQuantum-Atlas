from siamquantum_atlas.adapters.gdelt import GDELTAdapter
from siamquantum_atlas.adapters.youtube import YouTubeAdapter
from siamquantum_atlas.settings import settings


def test_sample_adapters_load_records() -> None:
    sample_path = settings.samples_dir / "thai_quantum_media.json"
    gdelt_records = GDELTAdapter().fetch(sample_path=sample_path)
    youtube_records = YouTubeAdapter().fetch(sample_path=sample_path)
    assert gdelt_records
    assert youtube_records
    assert all(record.platform == "gdelt_news" for record in gdelt_records)
    assert all(record.platform == "youtube" for record in youtube_records)
