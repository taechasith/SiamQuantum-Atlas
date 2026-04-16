from __future__ import annotations

from siamquantum_atlas.nlp.thai_preprocess import tokenize_thai_mixed


def extract_entities(text: str) -> list[str]:
    markers = {"ไทย", "ประเทศไทย", "quantum", "ควอนตัม", "คิวบิต", "ฟิสิกส์"}
    return sorted({token for token in tokenize_thai_mixed(text) if token in markers})
