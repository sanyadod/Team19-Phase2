# acmecli/baseline/mockdb.py

MOCK_ITEMS = [
    {"id": 1, "filename": "bert-base-uncased", "artifact_type": "model"},
    {"id": 2, "filename": "gpt-small", "artifact_type": "model"},
    {"id": 3, "filename": "bookcorpus", "artifact_type": "dataset"},
]

def scan_all_items():
    return MOCK_ITEMS.copy()
