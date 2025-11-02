from acmecli.metrics.hf_api import estimate_dataset_presence

def test_linked_datasets_detection():
    m1 = {"cardData": {"text": "Trained on ImageNet dataset."}}
    m2 = {"cardData": {"text": "Uses https://huggingface.co/datasets/squad"}}
    m3 = {"cardData": {"text": "Transformer model for text generation"}}

    assert estimate_dataset_presence(m1)
    assert estimate_dataset_presence(m2)
    assert not estimate_dataset_presence(m3)
