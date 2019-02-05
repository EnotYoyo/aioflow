from aioflow.pipeline import Pipeline

__author__ = "a.lemets"


def test_pipeline_create():
    pipeline = Pipeline("test_name", config={"this": "is", "awesome": "config"})

    assert pipeline.name == "test_name"
    assert pipeline.config == {"this": "is", "awesome": "config"}
    assert pipeline._callback is None
    assert pipeline._on_message is None

