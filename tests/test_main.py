from src.pipeline.main import get_workout_records


def test_get_workout_records():
    records = get_workout_records()

    assert len(records) == 3
    assert records[0][2] == "running"
    assert records[0][4] == 250