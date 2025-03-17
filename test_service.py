import os

import pytest
from fastapi.testclient import TestClient

from recommendations_service import Config, app

_COLD_USER_ID = 83
_COMMON_USER_ID = 4
_NOONLINE_USER_ID = 1374572

_CLIENT = TestClient(app)

@pytest.fixture(scope="package")
def upload_store():
    _CLIENT.post("/update_store")
    assert Config.database.exists()
    yield
    os.unlink(Config.database)

def test_cold_user_recommendations(upload_store):
    resp = _CLIENT.get("/recommendations", params={"user_id": _COLD_USER_ID, "k": 5})
    assert resp.status_code == 200
    assert resp.json() == [
        {'track_id': 75638259, 'track': 'Hayatım', 'score': 0.9932301044464111},
        {'track_id': 75113899, 'track': 'Mey Mənəm', 'score': 0.9910985827445984},
        {'track_id': 67944495, 'track': 'Yanlışımsan', 'score': 0.9905444979667664},
        {'track_id': 78555092, 'track': 'Təcili Yardım', 'score': 0.9901193380355835},
        {'track_id': 79356346, 'track': 'Yaşamalı', 'score': 0.9886537194252014},
    ]

def test_common_user_recommendations(upload_store):
    resp = _CLIENT.get("/recommendations", params={"user_id": _COMMON_USER_ID, "k": 5})
    assert resp.status_code == 200
    assert resp.json() == [
        {'track_id': 83375864, 'track': 'Bad Decisions', 'score': 0.9774048924446106},
        {'track_id': 78305163, 'track': 'Кровь', 'score': 0.9768791198730469},
        {'track_id': 78305171, 'track': 'Удочка для охоты', 'score': 0.9728075861930847},
        {'track_id': 89811429, 'track': 'DANCE-OFF', 'score': 0.9715034365653992},
        {'track_id': 78305168, 'track': 'Фауст', 'score': 0.9686200618743896},
    ]

def test_no_inline_history_user_recommendations(upload_store):
    resp = _CLIENT.get("/recommendations", params={"user_id": _NOONLINE_USER_ID, "k": 5})
    assert resp.status_code == 200
    assert resp.json() == [
        {'track_id': 57185718, 'track': 'Молчи', 'score': 0.23754716631625744},
        {'track_id': 50400505, 'track': 'Девочка с каре', 'score': 0.23598502810992045},
        {'track_id': 40828285, 'track': 'Blood // Water', 'score': 0.23149700376960053},
        {'track_id': 58515002, 'track': 'Дора дура', 'score': 0.22893907047393233},
        {'track_id': 47627256, 'track': 'Cradles', 'score': 0.2252903148125851},
    ]
