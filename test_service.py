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

def test_personal_recommendations(upload_store):
    resp = _CLIENT.get("/recommendations", params={"user_id": 23, "k": 5})
    assert resp.status_code == 200
    assert resp.json() == [
        {
            'score': 1.3827400207519531,
            'track': 'Twist in My Sobriety',
            'track_id': 26312203,
        },
        {
            'score': 1.3056058883666992,
            'track': 'The Night Is Young',
            'track_id': 23464071,
        },
        {
            'score': 1.3046672344207764,
            'track': 'Штрихкоды',
            'track_id': 24279093,
        },
        {
            'score': 1.3034749031066895,
            'track': 'Мама',
            'track_id': 23661034,
        },
        {
            'score': 1.2895008325576782,
            'track': 'Thinking About It (Let It Go)',
            'track_id': 27373110,
        },
    ]
