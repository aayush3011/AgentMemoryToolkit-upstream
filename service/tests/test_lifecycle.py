def test_reconcile_empty_body(api, mock_memory):
    result = {"kept": 3, "merged": 1, "contradicted": 0}
    mock_memory.reconcile.return_value = result

    response = api.post("/users/u1/reconcile", json={})

    assert response.status_code == 200
    assert response.json() == result
    mock_memory.reconcile.assert_awaited_once_with(user_id="u1", n=None)


def test_reconcile_with_n(api, mock_memory):
    result = {"kept": 3, "merged": 1, "contradicted": 0}
    mock_memory.reconcile.return_value = result

    response = api.post("/users/u1/reconcile", json={"n": 25})

    assert response.status_code == 200
    assert response.json() == result
    mock_memory.reconcile.assert_awaited_once_with(user_id="u1", n=25)


def test_reconcile_rejects_invalid_n(api):
    response = api.post("/users/u1/reconcile", json={"n": 0})

    assert response.status_code == 422


def test_reconcile_without_json_body(api, mock_memory):
    result = {"kept": 3, "merged": 1, "contradicted": 0}
    mock_memory.reconcile.return_value = result

    response = api.post("/users/u1/reconcile")

    assert response.status_code == 200
    assert response.json() == result
    mock_memory.reconcile.assert_awaited_once_with(user_id="u1", n=None)
