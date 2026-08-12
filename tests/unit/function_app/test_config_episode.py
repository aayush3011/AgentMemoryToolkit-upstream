from __future__ import annotations

import pytest
from shared import config


@pytest.mark.parametrize(
    ("env_name", "getter_name", "expected"),
    [
        ("EPISODE_EVAL_EVERY_N", "get_episode_eval_every_n", 4),
        ("EPISODE_IDLE_GAP_SECONDS", "get_episode_idle_gap_seconds", 1800),
        ("EPISODE_TOPIC_DRIFT", "get_episode_topic_drift", 0.0),
        ("EPISODE_MAX_TURNS", "get_episode_max_turns", 40),
        ("EPISODE_MIN_TURNS", "get_episode_min_turns", 2),
    ],
)
def test_episode_config_getters_defaults(
    monkeypatch: pytest.MonkeyPatch,
    env_name: str,
    getter_name: str,
    expected: int | float,
) -> None:
    monkeypatch.delenv(env_name, raising=False)

    assert getattr(config, getter_name)() == expected


@pytest.mark.parametrize(
    ("env_name", "getter_name", "raw", "expected"),
    [
        ("EPISODE_EVAL_EVERY_N", "get_episode_eval_every_n", "6", 6),
        ("EPISODE_IDLE_GAP_SECONDS", "get_episode_idle_gap_seconds", "2400", 2400),
        ("EPISODE_TOPIC_DRIFT", "get_episode_topic_drift", "0.35", 0.35),
        ("EPISODE_MAX_TURNS", "get_episode_max_turns", "50", 50),
        ("EPISODE_MIN_TURNS", "get_episode_min_turns", "3", 3),
    ],
)
def test_episode_config_getters_parse_env(
    monkeypatch: pytest.MonkeyPatch,
    env_name: str,
    getter_name: str,
    raw: str,
    expected: int | float,
) -> None:
    monkeypatch.setenv(env_name, raw)

    assert getattr(config, getter_name)() == expected
