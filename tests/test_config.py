from datetime import date

import pytest

from oms_trace import config


def test_topology_is_five_levels_and_four_edges():
    assert config.LEVELS == ("A_ROOT", "B_PARENT", "B_CHILD", "C_PARENT", "C_CHILD")
    assert [e.name for e in config.EDGES] == ["A-Bp", "Bp-Bc", "Bc-Cp", "Cp-Cc"]
    assert [e.scope for e in config.EDGES] == ["CROSS", "INTERNAL", "CROSS", "INTERNAL"]
    assert [e.expect_one for e in config.EDGES] == [True, False, True, False]
    for level in config.LEVELS[1:]:
        assert config.LEVEL_SYSTEM[config.LEVEL_PARENT[level]] == config.PARENT_SYSTEM[level]


def test_defaults_and_window():
    s = config.load_settings({})
    assert s.gap_age_sec == 30 and s.unresolved_alert_sec == 3600 and s.feed_stale_sec == 120
    s = s.with_(as_of=date(2026, 10, 26), date_window_days=2)
    assert s.dates() == ["2026-10-25", "2026-10-26", "2026-10-27"]  # yesterday, today, tomorrow


def test_env_parsing_and_validation():
    s = config.load_settings({"OMS_TRACE_GAP_AGE_SEC": "45", "OMS_TRACE_AGE_CLOCK": "event",
                              "OMS_TOPICS_B": "oms.b.t1, oms.b.t2", "OMS_TRACE_ONE_TABLE_PER_SYSTEM": "false",
                              "OMS_TRACE_AS_OF": "2026-10-26"})
    assert s.gap_age_sec == 45 and s.age_column() == "EventTs"
    assert s.topics["B"] == ("oms.b.t1", "oms.b.t2")
    assert s.raw_table_names("Orders", "B") == ["OrdersB_t1", "OrdersB_t2"]
    assert s.raw_table_names("Orders", "A") == ["OrdersA"]  # no topics listed -> one table
    assert s.with_(one_table_per_system=True).raw_table_names("Orders", "B") == ["OrdersB"]
    for bad in ({"OMS_TRACE_GAP_AGE_SEC": "x"}, {"OMS_TRACE_AGE_CLOCK": "wall"},
                {"OMS_TRACE_EXPECT_ONE_EDGES": "A-Z"}, {"OMS_TRACE_DATE_WINDOW_DAYS": "0"},
                {"OMS_TRACE_AS_OF": "26/10/2026"}, {"OMS_TRACE_ONE_TABLE_PER_SYSTEM": "maybe"}):
        with pytest.raises(config.SettingsError):
            config.load_settings(bad)


def test_sec_truncates_toward_zero_like_java():
    assert config.sec(1_999_999_999) == 1
    assert config.sec(-1_999_999_999) == -1
    assert config.sec(0) == 0
