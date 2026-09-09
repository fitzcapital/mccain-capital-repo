from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COLLECTOR = ROOT / "monitoring/netdata/mccain_capital.plugin"
ALARMS = ROOT / "monitoring/netdata/health.d/mccain_capital.conf"
START = ROOT / "monitoring/netdata/start.sh"


def test_netdata_collector_publishes_stable_gamma_and_worker_dimensions():
    body = COLLECTOR.read_text(encoding="utf-8")
    assert "CHART mccain_capital.gamma" in body
    assert '"gamma_stale"' in body
    assert '"gamma_refresh_failed"' in body
    assert "CHART mccain_capital.worker_resources" in body
    assert '"worker_pressure"' in body
    assert "/ops/health/market-pulse" in body


def test_netdata_alarms_create_sustained_and_recovery_events_without_repeats():
    body = ALARMS.read_text(encoding="utf-8")
    assert "alarm: mccain_gamma_stale" in body
    assert "alarm: mccain_gamma_refresh_failed" in body
    assert "alarm: mccain_worker_thread_pressure" in body
    assert "warn: $this == 1" in body
    assert "crit: $this >= 2" in body
    assert "delay: up" in body and "down" in body
    assert body.count("to: silent") == 3


def test_netdata_start_installs_collector_and_health_rules_then_restarts():
    body = START.read_text(encoding="utf-8")
    assert '"$NETDATA_PLUGIN_DIR/mccain_capital.plugin"' in body
    assert '"$NETDATA_CONFIG_DIR/health.d/mccain_capital.conf"' in body
    assert "brew services restart netdata" in body


def test_container_runtime_caps_and_worker_recycling_are_configured():
    for name in ("Containerfile", "Dockerfile"):
        body = (ROOT / name).read_text(encoding="utf-8")
        for variable in (
            "OPENBLAS_NUM_THREADS",
            "OMP_NUM_THREADS",
            "MKL_NUM_THREADS",
            "NUMEXPR_NUM_THREADS",
        ):
            assert f"ENV {variable}=1" in body
        assert "--max-requests ${GUNICORN_MAX_REQUESTS:-750}" in body
        assert "--max-requests-jitter ${GUNICORN_MAX_REQUESTS_JITTER:-100}" in body
