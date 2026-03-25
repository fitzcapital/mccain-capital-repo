from mccain_capital.services import market_pulse_runtime as runtime


def test_runtime_coordinator_defaults_to_in_process(monkeypatch):
    monkeypatch.delenv("MARKET_PULSE_REFRESH_MODE", raising=False)
    coordinator = runtime.get_market_pulse_runtime_coordinator()

    assert coordinator.mode.value == "in_process"


def test_runtime_coordinator_switches_to_external(monkeypatch):
    monkeypatch.setenv("MARKET_PULSE_REFRESH_MODE", "external")
    coordinator = runtime.get_market_pulse_runtime_coordinator()

    assert coordinator.mode.value == "external"


def test_refresh_market_pulse_runtime_returns_mode(monkeypatch):
    monkeypatch.setenv("MARKET_PULSE_REFRESH_MODE", "external")
    payload = runtime.refresh_market_pulse_runtime(force_gamma=True)

    assert payload["refresh_mode"] == "external"
