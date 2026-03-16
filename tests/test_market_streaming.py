from collections import deque
from datetime import date

from mccain_capital.services import market_data_service
from mccain_capital.services import market_worker


class _FakeStreamResponse:
    def __init__(self, lines):
        self._lines = iter(lines)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def readline(self):
        return next(self._lines, b"")


def test_iter_tradier_market_events_parses_linebreak_stream(monkeypatch):
    requests = []

    monkeypatch.setattr(market_data_service, "_tradier_api_key", lambda: "token")
    monkeypatch.setattr(market_data_service, "_tradier_market_session_id", lambda: "session-123")
    monkeypatch.setattr(
        market_data_service, "_tradier_stream_base_url", lambda: "https://stream.example.com"
    )

    def _fake_urlopen(req, timeout=0):
        requests.append((req, timeout))
        return _FakeStreamResponse(
            [
                b'{"type":"summary","symbol":"SPX","prevclose":5000}\n',
                b'data: {"type":"trade","symbol":"SPX","last":5002.5}\n',
                b"\n",
                b"",
            ]
        )

    monkeypatch.setattr(market_data_service.urllib.request, "urlopen", _fake_urlopen)

    events = list(market_data_service.iter_tradier_market_events(["SPX", "VIX"]))

    assert [event["type"] for event in events] == ["summary", "trade"]
    req, timeout = requests[0]
    assert req.full_url == "https://stream.example.com/v1/markets/events"
    assert timeout == 90
    body = req.data.decode("utf-8")
    assert "sessionid=session-123" in body
    assert "symbols=SPX%2CVIX" in body
    assert "filter=summary%2Ctrade%2Cquote" in body


def test_merge_stream_event_updates_cache_with_live_tick(monkeypatch):
    monkeypatch.setattr(market_worker.app_runtime, "now_iso", lambda: "2026-03-13T14:30:00+00:00")
    monkeypatch.setattr(
        market_worker,
        "_MARKET_CACHE",
        {
            "prices": {
                symbol: {"price": None, "pct_change": None, "as_of": ""}
                for symbol in market_worker.WATCHLIST
            },
            "series": {symbol: [] for symbol in market_worker.WATCHLIST},
            "series_points": {symbol: [] for symbol in market_worker.WATCHLIST},
            "alerts": [],
            "updated_at": "",
        },
    )
    monkeypatch.setattr(
        market_worker,
        "_SERIES",
        {
            symbol: deque(maxlen=market_worker.MAX_SERIES_POINTS)
            for symbol in market_worker.WATCHLIST
        },
    )
    monkeypatch.setattr(
        market_worker,
        "_SERIES_POINTS",
        {
            symbol: deque(maxlen=market_worker.MAX_SERIES_POINTS)
            for symbol in market_worker.WATCHLIST
        },
    )
    monkeypatch.setattr(
        market_worker, "_STREAM_CONTEXT", {symbol: {} for symbol in market_worker.WATCHLIST}
    )

    market_worker._merge_stream_event(
        {
            "type": "summary",
            "symbol": "SPX",
            "prevclose": 5000.0,
            "open": 5004.0,
            "high": 5008.0,
            "low": 4998.0,
        }
    )
    market_worker._merge_stream_event(
        {
            "type": "trade",
            "symbol": "SPX",
            "last": 5007.5,
            "date": 1741876200000,
        }
    )

    snapshot = market_worker.get_market_snapshot()
    quote = snapshot["prices"]["SPX"]

    assert quote["price"] == 5007.5
    assert quote["provider"] == "tradier"
    assert quote["reason"] == "tradier_stream_trade"
    assert quote["day_open"] == 5004.0
    assert quote["day_high"] == 5008.0
    assert quote["day_low"] == 4998.0
    assert round(quote["pct_change"], 3) == round(((5007.5 - 5000.0) / 5000.0) * 100.0, 3)
    assert snapshot["series_points"]["SPX"][-1]["v"] == 5007.5


def test_start_market_worker_runs_stream_and_fast_loop_when_stream_available(monkeypatch):
    started = []

    class _FakeThread:
        def __init__(self, *, target, name, daemon):
            self.target = target
            self.name = name
            self.daemon = daemon

        def start(self):
            started.append(self.name)

    monkeypatch.setattr(market_worker, "_STARTED", False)
    monkeypatch.setattr(market_worker, "_FAST_STARTED", False)
    monkeypatch.setattr(market_worker, "_STREAM_STARTED", False)
    monkeypatch.setattr(market_data_service, "tradier_stream_available", lambda: True)
    monkeypatch.setattr(market_worker.threading, "Thread", _FakeThread)

    market_worker.start_market_worker_once()

    assert "market-worker" in started
    assert "market-fast-tick-worker" in started
    assert "market-tradier-stream-worker" in started


def test_get_intraday_uses_short_lived_curve_cache(monkeypatch):
    calls = []

    monkeypatch.setattr(market_data_service, "_INTRADAY_CURVE_CACHE", {})
    monkeypatch.setattr(market_data_service, "_massive_api_key", lambda: "")
    monkeypatch.setattr(
        market_data_service,
        "_tradier_intraday_rows",
        lambda symbol: calls.append(symbol)
        or [
            {
                "ts": "2026-03-14T14:30:00+00:00",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10.0,
            }
        ]
        * 25,
    )

    first = market_data_service.get_intraday("SPX")
    second = market_data_service.get_intraday("SPX")

    assert len(calls) == 1
    assert len(first) == 25
    assert first == second
    assert first is not second


def test_get_prior_session_intraday_uses_short_lived_curve_cache(monkeypatch):
    calls = []

    monkeypatch.setattr(market_data_service, "_PRIOR_SESSION_CURVE_CACHE", {})
    monkeypatch.setattr(market_data_service, "_massive_api_key", lambda: "")
    monkeypatch.setattr(
        market_data_service,
        "_tradier_intraday_rows_for_date",
        lambda symbol, session_day: calls.append((symbol, session_day.isoformat()))
        or [
            {
                "ts": "2026-03-13T14:30:00+00:00",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10.0,
            }
        ]
        * 25,
    )

    first = market_data_service.get_prior_session_intraday(
        "SPX", anchor_session_day=date(2026, 3, 16)
    )
    second = market_data_service.get_prior_session_intraday(
        "SPX", anchor_session_day=date(2026, 3, 16)
    )

    assert calls == [("SPX", "2026-03-13")]
    assert len(first) == 25
    assert first == second
    assert first is not second
