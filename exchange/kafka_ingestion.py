from __future__ import annotations

# Control-plane demo and ingestion helpers for Biggest-O.
#
# This module wires together:
# - Kafka/Redpanda input
# - live trade feed bridging
# - risk monitoring and control actions
# - Redis state publishing
# - Prometheus metrics
# - a small visualiser and runnable demos

import json
import importlib
import os
import sys
import time
import argparse
from dataclasses import asdict
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple

try:
    import requests
except ImportError:  # pragma: no cover - optional helper dependency
    requests = None

try:
    from .quant_risk_control_plane import (
        Action,
        Decision,
        MarketEvent,
        OrderEvent,
        PnLEvent,
        StrategyHealthMonitor,
        Thresholds,
    )
except ImportError:
    # Allow the module to run both as part of the package and directly via
    # `python -m exchange.kafka_ingestion` from the exchange folder.
    if __package__ in (None, ""):
        sys.path.append(os.path.dirname(__file__))
        from quant_risk_control_plane import (  # type: ignore[no-redef]
            Action,
            Decision,
            MarketEvent,
            OrderEvent,
            PnLEvent,
            StrategyHealthMonitor,
            Thresholds,
        )
    else:
        raise


def _require_kafka() -> None:
    try:
        importlib.import_module("kafka")
    except ImportError as exc:
        raise RuntimeError(
            "kafka-python is required. Install with: pip install kafka-python"
        ) from exc


def _get_env(name: str, default: str) -> str:
    # Environment variables keep the demo configurable without code changes.
    value = os.getenv(name)
    return value if value is not None and value != "" else default


def _get_env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _parse_event(payload: Dict[str, object]) -> Optional[Tuple[str, object]]:
    # Convert raw JSON into the typed monitor events used by the control plane.
    event_type = payload.get("type") or payload.get("event_type")
    ts = float(payload.get("ts") or time.time())

    if event_type == "market":
        features = payload.get("features") or {}
        return (
            "market",
            MarketEvent(
                ts=ts,
                bid=float(payload.get("bid")),
                ask=float(payload.get("ask")),
                features={k: float(v) for k, v in dict(features).items()},
                venue_latency_ms=float(payload.get("venue_latency_ms") or 0.0),
            ),
        )

    if event_type == "order":
        return (
            "order",
            OrderEvent(ts=ts, accepted=bool(payload.get("accepted"))),
        )

    if event_type == "pnl":
        return (
            "pnl",
            PnLEvent(ts=ts, realized_pnl=float(payload.get("realized_pnl") or 0.0)),
        )

    return None


def create_consumer(
    brokers: str,
    topic: str,
    group_id: str,
    poll_timeout_ms: int = 1000,
) -> Any:
    # Build a Kafka/Redpanda consumer with optional SASL/TLS settings.
    _require_kafka()
    kafka_mod = importlib.import_module("kafka")

    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL")
    sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM")
    sasl_username = os.getenv("KAFKA_SASL_USERNAME")
    sasl_password = os.getenv("KAFKA_SASL_PASSWORD")

    kwargs = {
        "bootstrap_servers": [b.strip() for b in brokers.split(",") if b.strip()],
        "group_id": group_id,
        "enable_auto_commit": True,
        "auto_offset_reset": "latest",
        "consumer_timeout_ms": poll_timeout_ms,
    }
    if security_protocol:
        kwargs["security_protocol"] = security_protocol
    if sasl_mechanism:
        kwargs["sasl_mechanism"] = sasl_mechanism
    if sasl_username:
        kwargs["sasl_plain_username"] = sasl_username
    if sasl_password:
        kwargs["sasl_plain_password"] = sasl_password

    return kafka_mod.KafkaConsumer(topic, **kwargs)


def iter_events(consumer: Any) -> Iterator[Tuple[str, object]]:
    # Yield typed events as soon as messages arrive from Kafka.
    for msg in consumer:
        try:
            payload = json.loads(msg.value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue

        parsed = _parse_event(payload)
        if parsed is not None:
            yield parsed


def iter_trade_feed(feed_url: str, auth_token: Optional[str] = None) -> Iterator[Dict[str, object]]:
    """Yield JSON objects from a streaming trade feed.

    Supports newline-delimited JSON and simple SSE-style `data: {...}` frames.
    """
    if requests is None:
        raise RuntimeError("requests is required to connect to a trade feed")

    headers = {}
    if auth_token:
        headers["Authorization"] = auth_token

    # Accept either newline-delimited JSON or a simple SSE-style stream.
    with requests.get(feed_url, headers=headers, stream=True, timeout=30) as resp:
        resp.raise_for_status()
        for raw_line in resp.iter_lines(decode_unicode=True):
            if not raw_line:
                continue
            line = raw_line.strip()
            if line.startswith("data:"):
                line = line[5:].strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                yield payload


def bridge_trade_feed_to_kafka(
    feed_url: str,
    *,
    brokers: Optional[str] = None,
    topic: Optional[str] = None,
    group_id: Optional[str] = None,
    auth_token: Optional[str] = None,
    event_type: str = "market",
) -> None:
    """Read a live trade feed and publish each JSON event to Kafka/Redpanda."""
    _require_kafka()
    kafka_mod = importlib.import_module("kafka")
    KafkaProducer = kafka_mod.KafkaProducer

    brokers = brokers or _get_env("KAFKA_BROKERS", "localhost:9092")
    topic = topic or _get_env("KAFKA_TOPIC", "market-events")

    producer = KafkaProducer(
        bootstrap_servers=[b.strip() for b in brokers.split(",") if b.strip()],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
    )

    # Re-publish the live feed into the topic as JSON messages.
    for payload in iter_trade_feed(feed_url, auth_token=auth_token):
        event = dict(payload)
        event.setdefault("type", event_type)
        producer.send(topic, event)
        producer.flush()


def demo_trade_feed() -> list[Decision]:
    """Demonstrate the monitor against a small synthetic trade feed."""
    thresholds = Thresholds()
    monitor = StrategyHealthMonitor(
        thresholds=thresholds,
        feature_names=["ret_1s", "spread_bps", "imbalance", "vol_10s"],
    )

    # A short synthetic sequence that starts calm, then becomes risky enough to
    # trigger stronger control actions.
    sample_feed = [
        {"type": "market", "ts": 1.0, "bid": 99.9, "ask": 100.0, "features": {"ret_1s": 0.0002, "spread_bps": 10.0, "imbalance": 0.05, "vol_10s": 0.01}, "venue_latency_ms": 8.0},
        {"type": "order", "ts": 1.1, "accepted": True},
        {"type": "pnl", "ts": 1.2, "realized_pnl": 15.0},
        {"type": "market", "ts": 2.0, "bid": 99.2, "ask": 99.35, "features": {"ret_1s": -0.004, "spread_bps": 15.1, "imbalance": -0.7, "vol_10s": 0.06}, "venue_latency_ms": 62.0},
        {"type": "order", "ts": 2.1, "accepted": False},
        {"type": "pnl", "ts": 2.2, "realized_pnl": -350.0},
    ]

    decisions: list[Decision] = []
    for payload in sample_feed:
        parsed = _parse_event(payload)
        if parsed is None:
            continue
        event_type, event = parsed
        if event_type == "market":
            monitor.on_market(event)
        elif event_type == "order":
            monitor.on_order(event)
        elif event_type == "pnl":
            monitor.on_pnl(event)

        decisions.append(monitor.evaluate(event.ts))

    return decisions


def run_monitor_loop(
    *,
    feature_names: Iterable[str],
    thresholds: Optional[Thresholds] = None,
    on_decision: Optional[Callable[[Decision], None]] = None,
    stop_on_halt: bool = False,
) -> None:
    # Drive the online monitor from a Kafka topic and emit periodic decisions.
    brokers = _get_env("KAFKA_BROKERS", "localhost:9092")
    topic = _get_env("KAFKA_TOPIC", "market-events")
    group_id = _get_env("KAFKA_GROUP_ID", "qrm-monitor")
    poll_timeout_ms = int(_get_env_float("KAFKA_POLL_TIMEOUT_MS", 1000.0))
    eval_interval_s = _get_env_float("MONITOR_EVAL_INTERVAL_S", 1.0)

    monitor = StrategyHealthMonitor(
        thresholds=thresholds or Thresholds(),
        feature_names=feature_names,
    )

    consumer = create_consumer(
        brokers=brokers,
        topic=topic,
        group_id=group_id,
        poll_timeout_ms=poll_timeout_ms,
    )

    last_eval = 0.0
    on_decision = on_decision or (lambda d: print(d))

    for event_type, event in iter_events(consumer):
        if event_type == "market":
            monitor.on_market(event)
        elif event_type == "order":
            monitor.on_order(event)
        elif event_type == "pnl":
            monitor.on_pnl(event)

        now = time.time()
        if now - last_eval >= eval_interval_s:
            decision = monitor.evaluate(now)
            on_decision(decision)
            last_eval = now
            if stop_on_halt and decision.action.value == "HALT":
                break


def _call_first(gateway: Any, method_names: Iterable[str], *args: Any, **kwargs: Any) -> Any:
    for name in method_names:
        fn = getattr(gateway, name, None)
        if callable(fn):
            return fn(*args, **kwargs)
    return None


class ControlPlaneRuntime:
    """Connects the risk monitor to Redis, Prometheus, and an order gateway."""

    def __init__(
        self,
        *,
        feature_names: Iterable[str],
        thresholds: Optional[Thresholds] = None,
        gateway: Any = None,
        redis_url: Optional[str] = None,
        redis_key: str = "biggest_o:control_state",
        redis_channel: str = "biggest_o:control_events",
        prometheus_namespace: str = "biggest_o",
        prometheus_registry: Any = None,
    ):
        self.monitor = StrategyHealthMonitor(
            thresholds=thresholds or Thresholds(),
            feature_names=feature_names,
        )
        self.gateway = GatewayAdapter(gateway) if gateway is not None else None
        self.redis_key = redis_key
        self.redis_channel = redis_channel
        self.redis_client = self._build_redis(redis_url)
        self.metrics = self._build_metrics(prometheus_namespace, prometheus_registry)

    @staticmethod
    def _build_redis(redis_url: Optional[str]) -> Any:
        if not redis_url:
            return None
        redis_mod = importlib.import_module("redis")
        return redis_mod.Redis.from_url(redis_url, decode_responses=True)

    @staticmethod
    def _build_metrics(namespace: str, registry: Any) -> Dict[str, Any]:
        # Prometheus is optional for the demo; if it is missing, use no-op
        # metrics so the rest of the control plane can still run.
        try:
            prom = importlib.import_module("prometheus_client")
        except ModuleNotFoundError:
            class _NoOpMetric:
                def labels(self, **kwargs: Any) -> "_NoOpMetric":
                    return self

                def inc(self, amount: float = 1.0) -> None:
                    return None

                def set(self, value: float) -> None:
                    return None

            class _NoOpProm:
                def Counter(self, *args: Any, **kwargs: Any) -> _NoOpMetric:
                    return _NoOpMetric()

                def Gauge(self, *args: Any, **kwargs: Any) -> _NoOpMetric:
                    return _NoOpMetric()

            prom = _NoOpProm()
        kwargs: Dict[str, Any] = {"namespace": namespace}
        if registry is not None:
            kwargs["registry"] = registry
        return {
            "decision_total": prom.Counter(
                "control_decision_total",
                "Control decisions by action",
                ["action"],
                **kwargs,
            ),
            "market_latency_ms": prom.Gauge(
                "market_latency_ms",
                "Latest venue latency in ms",
                **kwargs,
            ),
            "market_age_seconds": prom.Gauge(
                "market_age_seconds",
                "Age of latest market event in seconds",
                **kwargs,
            ),
            "reject_rate": prom.Gauge(
                "reject_rate",
                "Latest order reject rate",
                **kwargs,
            ),
            "realized_pnl": prom.Gauge(
                "realized_pnl",
                "Latest realized pnl",
                **kwargs,
            ),
            "drawdown": prom.Gauge(
                "drawdown",
                "Current drawdown from peak pnl",
                **kwargs,
            ),
            "spread_bps": prom.Gauge(
                "spread_bps",
                "Latest spread in basis points",
                **kwargs,
            ),
        }

    def _publish_state(self, decision: Decision, now_ts: float) -> None:
        # Persist the latest control state so other services can read it quickly.
        state = {
            "ts": now_ts,
            "action": decision.action.value,
            "reasons": decision.reasons,
            "metrics": decision.metrics,
        }

        if self.redis_client is not None:
            payload = {
                "ts": str(now_ts),
                "action": decision.action.value,
                "reasons": json.dumps(decision.reasons),
                "metrics": json.dumps(decision.metrics),
            }
            self.redis_client.hset(self.redis_key, mapping=payload)
            self.redis_client.publish(self.redis_channel, json.dumps(state))

    def _record_metrics(self, decision: Decision) -> None:
        # Update Prometheus counters/gauges with the latest decision snapshot.
        action = decision.action.value
        self.metrics["decision_total"].labels(action=action).inc()
        if "venue_latency_ms" in decision.metrics:
            self.metrics["market_latency_ms"].set(decision.metrics["venue_latency_ms"])
        if "market_data_age_s" in decision.metrics:
            self.metrics["market_age_seconds"].set(decision.metrics["market_data_age_s"])
        if "reject_rate" in decision.metrics:
            self.metrics["reject_rate"].set(decision.metrics["reject_rate"])
        if "realized_pnl" in decision.metrics:
            self.metrics["realized_pnl"].set(decision.metrics["realized_pnl"])
        if "drawdown" in decision.metrics:
            self.metrics["drawdown"].set(decision.metrics["drawdown"])
        if "spread_bps" in decision.metrics:
            self.metrics["spread_bps"].set(decision.metrics["spread_bps"])

    def _apply_gateway(self, decision: Decision) -> None:
        # Push the decision into the execution layer through the adapter.
        if self.gateway is None:
            return

        self.gateway.apply(decision)

    def ingest(self, event_type: str, event: object, now_ts: Optional[float] = None) -> Decision:
        # Process one event, update the monitor, then publish control outputs.
        if event_type == "market":
            self.monitor.on_market(event)  # type: ignore[arg-type]
        elif event_type == "order":
            self.monitor.on_order(event)  # type: ignore[arg-type]
        elif event_type == "pnl":
            self.monitor.on_pnl(event)  # type: ignore[arg-type]

        ts = now_ts if now_ts is not None else time.time()
        decision = self.monitor.evaluate(ts)
        self._record_metrics(decision)
        self._publish_state(decision, ts)
        self._apply_gateway(decision)
        return decision


class GatewayAdapter:
    """Normalize different gateway implementations to the control-plane hooks."""

    def __init__(self, gateway: Any):
        self.gateway = gateway

    def cancel_all_orders(self) -> Any:
        # Abstract over common gateway naming conventions.
        return _call_first(
            self.gateway,
            ["cancel_all_orders", "cancel_all", "cancel_open_orders", "cancel_resting_orders", "flatten_orders"],
        )

    def pause_trading(self) -> Any:
        # Use the first compatible pause/disable method exposed by the gateway.
        return _call_first(
            self.gateway,
            ["pause_trading", "disable_trading", "stop_trading", "pause", "set_trading_enabled"],
            False,
        )

    def resume_trading(self) -> Any:
        # Re-enable trading when the monitor returns to OK.
        return _call_first(
            self.gateway,
            ["resume_trading", "enable_trading", "start_trading", "resume", "set_trading_enabled"],
            True,
        )

    def set_safe_mode(self) -> Any:
        # Enter a softer risk state before a full halt.
        return _call_first(
            self.gateway,
            ["set_safe_mode", "safe_mode", "enter_safe_mode", "set_mode", "mode"],
            "BORDERLINE",
        )

    def set_halt(self) -> Any:
        # Trigger an emergency stop if the monitor detects a hard failure.
        return _call_first(
            self.gateway,
            ["halt", "set_halt", "halt_trading", "emergency_stop", "set_mode"],
            "HALT",
        )

    def flatten_risk(self) -> Any:
        # Ask the gateway to reduce or close exposure.
        return _call_first(
            self.gateway,
            ["flatten_risk", "reduce_inventory", "liquidate", "close_all_positions", "flatten_positions"],
        )

    def apply(self, decision: Decision) -> None:
        # Translate the abstract risk decision into concrete execution actions.
        if decision.action == Action.OK:
            self.resume_trading()
            return

        if decision.action == Action.ALERT:
            _call_first(self.gateway, ["set_alert_mode", "set_mode"], "ALERT")
            return

        if decision.action == Action.SAFE_MODE:
            self.cancel_all_orders()
            self.flatten_risk()
            self.set_safe_mode()
            self.pause_trading()
            return

        if decision.action == Action.HALT:
            self.cancel_all_orders()
            self.flatten_risk()
            self.set_halt()
            self.pause_trading()


class ExampleOrderGateway:
    """Minimal example gateway implementation for SAFE_MODE/HALT wiring.

    Replace the print statements with real gateway calls in your execution stack.
    """

    def __init__(self):
        # The demo gateway just records what was asked of it.
        self.trading_enabled = True
        self.mode = "OK"
        self.cancelled_orders = 0
        self.flattened = 0

    def cancel_all_orders(self) -> None:
        # In a real gateway, this would send cancels to the venue or broker.
        self.cancelled_orders += 1
        print("[gateway] cancel_all_orders()")

    def cancel_open_orders(self) -> None:
        self.cancel_all_orders()

    def pause_trading(self, enabled: bool = False) -> None:
        self.trading_enabled = bool(enabled)
        print(f"[gateway] pause_trading({enabled})")

    def disable_trading(self) -> None:
        self.pause_trading(False)

    def stop_trading(self) -> None:
        self.pause_trading(False)

    def resume_trading(self, enabled: bool = True) -> None:
        self.trading_enabled = bool(enabled)
        print(f"[gateway] resume_trading({enabled})")

    def enable_trading(self) -> None:
        self.resume_trading(True)

    def start_trading(self) -> None:
        self.resume_trading(True)

    def set_trading_enabled(self, enabled: bool) -> None:
        self.trading_enabled = bool(enabled)
        print(f"[gateway] set_trading_enabled({enabled})")

    def set_safe_mode(self, mode: str = "BORDERLINE") -> None:
        self.mode = mode
        print(f"[gateway] set_safe_mode({mode})")

    def safe_mode(self) -> None:
        self.set_safe_mode("BORDERLINE")

    def enter_safe_mode(self) -> None:
        self.set_safe_mode("BORDERLINE")

    def set_halt(self, mode: str = "HALT") -> None:
        self.mode = mode
        print(f"[gateway] set_halt({mode})")

    def halt(self) -> None:
        self.set_halt("HALT")

    def halt_trading(self) -> None:
        self.set_halt("HALT")

    def emergency_stop(self) -> None:
        self.set_halt("HALT")

    def set_mode(self, mode: str) -> None:
        # Keep the current mode visible in the demo output.
        self.mode = mode
        print(f"[gateway] set_mode({mode})")

    def flatten_risk(self) -> None:
        self.flattened += 1
        print("[gateway] flatten_risk()")

    def reduce_inventory(self) -> None:
        self.flatten_risk()

    def liquidate(self) -> None:
        self.flatten_risk()

    def close_all_positions(self) -> None:
        self.flatten_risk()

    def flatten_positions(self) -> None:
        self.flatten_risk()


def run_control_loop(
    *,
    feature_names: Iterable[str],
    gateway: Any = None,
    thresholds: Optional[Thresholds] = None,
    redis_url: Optional[str] = None,
    stop_on_halt: bool = True,
    on_decision: Optional[Callable[[Decision], None]] = None,
) -> None:
    # Drive the online monitor from a Kafka topic and emit periodic decisions.
    """Consume Kafka/Redpanda events and apply control actions."""
    brokers = _get_env("KAFKA_BROKERS", "localhost:9092")
    topic = _get_env("KAFKA_TOPIC", "market-events")
    group_id = _get_env("KAFKA_GROUP_ID", "qrm-control")
    poll_timeout_ms = int(_get_env_float("KAFKA_POLL_TIMEOUT_MS", 1000.0))

    runtime = ControlPlaneRuntime(
        feature_names=feature_names,
        thresholds=thresholds,
        gateway=gateway,
        redis_url=redis_url or os.getenv("REDIS_URL"),
    )

    consumer = create_consumer(
        brokers=brokers,
        topic=topic,
        group_id=group_id,
        poll_timeout_ms=poll_timeout_ms,
    )

    on_decision = on_decision or (lambda d: print(d))

    for event_type, event in iter_events(consumer):
        decision = runtime.ingest(event_type, event)
        on_decision(decision)
        if stop_on_halt and decision.action == Action.HALT:
            break


def demo_control_runtime() -> list[Decision]:
    """Small runnable demo that exercises Redis/Prometheus/gateway wiring in memory.

    Uses synthetic events and the example gateway, so it can be run without Kafka.
    """
    gateway = ExampleOrderGateway()
    runtime = ControlPlaneRuntime(
        feature_names=["ret_1s", "spread_bps", "imbalance", "vol_10s"],
        gateway=gateway,
        redis_url=None,
    )

    # A short synthetic sequence that starts calm, then becomes risky enough to
    # trigger stronger control actions.
    events = [
        ("market", MarketEvent(ts=1.0, bid=99.9, ask=100.0, features={"ret_1s": 0.0002, "spread_bps": 10.0, "imbalance": 0.05, "vol_10s": 0.01}, venue_latency_ms=8.0)),
        ("order", OrderEvent(ts=1.1, accepted=True)),
        ("pnl", PnLEvent(ts=1.2, realized_pnl=25.0)),
        ("market", MarketEvent(ts=2.0, bid=99.2, ask=99.35, features={"ret_1s": -0.004, "spread_bps": 15.1, "imbalance": -0.7, "vol_10s": 0.06}, venue_latency_ms=62.0)),
        ("order", OrderEvent(ts=2.1, accepted=False)),
        ("pnl", PnLEvent(ts=2.2, realized_pnl=-350.0)),
    ]

    decisions: list[Decision] = []
    for event_type, event in events:
        # Feed each synthetic event into the runtime and collect the decision.
        decisions.append(runtime.ingest(event_type, event, now_ts=event.ts))

    return decisions


class _DemoKafkaMessage:
    def __init__(self, value: bytes):
        self.value = value


class _DemoKafkaConsumer:
    def __init__(self, payloads: Iterable[Dict[str, object]]):
        # Simulate Kafka messages without needing a live broker.
        self._messages = [_DemoKafkaMessage(json.dumps(payload).encode("utf-8")) for payload in payloads]

    def __iter__(self):
        return iter(self._messages)


def demo_kafka_control_pipeline() -> list[Decision]:
    """Second demo: simulate Kafka messages flowing into the control plane.

    This exercises the same parser as the real Kafka/Redpanda consumer.
    """
    # These payloads mirror the JSON shape that a real Kafka consumer would see.
    payloads = [
        {"type": "market", "ts": 10.0, "bid": 100.0, "ask": 100.02, "features": {"ret_1s": 0.0001, "spread_bps": 2.0, "imbalance": 0.02, "vol_10s": 0.01}, "venue_latency_ms": 7.5},
        {"type": "order", "ts": 10.1, "accepted": True},
        {"type": "pnl", "ts": 10.2, "realized_pnl": 12.0},
        {"type": "market", "ts": 11.0, "bid": 98.6, "ask": 98.9, "features": {"ret_1s": -0.005, "spread_bps": 30.4, "imbalance": -0.8, "vol_10s": 0.08}, "venue_latency_ms": 88.0},
        {"type": "order", "ts": 11.1, "accepted": False},
        {"type": "pnl", "ts": 11.2, "realized_pnl": -620.0},
    ]

    gateway = ExampleOrderGateway()
    runtime = ControlPlaneRuntime(
        feature_names=["ret_1s", "spread_bps", "imbalance", "vol_10s"],
        gateway=gateway,
        redis_url=None,
    )

    decisions: list[Decision] = []
    for event_type, event in iter_events(_DemoKafkaConsumer(payloads)):
        # Reuse the same runtime path the real consumer would exercise.
        decisions.append(runtime.ingest(event_type, event, now_ts=event.ts))

    return decisions


def _summarize_decisions(label: str, decisions: list[Decision]) -> None:
    # Compact console summary of the decision stream.
    print(f"== {label} ==")
    print(f"count: {len(decisions)}")
    actions = [decision.action.value for decision in decisions]
    print(f"actions: {', '.join(actions)}")
    for idx, decision in enumerate(decisions, start=1):
        reasons = "; ".join(decision.reasons[:3]) or "no reasons"
        print(f"  {idx}. {decision.action.value} | {reasons}")


def decisions_to_json(decisions: list[Decision], *, indent: int = 2) -> str:
    """Serialize decisions to JSON for logging or downstream storage."""
    payload = [
        {
            "action": d.action.value,
            "reasons": d.reasons,
            "metrics": d.metrics,
        }
        for d in decisions
    ]
    return json.dumps(payload, indent=indent)


def _print_json_demo(label: str, decisions: list[Decision]) -> None:
    # Handy for copying demo output into logs or notebooks.
    print(f"== {label} (json) ==")
    print(decisions_to_json(decisions, indent=2))


def visualize_decisions(
    decisions: list[Decision],
    title: str = "Control decisions",
    save_path: Optional[str] = None,
) -> None:
    """Display a simple visual summary of decision actions and key metrics.

    Falls back to a text-only summary if matplotlib is unavailable.
    """
    try:
        plt = importlib.import_module("matplotlib.pyplot")
    except ModuleNotFoundError:
        # When matplotlib is unavailable, keep the demo useful with text output.
        print(f"== {title} (text visualiser) ==")
        for idx, d in enumerate(decisions, start=1):
            print(f"{idx:02d} {d.action.value:10s} | spread_bps={d.metrics.get('spread_bps', 0):.2f} | latency_ms={d.metrics.get('venue_latency_ms', 0):.1f} | drawdown={d.metrics.get('drawdown', 0):.2f}")
        return

    xs = list(range(1, len(decisions) + 1))
    action_to_level = {"OK": 0, "ALERT": 1, "BORDERLINE": 2, "HALT": 3}
    ys = [action_to_level.get(d.action.value, 0) for d in decisions]
    colors = ["#2ecc71" if y == 0 else "#f39c12" if y == 1 else "#e67e22" if y == 2 else "#e74c3c" for y in ys]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 7), sharex=True)
    ax1.bar(xs, ys, color=colors, width=0.8)
    ax1.set_yticks([0, 1, 2, 3])
    ax1.set_yticklabels(["OK", "ALERT", "BORDERLINE", "HALT"])
    ax1.set_ylabel("Action")
    ax1.set_title(title)
    ax1.grid(axis="y", alpha=0.25)

    spread = [d.metrics.get("spread_bps", 0.0) for d in decisions]
    latency = [d.metrics.get("venue_latency_ms", 0.0) for d in decisions]
    drawdown = [d.metrics.get("drawdown", 0.0) for d in decisions]

    ax2.plot(xs, spread, marker="o", label="spread_bps")
    ax2.plot(xs, latency, marker="o", label="venue_latency_ms")
    ax2.plot(xs, drawdown, marker="o", label="drawdown")
    ax2.set_xlabel("Decision index")
    ax2.set_ylabel("Metric value")
    ax2.grid(alpha=0.25)
    ax2.legend(loc="best")

    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"saved chart: {save_path}")
    plt.show()


def _print_demo_decisions(decisions: list[Decision]) -> None:
    gateway = ExampleOrderGateway()
    print("--- demo_control_runtime ---")
    for idx, decision in enumerate(decisions, start=1):
        print(f"decision {idx}: {decision.action.value}")
        if decision.reasons:
            for reason in decision.reasons[:5]:
                print(f"  - {reason}")
        print(f"  metrics: {decision.metrics}")
    print("--- gateway state ---")
    print(f"trading_enabled={gateway.trading_enabled}")
    print(f"mode={gateway.mode}")
    print(f"cancelled_orders={gateway.cancelled_orders}")
    print(f"flattened={gateway.flattened}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Demo the Biggest-O control runtime")
    parser.add_argument(
        "--format",
        choices=("summary", "json", "both"),
        default="both",
        help="Output format for the demo results",
    )
    parser.add_argument(
        "--visualize",
        action="store_true",
        help="Show a simple chart of the output data",
    )
    parser.add_argument(
        "--chart-path",
        default="kafka_control_demo.png",
        help="PNG path for the visualiser output",
    )
    args = parser.parse_args()

    control_demo = demo_control_runtime()
    kafka_demo = demo_kafka_control_pipeline()

    if args.format in ("summary", "both"):
        _summarize_decisions("demo_control_runtime", control_demo)
        _summarize_decisions("demo_kafka_control_pipeline", kafka_demo)

    if args.format in ("json", "both"):
        _print_json_demo("demo_control_runtime", control_demo)
        _print_json_demo("demo_kafka_control_pipeline", kafka_demo)

    if args.visualize:
        visualize_decisions(
            control_demo,
            title="demo_control_runtime",
            save_path=args.chart_path,
        )
        base, ext = os.path.splitext(args.chart_path)
        kafka_chart_path = f"{base}_kafka{ext or '.png'}"
        visualize_decisions(
            kafka_demo,
            title="demo_kafka_control_pipeline",
            save_path=kafka_chart_path,
        )
