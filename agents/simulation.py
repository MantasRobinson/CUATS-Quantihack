from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Optional

from exchange.orderbook import OrderBook
from exchange.models import Trade
from .base import BaseAgent


@dataclass
class SimulationConfig:
    """Top-level simulation parameters."""

    num_steps: int = 2000
    asset: str = "ASSET"
    seed: int = 42


@dataclass
class StepRecord:
    """Snapshot of market state captured after all agents act in a step."""

    step: int
    best_bid: Optional[tuple] = None         # (price, volume) or None
    best_ask: Optional[tuple] = None
    spread: Optional[float] = None
    mid_price: Optional[float] = None
    last_trade_price: Optional[float] = None
    num_trades_total: int = 0
    agent_positions: dict = field(default_factory=dict)   # {agent_name: position}
    agent_pnls: dict = field(default_factory=dict)        # {agent_name: cash}


class Simulation:
    """Step-based simulation runner.

    Creates the exchange, manages the shared order-ID counter, runs agents
    each step, and records a time series of market snapshots.

    Usage::

        sim = Simulation()
        sim.add_agent(MarketMakerAgent("MM", sim.asset, sim.ob, sim.id_generator, ...))
        sim.add_agent(RetailTraderAgent("RT-0", sim.asset, sim.ob, sim.id_generator, ...))
        history = sim.run()
        print(sim.summary())
    """

    def __init__(self, config: Optional[SimulationConfig] = None):
        self.config = config or SimulationConfig()
        self.ob = OrderBook()
        self.agents: list[BaseAgent] = []
        self.history: list[StepRecord] = []
        self._rng = random.Random(self.config.seed)

        # Shared order-ID counter
        self._order_counter = 0

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def asset(self) -> str:
        return self.config.asset

    def id_generator(self) -> int:
        """Returns the next unique order ID.  Passed to agents as a callable."""
        self._order_counter += 1
        return self._order_counter

    # ------------------------------------------------------------------
    # Agent management
    # ------------------------------------------------------------------

    def add_agent(self, agent: BaseAgent) -> None:
        self.agents.append(agent)

    # ------------------------------------------------------------------
    # Simulation loop
    # ------------------------------------------------------------------

    def run(self, verbose: bool = False) -> list[StepRecord]:
        """Run the full simulation.  Returns the step-by-step history."""
        for step in range(self.config.num_steps):
            # Shuffle agent execution order to avoid systematic advantage
            self._rng.shuffle(self.agents)

            for agent in self.agents:
                agent.step(step)

            # Reconcile passive fills for all agents
            for agent in self.agents:
                agent.reconcile()

            # Record market snapshot
            record = self._snapshot(step)
            self.history.append(record)

            if verbose and step % 200 == 0:
                mid = record.mid_price or 0
                print(
                    f"  step {step:>5d}  |  mid={mid:>8.2f}  |  "
                    f"spread={record.spread or 0:>6.3f}  |  "
                    f"trades={record.num_trades_total}"
                )

        return self.history

    # ------------------------------------------------------------------
    # Post-simulation analysis
    # ------------------------------------------------------------------

    def summary(self) -> dict:
        """Per-agent summary statistics."""
        last_mid = None
        if self.history:
            last_mid = self.history[-1].mid_price

        results = {}
        for agent in self.agents:
            s = agent.state
            unrealized = s.unrealized_pnl(last_mid) if last_mid else s.cash
            results[agent.name] = {
                "position": s.position,
                "cash": round(s.cash, 2),
                "unrealized_pnl": round(unrealized, 2),
                "total_orders": s.total_orders_submitted,
                "total_fills": s.total_fills,
                "volume_traded": s.total_volume_traded,
                "active_orders": len(s.active_order_ids),
            }
        return results

    def get_all_trades(self) -> list[Trade]:
        """All trades that occurred during the simulation."""
        return self.ob.get_trade_history(self.config.asset, limit=999_999)

    def get_price_series(self) -> list[Optional[float]]:
        """Extract the mid-price time series from history."""
        return [r.mid_price for r in self.history]

    def get_trade_price_series(self) -> list[Optional[float]]:
        """Extract the last-trade-price time series from history."""
        return [r.last_trade_price for r in self.history]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _snapshot(self, step: int) -> StepRecord:
        asset = self.config.asset
        best_bid = self.ob.get_best_bid(asset)
        best_ask = self.ob.get_best_ask(asset)

        mid = None
        if best_bid is not None and best_ask is not None:
            mid = (best_bid[0] + best_ask[0]) / 2.0
        elif best_bid is not None:
            mid = best_bid[0]
        elif best_ask is not None:
            mid = best_ask[0]

        trades = self.ob.get_trade_history(asset, limit=1)
        last_price = trades[0].price if trades else None

        return StepRecord(
            step=step,
            best_bid=best_bid,
            best_ask=best_ask,
            spread=self.ob.get_spread(asset),
            mid_price=mid,
            last_trade_price=last_price,
            num_trades_total=len(self.ob._trades),
            agent_positions={a.name: a.state.position for a in self.agents},
            agent_pnls={a.name: round(a.state.cash, 2) for a in self.agents},
        )
