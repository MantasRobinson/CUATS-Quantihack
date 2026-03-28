"""Export simulation data to JSON for downstream consumption (sonification, art)."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from agents.simulation import Simulation


def export_simulation(sim: Simulation, filepath: Optional[str] = None) -> str:
    """Export all simulation data to a JSON file.

    Parameters
    ----------
    sim : Simulation
        A simulation that has already been ``run()``.
    filepath : str, optional
        Output path.  If *None*, a timestamped filename is generated
        in the current directory.

    Returns
    -------
    str
        The path of the written file.
    """
    if filepath is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"sim_output_{ts}.json"

    data = {
        "metadata": _build_metadata(sim),
        "trades": _build_trades(sim),
        "steps": _build_steps(sim),
        "agent_summary": sim.summary(),
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)

    print(f"Exported simulation data -> {filepath}")
    return filepath


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _build_metadata(sim: Simulation) -> dict:
    return {
        "timestamp": datetime.now().isoformat(),
        "num_steps": sim.config.num_steps,
        "asset": sim.config.asset,
        "seed": sim.config.seed,
        "num_agents": len(sim.agents),
        "agent_names": [a.name for a in sim.agents],
        "total_trades": len(sim.get_all_trades()),
    }


def _build_trades(sim: Simulation) -> list[dict]:
    trades = sim.get_all_trades()
    out = []
    for i, t in enumerate(trades):
        out.append({
            "trade_index": i,
            "asset": t.asset,
            "price": t.price,
            "quantity": t.quantity,
            "buy_order_id": t.buy_order_id,
            "sell_order_id": t.sell_order_id,
        })
    return out


def _build_steps(sim: Simulation) -> list[dict]:
    out = []
    for r in sim.history:
        step_data = {
            "step": r.step,
            "best_bid_price": r.best_bid[0] if r.best_bid else None,
            "best_bid_volume": r.best_bid[1] if r.best_bid else None,
            "best_ask_price": r.best_ask[0] if r.best_ask else None,
            "best_ask_volume": r.best_ask[1] if r.best_ask else None,
            "spread": r.spread,
            "mid_price": r.mid_price,
            "last_trade_price": r.last_trade_price,
            "num_trades_total": r.num_trades_total,
            "agent_positions": r.agent_positions,
            "agent_pnls": r.agent_pnls,
        }
        out.append(step_data)
    return out
