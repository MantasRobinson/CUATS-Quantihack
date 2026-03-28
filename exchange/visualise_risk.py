"""Visualisation for quant_risk_control_plane demo output."""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

from quant_risk_control_plane import demo_run, Action

ACTION_COLOURS = {
    Action.OK: "#2ecc71",
    Action.ALERT: "#f39c12",
    Action.SAFE_MODE: "#e67e22",
    Action.HALT: "#e74c3c",
}

DECISION_LABELS = ["Stable", "Regime Shift", "Hard Stale"]


def plot_decisions(decisions):
    fig, axes = plt.subplots(3, 1, figsize=(12, 10))
    fig.suptitle("Quant Risk Control Plane — Decision Summary", fontsize=14, fontweight="bold")

    # --- Panel 1: Action status ---
    ax = axes[0]
    action_order = [Action.OK, Action.ALERT, Action.SAFE_MODE, Action.HALT]
    action_levels = {a: i for i, a in enumerate(action_order)}
    colours = [ACTION_COLOURS[d.action] for d in decisions]
    levels = [action_levels[d.action] for d in decisions]
    bars = ax.bar(DECISION_LABELS, levels, color=colours, edgecolor="black", linewidth=0.8)
    ax.set_yticks(range(len(action_order)))
    ax.set_yticklabels([a.value for a in action_order])
    ax.set_ylabel("Action Level")
    ax.set_title("Decision Action")
    for bar, d in zip(bars, decisions):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                d.action.value, ha="center", va="bottom", fontsize=9, fontweight="bold")
    legend_patches = [mpatches.Patch(color=c, label=a.value) for a, c in ACTION_COLOURS.items()]
    ax.legend(handles=legend_patches, loc="upper left", fontsize=8)

    # --- Panel 2: Key numeric metrics ---
    ax = axes[1]
    metric_keys = ["drawdown", "reject_rate", "spread_bps", "venue_latency_ms"]
    metric_labels = ["Drawdown ($)", "Reject Rate", "Spread (bps)", "Latency (ms)"]
    x = np.arange(len(DECISION_LABELS))
    width = 0.2
    for i, (key, label) in enumerate(zip(metric_keys, metric_labels)):
        values = [d.metrics.get(key, 0.0) for d in decisions]
        ax.bar(x + i * width, values, width, label=label)
    ax.set_xticks(x + width * 1.5)
    ax.set_xticklabels(DECISION_LABELS)
    ax.set_title("Key Risk Metrics per Decision")
    ax.set_ylabel("Value")
    ax.legend(fontsize=8)

    # --- Panel 3: PnL and reasons count ---
    ax = axes[2]
    pnl_vals = [d.metrics.get("realized_pnl", 0.0) for d in decisions]
    reason_counts = [len(d.reasons) for d in decisions]
    ax2 = ax.twinx()
    ax.bar(DECISION_LABELS, pnl_vals,
           color=[ACTION_COLOURS[d.action] for d in decisions],
           edgecolor="black", linewidth=0.8, alpha=0.8, label="Realized PnL")
    ax2.plot(DECISION_LABELS, reason_counts, "ko--", linewidth=1.5, markersize=6, label="# Reasons")
    ax.set_ylabel("Realized PnL ($)")
    ax2.set_ylabel("# Active Reasons")
    ax.set_title("PnL and Reason Count")
    ax.legend(loc="upper left", fontsize=8)
    ax2.legend(loc="upper right", fontsize=8)

    # Annotate reasons below each decision label
    for i, d in enumerate(decisions):
        reason_text = "\n".join(f"• {r}" for r in d.reasons[:5]) or "no issues"
        fig.text(
            0.13 + i * 0.3, 0.01, reason_text,
            fontsize=6.5, verticalalignment="bottom",
            bbox=dict(boxstyle="round,pad=0.3", facecolor=ACTION_COLOURS[d.action], alpha=0.3),
        )

    plt.tight_layout(rect=[0, 0.12, 1, 1])
    plt.savefig("risk_dashboard.png", dpi=150, bbox_inches="tight")
    print("Saved risk_dashboard.png")
    plt.show()


if __name__ == "__main__":
    decisions = demo_run()
    plot_decisions(decisions)
