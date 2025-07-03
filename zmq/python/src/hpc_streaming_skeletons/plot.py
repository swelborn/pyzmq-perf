import ast
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import typer

from hpc_streaming_skeletons.utils import calculate_throughput

from .models import Role, TestConfig


def plot(
    csv_path: Path = typer.Argument(..., help="Path to the test results CSV file"),
    output_path: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Output file path for the plot (default: same as input with .png extension)",
    ),
    show: bool = typer.Option(
        True, "--show/--no-show", help="Whether to show the plot"
    ),
    figsize: tuple[float, float] = typer.Option(
        (10, 6), help="Figure size as width,height"
    ),
) -> None:
    """
    Create a log-log plot of throughput by message size, showing aggregate sender and receiver throughput.

    This function reads test results from a CSV file and creates visualizations showing
    true aggregate throughput (total messages / time from first start to last end)
    for both senders and receivers separately.

    By default, the output PNG file will be saved in the same directory as the input CSV
    with the same name but .png extension.
    """
    # Generate output path if not provided
    if output_path is None:
        output_path = csv_path.with_suffix(".png")

    # Load data
    df = pd.read_csv(csv_path)

    # Parse config column
    def parse_config(cfg: str):
        config_dict = ast.literal_eval(cfg)
        _cfg = TestConfig(**config_dict)
        return pd.Series(
            {
                "size": _cfg.size,
                "zero_copy": _cfg.zero_copy,
                "test_number": _cfg.test_number,
            }
        )

    df = df.join(df["config"].apply(parse_config))

    df_group_by_test = df.groupby(["test_number", "size", "zero_copy"])

    def get_receivers(group):
        return group[group["role"] == Role.receiver.value]

    def get_senders(group):
        return group[group["role"] == Role.sender.value]

    # --- True Aggregate Throughput Calculation ---
    def calculate_true_aggregate(group: pd.DataFrame):
        # The group name will be a tuple of (test_number, size, zero_copy)
        _, message_size, _ = group.name

        # Calculate the total duration from the first start to the last end
        min_start = group["start_time"].min()
        max_end = group["end_time"].max()
        total_duration = max_end - min_start

        if total_duration <= 0:
            # If no duration, return zero throughput
            return pd.Series(
                {
                    "throughput_mbps": 0,
                    "role": "aggregate",
                }
            )

        total_messages = get_receivers(group)["messages_received"].sum()
        throughput = calculate_throughput(
            total_messages, message_size, min_start, max_end
        )

        return pd.Series(
            {
                "throughput_mbps": throughput,
                "role": "aggregate",
            }
        )

    agg = df_group_by_test.apply(
        calculate_true_aggregate, include_groups=False
    ).reset_index()

    # --- Plotting ---
    plt.figure(figsize=figsize)

    # Create a combined grouping variable for legend
    agg["group"] = (
        agg["role"]
        + " ("
        + agg["zero_copy"].map({True: "zero_copy", False: "no_zero_copy"})
        + ")"
    )

    sns.lineplot(
        data=agg,
        x="size",
        y="throughput_mbps",
        hue="group",
        marker="o",
        palette="Set1",
    )

    plt.xscale("log")
    plt.yscale("log")
    plt.xlabel("Message Size (bytes)")
    plt.ylabel("Total Throughput (Mbps)")
    plt.title("Throughput Comparison: Role-based vs Aggregate vs Average (log-log)")
    plt.ylim(1, 100_000)  # 1 Mbps to 100 Gbps
    plt.legend(title="Role & Zero Copy")
    plt.tight_layout()

    # Always save the plot (output_path is guaranteed to be set)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    typer.echo(f"Plot saved to {output_path}")

    if show:
        plt.show()
    else:
        plt.close()
