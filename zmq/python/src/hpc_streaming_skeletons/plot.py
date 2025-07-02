import ast
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import typer


def plot_throughput(
    csv_path: Path = typer.Argument(..., help="Path to the test results CSV file"),
    output_path: Optional[Path] = typer.Option(
        None, "--output", "-o", help="Output file path for the plot"
    ),
    show: bool = typer.Option(
        True, "--show/--no-show", help="Whether to show the plot"
    ),
    figsize: tuple[float, float] = typer.Option(
        (10, 6), help="Figure size as width,height"
    ),
) -> None:
    """
    Create a log-log plot of total throughput vs. message size, colored by zero_copy setting.

    This function reads test results from a CSV file, aggregates throughput data by test,
    and creates a visualization showing how throughput varies with message size for
    different zero_copy configurations.
    """
    # Load data
    df = pd.read_csv(csv_path)

    # Parse config column to extract 'size' and 'zero_copy'
    def parse_config(cfg):
        d = ast.literal_eval(cfg)
        return pd.Series(
            {
                "size": d["size"],
                "zero_copy": d["zero_copy"],
                "test_number": d["test_number"],
            }
        )

    df = df.join(df["config"].apply(parse_config))

    # Aggregate throughput for each test_number, size, zero_copy
    agg = (
        df.groupby(["test_number", "size", "zero_copy"])
        .agg({"throughput_mbps": "sum"})
        .reset_index()
    )

    # Create the plot
    plt.figure(figsize=figsize)
    sns.lineplot(
        data=agg,
        x="size",
        y="throughput_mbps",
        hue="zero_copy",
        marker="o",
        palette="Set1",
    )

    plt.xscale("log")
    plt.yscale("log")
    plt.xlabel("Message Size (bytes)")
    plt.ylabel("Total Throughput (Mbps)")
    plt.title("Total Throughput vs. Message Size (log-log)")
    plt.ylim(1, 100_000)  # 1 Mbps to 100 Gbps
    plt.legend(title="Zero Copy")
    plt.tight_layout()

    # Save or show the plot
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        typer.echo(f"Plot saved to {output_path}")

    if show:
        plt.show()
    else:
        plt.close()


def main():
    """Main entry point for the plotting CLI."""
    typer.run(plot_throughput)
