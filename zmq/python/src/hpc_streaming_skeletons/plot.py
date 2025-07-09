import ast
from pathlib import Path
from typing import List, Optional

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import typer
import yaml
from pydantic import BaseModel

from hpc_streaming_skeletons.utils import calculate_throughput

from .models import Role, TestConfig


class DatasetConfig(BaseModel):
    csv_file: Path
    label: str
    description: Optional[str] = None


class PlotConfig(BaseModel):
    title: str
    datasets: List[DatasetConfig]
    output_path: Path
    figsize: tuple[float, float] = (10, 6)


def plot(
    input_file: Path = typer.Argument(
        ..., help="Path to CSV file or YAML configuration file"
    ),
    output_path: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Output file path for the plot (overrides config file setting)",
    ),
) -> None:
    """
    Create a log-log plot of throughput by message size from CSV files.

    This function can process either:
    1. A single CSV file
    2. A YAML configuration file that specifies multiple datasets to plot with custom labels

    For CSV files, the function will generate a basic plot with default settings.
    For YAML configuration files, you can specify multiple datasets, custom labels, and plot settings.

    Example YAML configuration:
    ```yaml
    title: "Throughput Comparison: 12 vs 32 Senders"
    datasets:
      - csv_file: "/path/to/coord_nic_sender_12/out/results.csv"
        label: "12 Senders"
        description: "Coordinator with 12 sender pairs"
      - csv_file: "/path/to/coord_nic_sender_32/out/results.csv"
        label: "32 Senders"
        description: "Coordinator with 32 sender pairs"
    output_path: "/path/to/output.png"
    show: false
    figsize: [12, 8]
    ```
    """
    # Determine if input is CSV or YAML based on file extension
    if input_file.suffix.lower() == ".csv":
        # Create a default configuration for single CSV file
        config = PlotConfig(
            title="Throughput vs. Message Size",
            datasets=[
                DatasetConfig(
                    csv_file=input_file,
                    label=input_file.stem,
                    description=f"Data from {input_file.name}",
                )
            ],
            output_path=output_path or input_file.with_suffix(".png"),
            show=show if show is not None else True,
            figsize=(10, 6),
        )
    else:
        # Assume YAML configuration file
        with open(input_file, "r") as f:
            config_data = yaml.safe_load(f)

        config = PlotConfig(**config_data)

        # Override config with command line arguments if provided
        if output_path is not None:
            config.output_path = output_path

    typer.echo(f"Processing {len(config.datasets)} datasets:")
    for i, dataset in enumerate(config.datasets, 1):
        typer.echo(f"  {i}. {dataset.label}: {dataset.csv_file}")

    typer.echo(f"Output will be saved to: {config.output_path}")

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

    def get_receivers(group):
        return group[group["role"] == Role.receiver.value]

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

    # Process each CSV file and collect aggregated results
    all_aggs = []
    for i, dataset in enumerate(config.datasets):
        csv_path = dataset.csv_file

        # Load data
        df = pd.read_csv(csv_path)

        # Parse config and join
        df = df.join(df["config"].apply(parse_config))

        # Group by test parameters
        df_group_by_test = df.groupby(["test_number", "size", "zero_copy"])

        # Calculate true aggregate throughput
        agg = df_group_by_test.apply(
            calculate_true_aggregate, include_groups=False
        ).reset_index()

        # Add source label using the dataset label
        agg["source"] = dataset.label

        all_aggs.append(agg)

    # Combine all results
    agg_combined = pd.concat(all_aggs, ignore_index=True)

    # Calculate maximum throughput for each dataset for bar chart
    max_throughput_per_dataset = (
        agg_combined.groupby("source")["throughput_mbps"].max().reset_index()
    )
    max_throughput_per_dataset["throughput_gbps"] = (
        max_throughput_per_dataset["throughput_mbps"] / 1000
    )

    # Create consistent color mapping and ordering
    unique_sources = agg_combined["source"].unique()
    color_palette = sns.color_palette("Set1", len(unique_sources))
    source_colors = dict(zip(unique_sources, color_palette))

    # Sort datasets by source name for consistent ordering
    max_throughput_per_dataset = max_throughput_per_dataset.sort_values("source")

    # --- Create subplots: line plot and bar chart ---
    fig, (ax1, ax2) = plt.subplots(
        1, 2, figsize=(config.figsize[0] * 2, config.figsize[1])
    )

    # --- Line Plot (left subplot) ---
    # Use source name directly for cleaner legend labels
    sns.lineplot(
        data=agg_combined,
        x="size",
        y="throughput_mbps",
        hue="source",
        marker="o",
        palette=source_colors,
        ax=ax1,
    )

    ax1.set_xscale("log")
    ax1.set_yscale("log")
    ax1.set_xlabel("Message Size (bytes)", fontsize=11)
    ax1.set_ylabel("Total Throughput (Mbps)", fontsize=11)
    ax1.set_title(f"{config.title}", fontsize=12)
    ax1.set_ylim(1_000, 200_000)  # 1 Gbps to 120 Gbps
    ax1.tick_params(axis="both", which="major", labelsize=10)

    # Add size labels and vertical lines above the x-axis
    size_labels = [
        (64, "64B"),
        (1024, "1KB"),
        (8192, "8KB"),
        (32768, "32KB"),
        (65536, "64KB"),
        (262144, "256KB"),
        (1048576, "1MB"),
        (8388608, "8MB"),
        (67108864, "64MB"),
    ]

    # Get the current y-axis limits to position labels appropriately
    y_min, y_max = ax1.get_ylim()
    label_y = y_max * 0.8  # Position labels slightly below the top

    for size_bytes, label in size_labels:
        # Only add label if the size is within the current x-axis range
        x_min, x_max = ax1.get_xlim()
        if x_min <= size_bytes <= x_max:
            # Add vertical dashed line
            ax1.axvline(
                x=size_bytes,
                color="gray",
                linestyle="--",
                alpha=0.5,
                linewidth=0.8,
                zorder=0,  # Put lines behind the data
            )

            # Add size label
            ax1.text(
                size_bytes,
                label_y,
                label,
                ha="center",
                va="bottom",
                fontsize=8,
                color="gray",
                alpha=0.7,
            )

    # Position legend to avoid covering the size labels
    # Get legend handles and labels, then reorder them to match bar chart ordering
    handles, labels = ax1.get_legend_handles_labels()

    # Create a mapping from label to handle for reordering
    label_to_handle = dict(zip(labels, handles))

    # Reorder handles and labels to match the sorted bar chart order
    sorted_sources = max_throughput_per_dataset["source"].tolist()
    ordered_handles = [
        label_to_handle[source]
        for source in sorted_sources
        if source in label_to_handle
    ]
    ordered_labels = [source for source in sorted_sources if source in label_to_handle]

    ax1.legend(ordered_handles, ordered_labels, loc="lower right", fontsize=10)

    # --- Bar Chart (right subplot) ---
    # Use the same colors and order as the line plot
    bar_colors = [
        source_colors[source] for source in max_throughput_per_dataset["source"]
    ]
    bars = ax2.bar(
        max_throughput_per_dataset["source"],
        max_throughput_per_dataset["throughput_gbps"],
        color=bar_colors,
    )

    ax2.set_xlabel("Dataset", fontsize=11)
    ax2.set_ylabel("Maximum Throughput (Gbps)", fontsize=11)
    ax2.set_title("Maximum Throughput by Dataset", fontsize=12)

    # Remove x-axis ticks and labels
    ax2.set_xticks([])
    ax2.tick_params(axis="y", labelsize=10)

    # Add dataset labels inside bars and value labels on top
    for i, (bar, value, source) in enumerate(
        zip(
            bars,
            max_throughput_per_dataset["throughput_gbps"],
            max_throughput_per_dataset["source"],
        )
    ):
        height = bar.get_height()

        # Add dataset label inside the bar with black background and white text (vertical)
        ax2.text(
            bar.get_x() + bar.get_width() / 2.0,
            height / 2.0,
            source,
            ha="center",
            va="center",
            fontsize=10,
            color="white",
            fontweight="bold",
            rotation=90,
            bbox=dict(boxstyle="round,pad=0.3", facecolor="black", alpha=0.8),
        )

        # Add value labels on top of bars
        ax2.text(
            bar.get_x() + bar.get_width() / 2.0,
            height + height * 0.01,
            f"{value:.0f} Gbps",
            ha="center",
            va="bottom",
            fontweight="bold",
        )

    plt.tight_layout()

    # Always save the plot (output_path is guaranteed to be set)
    fig.savefig(config.output_path, dpi=300, bbox_inches="tight")
    typer.echo(f"✅ Plot saved to {config.output_path}")
    plt.close(fig)
