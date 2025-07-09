import ast
from pathlib import Path
from typing import Optional, List

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
    output_path: Optional[Path] = None
    show: bool = True
    figsize: tuple[float, float] = (10, 6)


def plot(
    input_file: Path = typer.Argument(..., help="Path to CSV file or YAML configuration file"),
    output_path: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Output file path for the plot (overrides config file setting)",
    ),
    show: Optional[bool] = typer.Option(
        None, "--show/--no-show", help="Whether to show the plot (overrides config file setting)"
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
    if input_file.suffix.lower() == '.csv':
        # Create a default configuration for single CSV file
        config = PlotConfig(
            title="Throughput vs. Message Size",
            datasets=[DatasetConfig(
                csv_file=input_file,
                label=input_file.stem,
                description=f"Data from {input_file.name}"
            )],
            output_path=output_path or input_file.with_suffix(".png"),
            show=show if show is not None else True,
            figsize=(10, 6)
        )
    else:
        # Assume YAML configuration file
        with open(input_file, 'r') as f:
            config_data = yaml.safe_load(f)
        
        config = PlotConfig(**config_data)
        
        # Override config with command line arguments if provided
        if output_path is not None:
            config.output_path = output_path
        if show is not None:
            config.show = show
        
        # Generate output path if not provided (use config file location as base)
        if config.output_path is None:
            config.output_path = input_file.with_suffix(".png")
    
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

    # --- Plotting ---
    plt.figure(figsize=config.figsize)

    # Create a combined grouping variable for legend
    agg_combined["group"] = (
        agg_combined["source"] + " - "
        + agg_combined["role"]
    )

    sns.lineplot(
        data=agg_combined,
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
    plt.title(config.title)
    plt.ylim(1, 100_000)  # 1 Mbps to 100 Gbps
    plt.legend()
    plt.tight_layout()

    # Always save the plot (output_path is guaranteed to be set)
    plt.savefig(config.output_path, dpi=300, bbox_inches="tight")
    typer.echo(f"✅ Plot saved to {config.output_path}")

    if config.show:
        plt.show()
    else:
        plt.close()
