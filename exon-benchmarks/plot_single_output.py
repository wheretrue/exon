import os
import json
import pandas as pd
import altair as alt
import pathlib
import random
import argparse

HERE = pathlib.Path(__file__).parent

PLOTS = HERE / "plots"
PLOTS.mkdir(exist_ok=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Add an argument for the path to the results file
    parser.add_argument("results_file", type=str)

    # Add an optional argument for the chart title.
    parser.add_argument("--title", type=str, default=None)

    # Parse the command line arguments
    args = parser.parse_args()
    bench_group, git_hash = args.results_file[:-5].split("_")

    path = pathlib.Path(args.results_file)
    data = json.loads(path.read_text())

    chart_title = args.title or f"Results from {path.name}"

    rows = []

    for result in data["results"]:
        command = result["command"]
        for time in result["times"]:
            rows.append(
                {
                    "Run Group": command,
                    "Time (s)": time,
                }
            )

    df = pd.DataFrame(rows)

    chart = (
        alt.Chart(df)
        .mark_point(size=18)
        .encode(
            y="Run Group",
            x=alt.X(
                "Time (s):Q",
            ).scale(domain=(df["Time (s)"].min() - 2, df["Time (s)"].max() + 2)),
            yOffset="jitter:Q",
            color="Run Group",
        )
        .properties(
            width=200,
            height=200,
            title=chart_title,
        )
        .configure_axisY(title=None)
        .transform_calculate(
            # Generate Gaussian jitter with a Box-Muller transform
            jitter="sqrt(-1*log(random()))*cos(1*PI*random())"
        )
        .resolve_scale(yOffset="independent")
    )

    chart.save(PLOTS / f"{path.name}.svg")
