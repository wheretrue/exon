import os
import json
import pandas as pd
import altair as alt
import pathlib


HERE = pathlib.Path(__file__).parent

PLOTS = HERE / "plots"
PLOTS.mkdir(exist_ok=True)

# Define the directory path where the JSON files are located
directory = HERE / "results"

# Initialize an empty list to store the dataframes
dataframes = []

# Iterate over the files in the directory
for filepath in directory.glob("*.json"):
    print(f"Processing {filepath.name}...")
    data = json.loads(filepath.read_text())

    # Extract the command and git hash from the filename
    bench_group, git_hash = filepath.name[:-5].split("_")

    # Extract the times from the JSON data
    benchmark_times = [result["mean"] for result in data["results"]]
    commands = [result["command"] for result in data["results"]]

    # Create a dataframe for the benchmark data
    df = pd.DataFrame({"Command": commands, "Time (s)": benchmark_times})

    df["Git Hash"] = git_hash
    df["Bench Group"] = bench_group

    # Append the dataframe to the list
    dataframes.append(df)

# Concatenate all dataframes into a single dataframe
combined_df = pd.concat(dataframes)

# Create subplots for each benchmark file
charts = []
for bench_group, df in combined_df.groupby("Bench Group"):
    if df["Git Hash"].nunique() == 1:
        chart = (
            alt.Chart(df)
            .mark_bar()
            .encode(
                x="Command",
                y="Time (s)",
            )
            .properties(width=200, height=200, title=bench_group)
        )
    else:
        chart = (
            alt.Chart(df)
            .mark_line()
            .encode(
                color="Command",
                y="Time (s)",
                x="Git Hash:O",
            )
            .properties(width=200, height=200, title=bench_group)
        )

    chart.save(PLOTS / f"{bench_group}.svg")
