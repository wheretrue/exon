# Copyright 2024 WHERE TRUE Technologies.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
from pathlib import Path

import pandas as pd

PARENT = Path(__file__).parent


def process_file(filename):
    with open(filename, "r") as file:
        data = json.load(file)

    tag = filename.split("_")[-1].split(".")[0]  # Extracting TAG from filename
    rows = []

    for result in data["results"]:
        command = result["command"]
        for time in result["times"]:
            rows.append([tag, command, time])

    return rows


def create_dataframe(directory: Path) -> pd.DataFrame:
    all_rows = []
    for filename in os.listdir(directory):
        if filename.startswith("benchmarks_") and filename.endswith(".json"):
            file_path = os.path.join(directory, filename)
            all_rows.extend(process_file(file_path))

    df = pd.DataFrame(all_rows, columns=["Tag", "Command", "Time"])
    return df


if __name__ == "__main__":
    results_dir = PARENT / "results"

    df = create_dataframe(results_dir)
    print(df)
