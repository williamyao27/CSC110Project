"""
Copyright and Usage Information
===============================
This file is Copyright (c) 2021 Allen Uy, Nathaniel Yong, and William Yao.
"""
import json
import gzip
from pathlib import Path
from typing import Union
from datetime import datetime
import pandas as pd


def read_jsonl(filepath: Union[str, Path]) -> list[str]:
    """Convert jsonl file into a list of json strings and return it.

    Preconditions:
        - filepath is a valid path for a jsonl file
    """
    lines = []
    with gzip.open(filepath, "rt") as file:
        for line in file:
            lines.append(line)
        return lines


def process_json(data: list[str]) -> list[dict]:
    """Convert a list of json strings into a list of dictionaries and return it.

    Preconditions:
        - data is a list of json strings
    """
    json_list = []
    for s in data:
        json_list.append(json.loads(s))
    return json_list


def read_files(data_dirs: list[str], batches_per_day: int) -> list[dict]:
    """Read data from every file in every directory in data_dirs,
    then process it all into one list and returns it.

    Preconditions:
        - data_dirs contains names of valid directories
        - 0 <= batches_per_day <= 99
    """
    total_list = []
    for data_dir in data_dirs:
        for path in Path("data/avax/streaming-tweetids/" + data_dir).iterdir():
            if path.name.endswith(".jsonl.gz"):
                data = read_jsonl(path)
                data = process_json(data)
                total_list.extend(data)
                if path.name.endswith(f"{batches_per_day:02d}.jsonl.gz"):
                    break

    return total_list


def convert_str_to_datetime(date_str: str) -> datetime:
    """Convert date_str, which represents a point in time using text with day of week, month, day,
    time, UTC offset, and year, respectively, into a datetime object.

    Preconditions:
        - date_str must describe time in the format of '%a %b %d %X %z %Y'

    >>> convert_str_to_datetime("Mon Oct 19 06:00:22 +0000 2020")
    datetime.datetime(2020, 10, 19, 6, 0, 22, tzinfo=datetime.timezone.utc)
    """
    return datetime.strptime(date_str, "%a %b %d %X %z %Y")


def create_df(json_list: list[dict]) -> pd.DataFrame:
    """Convert json_list into a pandas dataframe.

    Preconditions:
        - None
    """
    df = pd.DataFrame.from_records(json_list)
    df["created_at"] = pd.to_datetime(df["created_at"].apply(lambda x: convert_str_to_datetime(x)))
    return df


if __name__ == "__main__":
    import doctest
    doctest.testmod()

    # python_ta has a conflict with datetime.datetime
    # import python_ta.contracts
    # python_ta.contracts.check_all_contracts()

    import python_ta
    python_ta.check_all(config={
        'extra-imports': ['json', 'gzip', 'pandas', 'pathlib', 'typing', 'datetime'],
        'allowed-io': ['read_jsonl'],
        'max-line-length': 100,
        'disable': ['R1705', 'C0200', 'R1702', 'W0108']
    })
