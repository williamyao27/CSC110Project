"""
Copyright and Usage Information
===============================
This file is Copyright (c) 2021 Allen Uy, Nathaniel Yong, and William Yao.
"""
import csv
from datetime import datetime
import pandas as pd


def create_df_from_csv(filename: str) -> pd.DataFrame:
    """Return a pandas dataframe generated from data stored in a csv file with the given filename.
    The correspond to the headers in the file, and each row represents one row of data stored in the
    csv file."

    Preconditions:
        - filename refers to a valid csv file with headers
    """
    # First, read the csv file
    csv_tuple = read_csv_file(filename)

    # First, generate a dictionary from csv_tuple that maps each header to a list of the
    # corresponding values from each row.
    converted_dict = {}
    for h in range(len(csv_tuple[0])):
        converted_dict[csv_tuple[0][h]] = []
        for row in csv_tuple[1]:
            converted_dict[csv_tuple[0][h]].append(row[h])

    # Then, return the dictionary converted to a pandas dataframe.
    return pd.DataFrame.from_records(converted_dict)


def read_csv_file(filename: str) -> tuple[list[str], list[list[str]]]:
    """Return the headers and data stored in a csv file with the given filename as a tuple.

    Preconditions:
        - filename refers to a valid csv file with headers
    """
    with open(filename) as file:
        reader = csv.reader(file)
        headers = next(reader)
        data = list(reader)
    return (headers, data)


def convert_df_types_canada(df: pd.DataFrame) -> None:
    """Convert all strings in the dataframe representing points in time to datetime objects.

    Preconditions:
        - Headers in dataframe correspond to headers used in the Canadian dataset
    """
    df["numtotal_all_administered"] = df["numtotal_all_administered"].apply(lambda x: int(x))
    df["report_date"] = df["report_date"].apply(lambda x: str_to_datetime_canada(x))
    df["as_of_date"] = df["as_of_date"].apply(lambda x: str_to_datetime_canada(x))


def str_to_datetime_canada(date_str: str) -> datetime:
    """Convert date_str, which represents a point in time using text with year, month, and day,
    respectively, into a datetime object. This format is used by Health Canada.
    """
    return datetime.strptime(date_str, "%Y-%m-%d")


def convert_df_types_cdc(df: pd.DataFrame) -> None:
    """Convert all strings stored in df representing points in time to datetime objects.

    Preconditions:
        - Headers in df correspond to headers used in the CDC dataset
    """
    df["Date"] = df["Date"].apply(lambda x: str_to_datetime_cdc(x))
    df["Administered"] = df["Administered"].apply(lambda x: int(x.replace(',', '')))


def str_to_datetime_cdc(date_str: str) -> datetime:
    """Convert date_str, which represents a point in time using text with month, day, and year,
    respectively, into a datetime object. This format is used by the CDC.
    """
    return datetime.strptime(date_str, "%m/%d/%Y")


def sum_dfs(df_canada: pd.DataFrame, df_cdc: pd.DataFrame) -> pd.DataFrame:
    """Return a pandas dataframe given a df_canada dataframe storing data in Health Canada's format
    and a df_cdc dataframe storing data in CDC's format. The returned dataframe shows nationwide
    doses administered for Canada, US, and the two countries combined.

    Preconditions:
        - Headers in df_canada correspond to headers used in the Canadian dataset
        - Headers in df_cdc correspond to headers used in the CDC dataset
    """
    temp_dict = {}

    # Find values for nationwide doses in Canada
    for i in range(len(df_canada)):
        if df_canada.loc[i]['prename'] == 'Canada':
            temp_dict[df_canada.loc[i]['as_of_date']] =\
                [df_canada.loc[i]['as_of_date'], df_canada.loc[i]['numtotal_all_administered']]

    # Find values for nationwide doses in the US; first check if a data point with the same date
    # is already in the dictionary from iterating through the Canadian data
    for i in range(len(df_cdc)):
        if df_cdc.loc[i]['Location'] == 'US':
            if df_cdc.loc[i]['Date'] in temp_dict:
                temp_dict[df_cdc.loc[i]['Date']].append(df_cdc.loc[i]['Administered'])
            else:
                temp_dict[df_cdc.loc[i]['Date']] =\
                    [df_cdc.loc[i]['Date'], 0, df_cdc.loc[i]['Administered']]

    # Sum values
    for i in temp_dict:
        temp_dict[i].append(temp_dict[i][1] + temp_dict[i][2])

    # Then, return the dictionary converted to a pandas dataframe.
    return pd.DataFrame.from_dict(temp_dict, orient='index',
                                  columns=['Date', 'Total vaccine doses in Canada',
                                           'Total vaccine doses in the US',
                                           'Total vaccine doses in Canada and the US combined'])


if __name__ == "__main__":
    canada_df = create_df_from_csv("data/vaccination/canada_data.csv")
    convert_df_types_canada(canada_df)

    cdc_df = create_df_from_csv("data/vaccination/cdc_data.csv")
    convert_df_types_cdc(cdc_df)

    total_df = sum_dfs(canada_df, cdc_df)

    import doctest
    doctest.testmod()

    # python_ta has a conflict with datetime.datetime
    # import python_ta.contracts
    # python_ta.contracts.check_all_contracts()

    import python_ta
    python_ta.check_all(config={
        'extra-imports': ['csv', 'pandas', 'datetime'],
        'allowed-io': ['read_csv_file'],
        'max-line-length': 100,
        'disable': ['R1705', 'C0200', 'W0108', 'E9994']
    })
