import pandas as pd
import numpy as np
from datetime import datetime
from utils import load_data

# ──────────────────────────────────────────────────────────────
# 🛠️ Configuration Constants
# ──────────────────────────────────────────────────────────────
DLC_COLUMN = "dlc"
FRAME_TYPE_COLUMN = "frame_type"
FILTER_COLUMN = "updated_flag"
STRATIFIED_COLUMN = "dlc"
RANDOM_SAMPLE_SIZE = 40000
STRATIFIED_ATTACK_FREE_FRACTION = 0.02
STRATIFIED_ATTACK_FREE_INSIDE_DOS_FRACTION = 0.003
STRATIFIED_ATTACK_FREE_INSIDE_FUZZY_FRACTION = 0.003
SORTED_COLUMN_NAME = "timestamp"
# ──────────────────────────────────────────────────────────────


def validate_column_in_dataframe(df, column_name):
    """
    Checks column exist or not in given df.

    Parameters
    ----------
    df :pl.DataFrame
        Input DataFrame.
    column_name : str
        Column name that will be checked.

    Raises
    ------
    ValueError
       If the specified column does not exist in the DataFrame.
    """

    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in DataFrame.")


def divide_df(df, column_name, column_value):
    """
    Filters the given DataFrame based on a specific column value.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame to be filtered.
    column_name : str
        The name of column to apply to filter on.
    column_value : str
        The value to filter rows by in specified column.

    Returns
    -------
    pd.DataFrame
        A filtered dataframe.
    """
    return df[df[column_name] == column_value]


def delete_columns_or_noisy_data(
    attack_free_inside_fuzzy_df, dlc_column, attack_free_df, frame_type_column
):
    """
    Remove columns or noisy data based on given conditions

    Parameters
    ----------
    attack_free_inside_fuzzy_df : pd.DataFrame
        A subset of the fuzzy dataset containing only non-attack records.
    dlc_column : str
        The name of the DLC (Data Length Code) column
    attack_free_df : pd.DataFrame
        A DataFrame containing non-attack data from the main dataset
    frame_type_column : str
        The name of the frame_type column to be dropped as it's not useful.

    Returns
    -------
    tuple of pd.DataFrame
        - `attack_free_df`: Updated DataFrame with specified columns removed.
        - `attack_free_inside_fuzzy_df`: Cleaned DataFrame with noisy rows removed.
    """
    # In the attack-free dataset inside the fuzzy dataset, the value 6 in the dlc column appears only
    # three times out of 3 million records. Since it is noisy, it needs to be removed.
    attack_free_inside_fuzzy_df = attack_free_inside_fuzzy_df[
        attack_free_inside_fuzzy_df[dlc_column] != 6
    ]

    # All values in frame_type column is 0, that's why I decided to delete it!
    attack_free_df = drop_columns(attack_free_df, frame_type_column)
    return attack_free_df, attack_free_inside_fuzzy_df


def drop_columns(df, columns):
    """
    Drop specified columns from DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame that columns will be dropped.
    columns : list
        list of column names to be dropped.

    Returns
    -------
    pd.DataFrame
        Modified DataFrame with the specified columns removed.

    Raises
    ------
    KeyError
        If any of the specified columns do not exist in the DataFrame.
    """
    validate_column_in_dataframe(df, columns)
    return df.drop(columns=columns)


def do_random_sampling(df, sample_size):
    """

    Perform random sampling on a given DataFrame.
    ----------
    df : pd.DataFrame
        The input dataframe from which to sample data.
    sample_size : int
        The number of samples to extract

    Returns
    -------
    pd.DataFrame
        A randomly sampled DataFrame with 'sample_size' rows
    """
    return df.sample(n=sample_size, random_state=42)


def do_proportionate_stratified_sampling(df, column_name, sample_fraction):
    """_summary_

    Perform proportionate stratified sampling on a given DataFrame.

    This function samples a specified fraction of each unique category
    in the given column, ensuring the original distribution is maintained.
    ----------
    df : _type_
        The input DataFrame containing data.
    column_name : _type_
        The name of column to use for stratified sampling.
    sample_fraction : _type_
        The fraction of data to sample from each category. (between 0 and 1)

    Returns
    -------
    pd.DataFrame
        A proportionately stratified sample of the input DataFrame.

    Raises
    ------
    ValueError
        If the sample_fraction is not between 0 and 1.
    """

    if not (0 < sample_fraction <= 1):
        raise ValueError("sample_fraction must be between 0 and 1")

    # group by creates sub-dataframes for each unique value in the column
    # apply allows us to apply a function to each of these sub-dataframes
    # lambda applies sample to each sub-dataframe

    return df.groupby(column_name, group_keys=False).apply(
        lambda x: x.sample(frac=sample_fraction, random_state=42)
    )


def sample_data(
    dfs_dict,
    random_sample_size,
    stratified_attack_free_fraction,
    stratified_attack_free_inside_dos_fraction,
    stratified_attack_free_inside_fuzzy_fraction,
    stratified_column,
):
    """
    Samples data from various datasets using random and stratified sampling.

    Parameters
    ----------
    dfs_dict : dict
        Dictionary containing the following DataFrames:
        - 'only_dos_df'
        - 'only_fuzzy_df'
        - 'attack_free_df'
        - 'attack_free_inside_dos_df'
        - 'attack_free_inside_fuzzy_df'
    random_sample_size : int
        Number of rows to sample randomly from the DoS and fuzzy datasets.
    stratified_attack_free_fraction : float
        Fraction of rows to sample from 'attack_free_df' using stratified sampling.
    stratified_attack_free_inside_dos_fraction : float
        Fraction of rows to sample from 'attack_free_inside_dos_df' using stratified sampling.
    stratified_attack_free_inside_fuzzy_fraction : float
        Fraction of rows to sample from 'attack_free_inside_fuzzy_df' using stratified sampling.
    stratified_column : str
        Column name to use for stratified sampling.

    Returns
    -------
    dict
        Dictionary containing sampled DataFrames with keys:
        - 'only_dos_df'
        - 'only_fuzzy_df'
        - 'attack_free_df'
        - 'attack_free_inside_dos_df'
        - 'attack_free_inside_fuzzy_df'
    """

    sampled_dfs_dict = {}

    sampled_dos_df = do_random_sampling(dfs_dict["only_dos_df"], random_sample_size)
    sampled_fuzzy_df = do_random_sampling(dfs_dict["only_fuzzy_df"], random_sample_size)

    sampled_attack_free_df = do_proportionate_stratified_sampling(
        dfs_dict["attack_free_df"], stratified_column, stratified_attack_free_fraction
    )
    sampled_attack_free_inside_dos_df = do_proportionate_stratified_sampling(
        dfs_dict["attack_free_inside_dos_df"],
        stratified_column,
        stratified_attack_free_inside_dos_fraction,
    )
    sampled_attack_free_inside_fuzzy_df = do_proportionate_stratified_sampling(
        dfs_dict["attack_free_inside_fuzzy_df"],
        stratified_column,
        stratified_attack_free_inside_fuzzy_fraction,
    )
    sampled_dfs_dict = {
        "only_dos_df": sampled_dos_df,
        "only_fuzzy_df": sampled_fuzzy_df,
        "attack_free_df": sampled_attack_free_df,
        "attack_free_inside_dos_df": sampled_attack_free_inside_dos_df,
        "attack_free_inside_fuzzy_df": sampled_attack_free_inside_fuzzy_df,
    }

    return sampled_dfs_dict


def sort_df_by_column(df, column_name):
    """

    Sort the given DataFrame by the values in the specified column.
    ----------
    df : pd.DataFrame
        The input DataFrame to sort.
    column_name : str
        The name of the column to use for sorting.

    Returns
    -------
    pd.DataFrame
        The input DataFrame sorted by the values in the specified column.

    Raises
    ------
    ValueError
        If the column name is not found in the DataFrame.
    """
    validate_column_in_dataframe(df, column_name)
    return df.sort_values(by=column_name, ascending=True)


def sort_data(dfs_dict, sorted_column_name):
    """
    Sorts each DataFrame in the given dictionary by a specified column.

    Parameters
    ----------
    dfs_dict : dict
        Dictionary containing DataFrames to be sorted.
    sorted_column_name : str
        Column name by which each DataFrame will be sorted.

    Returns
    -------
    dict
        Dictionary containing sorted DataFrames.
    """
    sorted_dfs_dict = {}
    for key, data in dfs_dict.items():
        sorted_dfs_dict[key] = sort_df_by_column(data, sorted_column_name)
    return sorted_dfs_dict


def main():
    print("Loading data!")
    dfs_dict = load_data("out_paths", backend="pandas")

    dos_df = dfs_dict["dos_df"]
    fuzzy_df = dfs_dict["fuzzy_df"]
    attack_free_df = dfs_dict["attack_free_df"]

    # T represents injected message!
    # R represents normal message!

    print("Dividing data!")
    only_dos_df = divide_df(dos_df, FILTER_COLUMN, "T")
    only_fuzzy_df = divide_df(fuzzy_df, FILTER_COLUMN, "T")
    attack_free_inside_dos_df = divide_df(dos_df, FILTER_COLUMN, "R")
    attack_free_inside_fuzzy_df = divide_df(fuzzy_df, FILTER_COLUMN, "R")

    print("Removing noisy data or columns!")
    attack_free_df, attack_free_inside_fuzzy_df = delete_columns_or_noisy_data(
        attack_free_inside_fuzzy_df, DLC_COLUMN, attack_free_df, FRAME_TYPE_COLUMN
    )
    dfs_dict = {
        "only_dos_df": only_dos_df,
        "only_fuzzy_df": only_fuzzy_df,
        "attack_free_df": attack_free_df,
        "attack_free_inside_dos_df": attack_free_inside_dos_df,
        "attack_free_inside_fuzzy_df": attack_free_inside_fuzzy_df,
    }

    print("Sampling data!")
    sampled_dfs_dict = sample_data(
        dfs_dict,
        RANDOM_SAMPLE_SIZE,
        STRATIFIED_ATTACK_FREE_FRACTION,
        STRATIFIED_ATTACK_FREE_INSIDE_DOS_FRACTION,
        STRATIFIED_ATTACK_FREE_INSIDE_FUZZY_FRACTION,
        STRATIFIED_COLUMN,
    )
    # for key, data in sampled_dfs_dict.items():
    #     print(key, data.shape)

    # print(sampled_dfs_dict["only_dos_df"].head())

    print("Sorting data!")
    sorted_dfs_dict = sort_data(sampled_dfs_dict, SORTED_COLUMN_NAME)
    # print(sorted_dfs_dict["only_dos_df"].head())


if __name__ == "__main__":
    main()
