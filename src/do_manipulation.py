import polars as pl
from utils import load_data_paths_from_config


def convert_timestamp_to_datetime(df, new_column_name, existing_column_name):
    """
    Convert float timestamp column into datetime and add as new column.

    Parameters
    ----------
    data : pl.DataFrame
        Input DataFrame containing the timestamp column.
    new_column_name : str
        Name of new column to be added.
    existing_column_name : str
        Name of the existing column containing timestamps.

    Returns
    -------
    pl.DataFrame
        DataFrame with newly added datetime column.

    Raises
    ------
    ValueError
        If the existing column is not found in the DataFrame.
    """

    if existing_column_name not in df.columns:
        raise ValueError
    (f"Column '{existing_column_name}' not found in DataFrame.")

    return df.with_columns(
        pl.from_epoch(pl.col(existing_column_name), time_unit="s").alias(
            new_column_name
        )
    )


def convert_multiple_dfs_timestamp_to_datetime(
    dfs, new_column_name, existing_column_name
):
    """
    Convert multiple dfs' timestamp column into datetime column.

    Parameters
    ----------
    dfs : list
        List of DataFrame.
    new_column_name : str
        Name of new column to be added
    existing_column_name : str
        Name of the existing column containing timestamps.

    Returns
    -------
    list
        List of updated DataFrame.
    """
    return [
        convert_timestamp_to_datetime(df, new_column_name, existing_column_name)
        for df in dfs
    ]


def convert_str_hex_to_int(df, new_column_name, existing_column_name):
    """
    Convert hex that it's dtype is str into int column.

    Parameters
    ----------
    df : pl.DataFrame
        Input DataFrame containing the hex column.
    new_column_name : str
        Name of new column to be added.
    existing_column_name : str
        Name of the existing column containing hex.

    Returns
    -------
    pl.DataFrame
        DataFrame with newly added hex int column.

    Raises
    ------
    ValueError
        If the existing column is not found in the DataFrame.
    """

    if existing_column_name not in df.columns:
        raise ValueError
    (f"Column '{existing_column_name}' not found in DataFrame.")

    return df.with_columns(
        pl.col(existing_column_name)
        .str.to_integer(base=16, strict=True)
        .alias(new_column_name)
    )


def convert_multiple_dfs_str_hex_canid_to_int(
    dfs, new_column_name, existing_column_name
):
    """
    Convert multiple dfs' str hex column into int hex column.

    Parameters
    ----------
    dfs : list
        List of DataFrame.
    new_column_name : str
        Name of new column to be added
    existing_column_name : str
        Name of the existing column containing hex.

    Returns
    -------
    list
        List of updated DataFrame.
    """
    return [
        convert_str_hex_to_int(df, new_column_name, existing_column_name) for df in dfs
    ]


def combine_byte_columns_to_message_column(df, existing_column_name, new_column_name):
    """
    Combine byte0...byte7 columns that represent message parts to one column which directly name is message.

    Parameters
    ----------
    df : pl.DataFrame
        Input DataFrame containing the byte columns.
    new_column_name : str
        Name of new column to be added.
    existing_column_name : str
        Name of the existing column containing dlc.
    Returns
    -------
    pl.DataFrame
        DataFrame with newly added message str column.
    """

    return df.with_columns(
        pl.concat_str(
            [f"byte{i}" for i in range(df[existing_column_name].max())],
            ignore_nulls=True,
        ).alias(new_column_name)
    )


def combine_multiple_dfs_bytes_to_message_column(
    dfs, existing_column_name, new_column_name
):
    """
    Combine byte columns from multiple DataFrames into a new message column.

    Parameters
    ----------
    dfs : list
        List of DataFrame.
    new_column_name : str
        Name of new column to be added
    existing_column_name : str
        Name of the existing column containing dlc.

    Returns
    -------
    list
        List of updated DataFrame.
    """
    return [
        combine_byte_columns_to_message_column(
            df, existing_column_name, new_column_name
        )
        for df in dfs
    ]


def load_datasets():
    dos_df_out_path, fuzzy_df_out_path, attack_free_csv_out_path = (
        load_data_paths_from_config("out_paths")
    )

    dos_df = pl.read_csv(dos_df_out_path)
    fuzzy_df = pl.read_csv(fuzzy_df_out_path)
    attack_free_df = pl.read_csv(attack_free_csv_out_path)
    return dos_df, fuzzy_df, attack_free_df


def convert_data_types(dfs):

    new_timestamp_column_name = "datetime"
    existing_timestamp_column_name = "timestamp"

    new_canid_column_name = "updatedCanIdInt"
    existing_canid_column_name = "canId"

    dfs = convert_multiple_dfs_timestamp_to_datetime(
        dfs, new_timestamp_column_name, existing_timestamp_column_name
    )

    return convert_multiple_dfs_str_hex_canid_to_int(
        dfs, new_canid_column_name, existing_canid_column_name
    )


def add_new_features(dfs):
    existing_dlc_column_name = "dlc"
    new_message_column_name = "message"
    return combine_multiple_dfs_bytes_to_message_column(
        dfs, existing_dlc_column_name, new_message_column_name
    )


if __name__ == "__main__":

    dos_df, fuzzy_df, attack_free_df = load_datasets()

    dfs = [dos_df, fuzzy_df, attack_free_df]

    dfs = convert_data_types(dfs)
    dfs = add_new_features(dfs)

    print(dos_df.head())
    print(fuzzy_df.head())
    print(attack_free_df.head())
