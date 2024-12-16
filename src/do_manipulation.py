import polars as pl
from utils import load_data_paths_from_config


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

    validate_column_in_dataframe(df, existing_column_name)

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


def convert_hex_column_to_int(df, new_column_name, existing_column_name):
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

    validate_column_in_dataframe(df, existing_column_name)
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
        convert_hex_column_to_int(df, new_column_name, existing_column_name) for df in dfs
    ]


def merge_byte_columns(df, existing_column_name, new_column_name):
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

    byte_columns = [f"byte{i}" for i in range(df[existing_column_name].max())]
    missing_columns = [col for col in byte_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing byte columns: {missing_columns}")
    
    return df.with_columns(
        pl.concat_str(
            byte_columns,
            ignore_nulls=True,
        ).alias(new_column_name)
    )


def merge_multiple_dfs_bytes_to_message_column(
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
        merge_byte_columns(
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

    converted_timestamp_dfs = convert_multiple_dfs_timestamp_to_datetime(
        dfs, new_timestamp_column_name, existing_timestamp_column_name
    )
    new_canid_column_name = "updatedCanIdInt"
    existing_canid_column_name = "canId"

    return convert_multiple_dfs_str_hex_canid_to_int(
        converted_timestamp_dfs, new_canid_column_name, existing_canid_column_name
    )


def add_updated_flag_column_to_attack_free(df):
    """
    Adds an 'updatedFlag' column to the attack-free DataFrame, assigning 'R'
    (representing a normal message) to all rows.

    Parameters
    ----------
    df : pl.DataFrame
        The input DataFrame to which the 'updatedFlag' column will be added.

    Returns
    -------
    pl.DataFrame
        DataFrame with newly added updatedFlag str column.
    """
    updated_flag = pl.Series("updatedFlag", ["R"] * len(df))
    return df.with_columns(updated_flag)


def add_new_features(dfs):
    existing_dlc_column_name = "dlc"
    new_message_column_name = "message"
    dos_df, fuzzy_df, attack_free_df = merge_multiple_dfs_bytes_to_message_column(
        dfs, existing_dlc_column_name, new_message_column_name
    )
    attack_free_df = add_updated_flag_column_to_attack_free(attack_free_df)
    return dos_df, fuzzy_df, attack_free_df


def drop_column(df, column_to_delete):
    return df.drop(column_to_delete)


def drop_multiple_columns(df, columns_to_delete):
    """
    Drop multiple columns from DataFrame

    Parameters
    ----------
    df : pl.DataFrame
        The input DataFrame to which the columns_to_delete will be deleted.

    Returns
    -------
    pl.DataFrame
        DataFrame with newly deleted columns.
    """
    return df.drop(columns_to_delete)


def drop_multiple_dfs_multiple_columns(dfs, columns_to_delete):
    """_summary_

    Drop multiple columns from multiple DataFrames.
    ----------
    dfs : list
        List of DataFrame.
    columns_to_delete : list
        Column names that will be deleted.

    Returns
    -------
    list
        List of updated DataFrame.
    """
    return [drop_multiple_columns(df, columns_to_delete) for df in dfs]

def get_byte_column_names(dfs, dlc_column):
    max_dlc_value = max([df[dlc_column].max() for df in dfs])
    return [f"byte{i}" for i in range(max_dlc_value)]


def drop_existing_features(dfs):
    existing_dlc_column_name = "dlc"
    existing_frame_type_column_name = "frameType"
    dos_df, fuzzy_df, attack_free_df = dfs
    byte_columns = get_byte_column_names(dfs, existing_dlc_column_name)
    columns_to_delete = ["timestamp", "canId"] + byte_columns
    attack_free_df = drop_column(attack_free_df, existing_frame_type_column_name)    
    return drop_multiple_dfs_multiple_columns(
        [dos_df, fuzzy_df, attack_free_df], columns_to_delete
    )


if __name__ == "__main__":

    dos_df, fuzzy_df, attack_free_df = load_datasets()
    converted_data_types_df = convert_data_types([dos_df, fuzzy_df, attack_free_df])
    dos_df, fuzzy_df, attack_free_df = add_new_features(converted_data_types_df)
    dos_df, fuzzy_df, attack_free_df = drop_existing_features(
        [dos_df, fuzzy_df, attack_free_df]
    )

    print("dos", dos_df.columns)
    print("fuzzy", fuzzy_df.columns)
    print("free", attack_free_df.columns)
